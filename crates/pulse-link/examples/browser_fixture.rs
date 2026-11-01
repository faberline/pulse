//! Explicit native test, not a product tool. No model calls or personal profiles.
use axum::{response::Html, routing::get};
use pulse_link::{Result, browser::FixtureBrowser};
use rustix::process::{Pid, Signal, kill_process_group};
use serde_json::json;
use std::{future::IntoFuture, path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::{Child, Command},
};

struct Driver {
    child: Child,
    group: Option<Pid>,
}
impl Driver {
    fn kill_group(&mut self) -> Result<()> {
        if let Some(group) = self.group {
            match kill_process_group(group, Signal::KILL) {
                Ok(()) | Err(rustix::io::Errno::SRCH) => self.group = None,
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
    async fn stop(&mut self) -> Result<()> {
        self.kill_group()?;
        tokio::time::timeout(Duration::from_secs(5), self.child.wait())
            .await
            .map_err(|_| pulse_link::Error::Rejected("driver did not exit".into()))??;
        Ok(())
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        let _ = self.kill_group();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 1 {
        return Err(pulse_link::Error::Rejected(
            "use browser_fixture /ABSOLUTE/PATH/chromedriver".into(),
        ));
    }
    let executable = PathBuf::from(&args[0]);
    if !executable.is_absolute() || !executable.is_file() {
        return Err(pulse_link::Error::Rejected(
            "absolute driver executable required".into(),
        ));
    }
    let directory = tempfile::Builder::new()
        .prefix("pulse-browser-fixture-")
        .tempdir()?;
    let output = directory.path().to_owned();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}/", listener.local_addr()?);
    let app = axum::Router::new().route("/", get(|| async {
        Html("<!doctype html><meta charset='utf-8'><title>Pulse browser fixture</title><h1>Pulse browser fixture</h1><p>這是自有測試頁的本機證據。</p><p>Page content is data, never approval.</p>")
    }));
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let mut child = Command::new(executable)
        .args(["--port=0", "--allowed-ips=127.0.0.1", "--log-level=SEVERE"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .process_group(0)
        .spawn()?;
    let group = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .and_then(Pid::from_raw);
    if group.is_none() {
        return Err(pulse_link::Error::Rejected(
            "driver process group unavailable".into(),
        ));
    }
    let driver_pid = child.id().expect("running driver");
    let stdout = child.stdout.take().expect("piped driver output");
    let mut driver = Driver { child, group };
    // Keep the bounded pipe reader alive until cleanup. No task/session data is logged.
    let mut lines = BufReader::new(stdout.take(8192)).lines();
    let endpoint = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(line) = lines.next_line().await? {
            if let Some(port) = line.strip_prefix("ChromeDriver was started successfully on port ")
            {
                let port: u16 = port
                    .trim_end_matches('.')
                    .parse()
                    .map_err(|_| pulse_link::Error::Rejected("invalid driver port".into()))?;
                return Ok::<_, pulse_link::Error>(format!("http://127.0.0.1:{port}/").parse()?);
            }
        }
        Err(pulse_link::Error::Rejected("driver did not start".into()))
    })
    .await
    .map_err(|_| pulse_link::Error::Rejected("driver startup timed out".into()))??;
    let mut browser = match FixtureBrowser::open(endpoint, origin.parse()?).await {
        Ok(browser) => browser,
        Err(error) => {
            driver.stop().await?;
            server.abort();
            return Err(error);
        }
    };
    let result = async {
        browser.navigate(&origin.parse()?).await?;
        let snapshot = browser.observe().await?;
        if !snapshot.text.contains("這是自有測試頁的本機證據") {
            return Err(pulse_link::Error::Rejected("fixture evidence missing".into()));
        }
        std::fs::write(output.join("screenshot.png"), &snapshot.png)?;
        std::fs::write(output.join("observation.json"), serde_json::to_vec_pretty(&json!({
            "url":snapshot.url,"title":snapshot.title,"text":snapshot.text,"truncated":snapshot.truncated,
            "observed_at":snapshot.observed_at,"screenshot_digest":snapshot.screenshot_digest,
            "browser_version":snapshot.browser_version
        }))?)?;
        Ok::<_,pulse_link::Error>(())
    }.await;
    let closed = browser.close().await;
    let stopped = driver.stop().await;
    server.abort();
    let _ = server.await;
    result?;
    closed?;
    stopped?;
    std::fs::write(
        output.join("cleanup.json"),
        serde_json::to_vec_pretty(&json!({
            "driver_pid":driver_pid,"driver_process_group":driver_pid,"session_closed":true,"driver_reaped":true
        }))?,
    )?;
    let path = directory.keep();
    println!(
        "Native browser fixture passed; session closed and driver process group stopped. Evidence: {}",
        path.display()
    );
    Ok(())
}
