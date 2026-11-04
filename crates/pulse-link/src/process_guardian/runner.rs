use super::*;
use rustix::process::{Pid, Signal, kill_process_group, test_kill_process_group};

/// Entry for the fixed sibling binary. No HTTP, model or task scheduler runs here.
pub async fn run(directory: &Path) -> Result<i32> {
    use std::os::fd::AsFd;
    let socket = UnixStream::from(std::io::stderr().as_fd().try_clone_to_owned()?);
    socket.peer_addr()?;
    socket.set_nonblocking(true)?;
    let control = tokio::net::UnixStream::from_std(socket)?;
    let lease = private_file(&directory.join("lease"), false)?;
    lease.try_lock_exclusive()?;
    let mut state = record(directory)?;
    validate_contents(directory)?;
    if state.phase == Phase::Stopped {
        return Ok(0);
    }
    ensure(
        state.phase == Phase::Prepared,
        "guardian launch was already attempted",
    )?;
    if let Some(prior_boot) = &state.boot {
        ensure(
            boot::current().is_ok_and(|current| current == *prior_boot),
            "guardian boot identity changed or unavailable",
        )?;
    }
    let request: Request = read(&directory.join("request.json"))?;
    ensure(
        request.program.is_absolute()
            && request.args.len() <= 128
            && request.diagnostics_limit.is_none_or(|limit| limit == 4096),
        "invalid guardian executable or arguments",
    )?;
    if let Some(resource) = &state.resource {
        resource.valid()?;
    }
    // If this owner is itself killed after Starting, recovery must retain
    // uncertainty. A released lease alone cannot prove its child stopped.
    state.phase = Phase::Starting;
    write(&directory.join("state.json"), &state)?;
    let mut command = Command::new(request.program);
    command
        .args(request.args)
        .env_clear()
        .envs(request.env)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true);
    if let Some(cwd) = request.cwd {
        command.current_dir(cwd);
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            state.phase = Phase::Stopped;
            write(&directory.join("state.json"), &state)?;
            return Err(error.into());
        }
    };
    let group = child
        .id()
        .and_then(|pid| i32::try_from(pid).ok())
        .and_then(Pid::from_raw)
        .ok_or_else(|| crate::Error::Rejected("guardian child identity unavailable".into()))?;
    let mut stderr = child.stderr.take().unwrap();
    let limit = request.diagnostics_limit;
    let mut diagnostics = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        match limit {
            Some(limit) => {
                let mut bytes = Vec::new();
                stderr
                    .take(limit as u64 + 1)
                    .read_to_end(&mut bytes)
                    .await?;
                Ok::<_, std::io::Error>(bytes.len() <= limit)
            }
            None => {
                tokio::io::copy(&mut stderr, &mut tokio::io::sink()).await?;
                Ok(true)
            }
        }
    });
    let mut pending = Vec::new();
    let mut driver = None;
    let mut stopping = false;
    let mut graceful = !state.browser;
    let mut stop_until = None;
    let mut diagnostic_ok = true;
    let mut diagnostics_done = false;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        tokio::select! {
            ready=control.readable(),if !stopping=>{
                ready?;let mut bytes=[0;2048];
                match control.try_read(&mut bytes) {
                    Ok(count)=>{
                        let eof=count==0;pending.extend_from_slice(&bytes[..count]);ensure(pending.len()<=8192,"guardian control capacity")?;
                        let mut stop=eof;
                        while let Some(end)=pending.iter().position(|b|*b==b'\n') {
                            let message:Control=serde_json::from_slice(&pending[..end])?;pending.drain(..=end);
                            match message {
                                Control::Stop=>stop=true,
                                Control::Driver {endpoint}=>{
                                    let endpoint: url::Url=endpoint.parse()?;
                                    ensure(state.browser && driver.is_none() && endpoint.scheme()=="http" && endpoint.host_str()==Some("127.0.0.1") && endpoint.port().is_some_and(|p|p>0) && endpoint.path()=="/" && endpoint.username().is_empty() && endpoint.password().is_none() && endpoint.query().is_none() && endpoint.fragment().is_none(),"guardian driver endpoint")?;
                                    driver=Some(endpoint);
                                    state.driver_endpoint=driver.as_ref().map(ToString::to_string);
                                    write(&directory.join("state.json"),&state)?;
                                    if control.try_write(&[1]).is_err() {stop=true;}
                                }
                            }
                        }
                        if stop {
                            stopping=true;
                            graceful=match &driver {Some(endpoint)=>crate::owned_process::request_shutdown(&endpoint.join("shutdown")?).await,None=>true};
                            if driver.is_none() || !graceful {match kill_process_group(group,Signal::KILL) {Ok(())|Err(rustix::io::Errno::SRCH)=>{},Err(e)=>return Err(e.into())};}
                            stop_until=Some(tokio::time::Instant::now()+Duration::from_secs(5));
                        }
                    }
                    Err(error) if error.kind()==std::io::ErrorKind::WouldBlock=>{},
                    Err(error)=>return Err(error.into()),
                }
            }
            result=&mut diagnostics,if !diagnostics_done=>{
                diagnostics_done=true;
                diagnostic_ok=matches!(result,Ok(Ok(true)));
                if !diagnostic_ok {let _=kill_process_group(group,Signal::KILL);stopping=true;stop_until=Some(tokio::time::Instant::now()+Duration::from_secs(5));}
            }
            _=tokio::time::sleep(Duration::from_millis(10))=>{
                if stop_until.is_some_and(|until|tokio::time::Instant::now()>=until) {let _=kill_process_group(group,Signal::KILL);return Err(crate::Error::Rejected("guardian stop deadline exceeded".into()));}
            }
        }
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match test_kill_process_group(group) {
            Err(rustix::io::Errno::SRCH) => break,
            Ok(()) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(10)).await
            }
            _ => {
                return Err(crate::Error::Rejected(
                    "guardian descendants unverified".into(),
                ));
            }
        }
    }
    if !diagnostics_done {
        diagnostic_ok = matches!(
            tokio::time::timeout(Duration::from_secs(1), &mut diagnostics).await,
            Ok(Ok(Ok(true)))
        );
    }
    ensure(
        graceful && (!state.browser || driver.is_none() || status.success()),
        "guardian tool shutdown unverified",
    )?;
    state.phase = Phase::Stopped;
    write(&directory.join("state.json"), &state)?;
    Ok(if diagnostic_ok {
        status.code().unwrap_or(1)
    } else {
        125
    })
}
