//! An independent local process. It has no dependency on a desktop window.
use pulse_link::{
    Result,
    client::{Config, Device, NativeVault},
    protocol::{ClientRole, NativeCommand, NativeState},
};
use tokio::sync::{mpsc, watch};

#[tokio::main]
async fn main() {
    if run().await.is_err() {
        eprintln!(
            "Local executor stopped. Check its arguments, bundled guardian, private directory and Keychain."
        );
        std::process::exit(1);
    }
}
async fn run() -> Result<()> {
    if std::env::args().skip(1).eq(["--computer-status"]) {
        // Fixed sibling only. This command needs no server, credentials, target or cache.
        let runtime = pulse_link::computer_runtime::ComputerRuntime::bundled(
            std::path::Path::new("."),
            None,
        )?;
        let value = match runtime {
            Some(runtime) => match runtime.permissions().await {
                Ok(permissions) => serde_json::json!({"installed":true,"permissions":permissions}),
                Err(_) => serde_json::json!({"installed":true,"error":"native_probe_unavailable"}),
            },
            None => serde_json::json!({"installed":false,"error":"bundled_helper_missing"}),
        };
        println!("{}", serde_json::to_string(&value)?);
        return Ok(());
    }
    let (config, pairing) =
        Config::parse_with_pairing(std::env::args().skip(1), ClientRole::LocalExecutor)?;
    let device = Device::open(config, std::sync::Arc::new(NativeVault))
        .await?
        .with_runtime_recovery()?;
    let (commands, receiver) = mpsc::channel(32);
    let (states, mut state) = watch::channel(NativeState::default());
    if let Some(code) = pairing {
        commands
            .send(NativeCommand::Pair { code })
            .await
            .map_err(|_| pulse_link::Error::Rejected("executor stopped".into()))?;
    }
    let mut worker = tokio::spawn(device.run(receiver, states));
    let mut previous = None;
    loop {
        tokio::select! {
            result=&mut worker=>break result.map_err(|_|pulse_link::Error::Rejected("executor stopped".into()))?,
            result=tokio::signal::ctrl_c()=>{
                result?;
                let _=commands.send(NativeCommand::Shutdown).await;
                break worker.await.map_err(|_|pulse_link::Error::Rejected("executor stopped".into()))?;
            }
            result=state.changed()=>{
                if result.is_err() {continue;}
                let value=state.borrow_and_update();
                // Deliberately omit task content, credentials and pairing codes.
                let status=(value.paired,value.connected,value.stopped,value.pending);
                if previous != Some(status) {
                    eprintln!("Local executor: paired={}, connected={}, stopped={}, pending={}",value.paired,value.connected,value.stopped,value.pending);
                    previous=Some(status);
                }
            }
        }
    }
}
