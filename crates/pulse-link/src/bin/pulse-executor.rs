//! An independent local process. It has no dependency on a desktop window.
use pulse_link::{
    Result,
    client::{Config, Device, NativeVault},
    journal::private_read,
    protocol::{NativeCommand, NativeState},
};
use tokio::sync::{mpsc, watch};

#[tokio::main]
async fn main() {
    if run().await.is_err() {
        eprintln!("Local executor stopped. Check its arguments, private directory and Keychain.");
        std::process::exit(1);
    }
}
async fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut connection = Vec::new();
    let mut pairing = None;
    while let Some(arg) = args.next() {
        if arg == "--pair-code-file" {
            if pairing.is_some() {
                return Err(pulse_link::Error::Rejected("duplicate pair file".into()));
            }
            pairing = Some(private_read(std::path::Path::new(
                &args
                    .next()
                    .ok_or_else(|| pulse_link::Error::Rejected("pair file required".into()))?,
            ))?);
        } else {
            connection.push(arg);
        }
    }
    let device = Device::open(Config::parse(connection)?, std::sync::Arc::new(NativeVault)).await?;
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
