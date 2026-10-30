use pulse_link::{
    Result,
    client::{Config, Device, NativeVault},
    protocol::{MAX_FRAME, MAX_IPC, NativeCommand, NativeState, parse},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::{mpsc, watch},
};

#[tokio::main]
async fn main() {
    if run().await.is_err() {
        eprintln!(
            "Pulse helper stopped. Check the server, private data directory and Keychain access."
        );
        std::process::exit(1);
    }
}
async fn run() -> Result<()> {
    let config = Config::parse(std::env::args().skip(1))?;
    let device = Device::open(config, std::sync::Arc::new(NativeVault)).await?;
    let (commands, receiver) = mpsc::channel(32);
    let (states, mut state) = watch::channel(NativeState::default());
    let mut input = tokio::spawn(async move {
        let mut reader = BufReader::new(tokio::io::stdin());
        loop {
            // fill_buf caps each allocation; an untrusted line cannot grow forever.
            let mut bytes = Vec::new();
            loop {
                let chunk = reader.fill_buf().await?;
                if chunk.is_empty() {
                    return Ok::<_, pulse_link::Error>(());
                }
                let end = chunk
                    .iter()
                    .position(|b| *b == b'\n')
                    .map(|i| i + 1)
                    .unwrap_or(chunk.len());
                if bytes.len() + end > MAX_FRAME {
                    return Err(pulse_link::Error::Rejected("IPC line too large".into()));
                }
                bytes.extend_from_slice(&chunk[..end]);
                reader.consume(end);
                if bytes.ends_with(b"\n") {
                    break;
                }
            }
            let command: NativeCommand = parse(&bytes)?;
            if commands.send(command).await.is_err() {
                return Ok(());
            }
        }
    });
    let mut output = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while state.changed().await.is_ok() {
            let mut bytes = serde_json::to_vec(&*state.borrow_and_update())?;
            bytes.push(b'\n');
            if bytes.len() > MAX_IPC {
                return Err(pulse_link::Error::Rejected("IPC state too large".into()));
            }
            stdout.write_all(&bytes).await?;
            stdout.flush().await?;
        }
        Ok::<_, pulse_link::Error>(())
    });
    let result = tokio::select! {
        result=device.run(receiver,states)=>result,
        result=&mut input=>result.map_err(|_|pulse_link::Error::Rejected("IPC input stopped".into()))?,
        result=&mut output=>result.map_err(|_|pulse_link::Error::Rejected("IPC output stopped".into()))?,
    };
    input.abort();
    output.abort();
    result
}
