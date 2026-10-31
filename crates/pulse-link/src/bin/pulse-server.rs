use pulse_link::{
    Result,
    journal::private_write,
    now,
    protocol::ClientRole,
    server::{Hub, router},
};
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    if run().await.is_err() {
        eprintln!(
            "Pulse server stopped. Check private database permissions and command arguments."
        );
        std::process::exit(1);
    }
}
async fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_default();
    let database = PathBuf::from(args.next().ok_or_else(|| {
        pulse_link::Error::Rejected(
            "use pulse-server serve|pair-chat|pair-local|revoke PRIVATE_DB [PORT|PRIVATE_CODE_FILE|DEVICE_ID]"
                .into(),
        )
    })?);
    let value = args.next();
    if args.next().is_some() {
        return Err(pulse_link::Error::Rejected("too many arguments".into()));
    }
    let _server_lock = if mode == "serve" {
        use std::os::unix::fs::OpenOptionsExt;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(database.with_extension("server.lock"))?;
        fs2::FileExt::try_lock_exclusive(&file)
            .map_err(|_| pulse_link::Error::Rejected("another server owns this database".into()))?;
        Some(file)
    } else {
        None
    };
    let hub = Hub::new(pulse_core::Store::open(database, "pulse-owner").await?);
    match mode.as_str() {
        "serve" => {
            hub.recover().await?;
            let port: u16 = value
                .as_deref()
                .unwrap_or("8721")
                .parse()
                .map_err(|_| pulse_link::Error::Rejected("invalid port".into()))?;
            let listener =
                tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
            eprintln!(
                "Pulse listener: http://{} (loopback; production requires a TLS reverse proxy)",
                listener.local_addr()?
            );
            axum::serve(listener, router(hub))
                .with_graceful_shutdown(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await?;
        }
        "pair-chat" | "pair-local" => {
            let file = value.ok_or_else(|| {
                pulse_link::Error::Rejected("private output file is required".into())
            })?;
            let role = if mode == "pair-chat" {
                ClientRole::Chat
            } else {
                ClientRole::LocalExecutor
            };
            let code = hub.pairing(role, now()).await?;
            private_write(std::path::Path::new(&file), code.as_bytes())?;
            eprintln!(
                "One-use pairing code saved to the requested private file. It expires in ten minutes."
            );
        }
        "revoke" => {
            let device =
                value.ok_or_else(|| pulse_link::Error::Rejected("device ID is required".into()))?;
            hub.revoke(&device).await?;
            eprintln!("Device revoked. Existing leases are invalid.");
        }
        _ => return Err(pulse_link::Error::Rejected("unknown mode".into())),
    }
    Ok(())
}
