use pulse_link::{
    Result,
    journal::private_write,
    now,
    protocol::ClientRole,
    server::{Hub, router},
};
use std::{future::IntoFuture, path::PathBuf, time::Duration};

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
            "use pulse-server serve|pair-chat|pair-local|revoke PRIVATE_DB [VALUE], or check-backup PRIVATE_DB BACKUP NEW_DIRECTORY"
                .into(),
        )
    })?);
    let value = args.next();
    if mode == "check-backup" {
        let backup =
            value.ok_or_else(|| pulse_link::Error::Rejected("encrypted backup required".into()))?;
        let destination = args.next().ok_or_else(|| {
            pulse_link::Error::Rejected("new candidate directory required".into())
        })?;
        if args.next().is_some() {
            return Err(pulse_link::Error::Rejected("too many arguments".into()));
        }
        let core = pulse_core::Store::open(database, "pulse-owner").await?;
        let check = core
            .check_backup(
                std::path::Path::new(&backup),
                std::path::Path::new(&destination),
                &pulse_link::remote::versions(),
                now(),
            )
            .await?;
        eprintln!(
            "Backup checked in quarantine: {} tasks, {} unknown tasks, {} live memories. No tasks started.",
            check.tasks, check.unknown_tasks, check.memories
        );
        core.close().await;
        return Ok(());
    }
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
            hub.core.maintain(now()).await?;
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
            let remote = pulse_link::remote::run(hub.clone());
            let maintenance = pulse_link::maintenance::run(hub.core.clone());
            let schedules = pulse_link::schedules::run(hub.clone());
            let (shutdown, stopping) = tokio::sync::oneshot::channel::<()>();
            let server = axum::serve(listener, router(hub))
                .with_graceful_shutdown(async {
                    let _ = stopping.await;
                })
                .into_future();
            tokio::pin!(server);
            tokio::select! {
                result = remote => result?,
                result = maintenance => result?,
                result = schedules => result?,
                result = &mut server => result?,
                signal = tokio::signal::ctrl_c() => {
                    signal?;
                    let _ = shutdown.send(());
                    // Stop polling the work loops and bound HTTP draining. An
                    // incomplete request body must not keep this process alive.
                    // Cancelling a provider future never clears its reservation.
                    match tokio::time::timeout(Duration::from_secs(2), &mut server).await {
                        Ok(result) => result?,
                        Err(_) => eprintln!("HTTP drain deadline reached. Pending results remain unconfirmed."),
                    }
                }
            }
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
