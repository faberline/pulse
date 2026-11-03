use pulse_link::{
    cli,
    client::{Config, NativeVault},
    protocol::ClientRole,
};

fn failure_summary(error: &pulse_link::Error) -> &'static str {
    use pulse_link::Error;
    match error {
        Error::CredentialStore(_) => {
            "macOS Keychain access failed. Check system credential access."
        }
        Error::Rejected(reason) => match reason.as_str() {
            "Keychain access failed" | "Keychain save failed" | "Keychain worker failed" => {
                "macOS Keychain access failed. Check system credential access."
            }
            "CLI startup unconfirmed; check pairing and Keychain" => {
                "CLI startup timed out. Pairing and Keychain status are unconfirmed."
            }
            "text file selection rejected; no new message submitted" => {
                "Text file selection failed. Check the file type, path and size."
            }
            "input was not saved; check connection identity, size and queue capacity" => {
                "Input was not saved. Check pairing, input size and queue capacity."
            }
            "CLI pairing unconfirmed; check original pairing state" => {
                "Pairing is unconfirmed. Check the reason above."
            }
            _ => "The request could not be confirmed. Check pairing and the server.",
        },
        Error::Io(_) | Error::System(_) => {
            "Local file access failed. Check the private data directory."
        }
        Error::Database(_) => "Local journal access failed. Check the private data directory.",
        Error::CredentialRejected | Error::PairingRejected => {
            "Pairing was rejected. Check the device identity before pairing again."
        }
        _ => "The request could not be confirmed. Check pairing and the server.",
    }
}

fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args == ["--help"] || args == ["-h"] {
        print!("{}", cli::HELP);
        return;
    }
    let watch = args.first().is_some_and(|s| s == "--watch");
    if watch {
        args.remove(0);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("CLI runtime");
    let result = runtime.block_on(async {
        let (config, pairing) = Config::parse_with_pairing(args, ClientRole::Chat)?;
        cli::run(
            config,
            pairing,
            std::sync::Arc::new(NativeVault),
            tokio::io::BufReader::new(tokio::io::stdin()),
            tokio::io::stdout(),
            watch,
        )
        .await
    });
    runtime.shutdown_timeout(std::time::Duration::from_secs(1));
    if let Err(error) = result {
        eprintln!("{}", failure_summary(&error));
        if let pulse_link::Error::CredentialStore(code) = &error {
            eprintln!("System credential error code: {code}");
        }
        eprintln!(
            "Pulse CLI stopped without confirmation. Check pairing, the server and the same private data directory. Do not resubmit an unknown request as a new task."
        );
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_do_not_echo_error_payloads_or_file_paths() {
        for error in [
            pulse_link::Error::Rejected("PRIVATE_TASK_TEXT token=SECRET".into()),
            pulse_link::Error::Io(std::io::Error::other("/PRIVATE/secret-file SECRET")),
        ] {
            let text = failure_summary(&error);
            assert!(!text.contains("SECRET") && !text.contains("PRIVATE"));
        }
        assert!(
            failure_summary(&pulse_link::Error::Rejected(
                "Keychain access failed".into()
            ))
            .contains("Keychain access failed")
        );
    }
}
