use pulse_link::{
    cli,
    client::{Config, NativeVault},
    protocol::ClientRole,
};

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
    if result.is_err() {
        eprintln!(
            "Pulse CLI stopped without confirmation. Check pairing, the server and the same private data directory. Do not resubmit an unknown request as a new task."
        );
        std::process::exit(1);
    }
}
