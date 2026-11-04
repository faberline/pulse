#[tokio::main]
async fn main() {
    let mut args = std::env::args_os().skip(1);
    let Some(directory) = args.next() else {
        std::process::exit(125)
    };
    if args.next().is_some() {
        std::process::exit(125);
    }
    // stderr is the private parent socket. Never print diagnostics into it.
    let status = pulse_link::process_guardian::run(std::path::Path::new(&directory))
        .await
        .unwrap_or(125);
    std::process::exit(status);
}
