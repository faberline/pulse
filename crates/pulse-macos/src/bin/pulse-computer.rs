use pulse_macos::{
    Error, Request, Response,
    contract::{LIMIT, now},
};
use std::{
    io::{Read, Write},
    path::PathBuf,
};

fn run() -> Response {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args == ["--status"] {
        return Response::empty();
    }
    // Listing is local-only. Operator restrictions are read by the adapter before
    // inspecting titles. It never enters task history, server or model context.
    if args == ["--list"] {
        return pulse_macos::list_windows();
    }
    if args.len() != 8
        || args[0] != "--bundle-id"
        || args[2] != "--pid"
        || args[4] != "--window-id"
        || args[6] != "--cache"
    {
        return Response::error(Error::InvalidRequest);
    }
    let target = pulse_core::ComputerTarget {
        bundle_id: args[1].clone(),
        pid: args[3].parse().unwrap_or(0),
        window_id: args[5].parse().unwrap_or(0),
    };
    let cache = PathBuf::from(&args[7]);
    if !target.valid() || !cache.is_absolute() {
        return Response::error(Error::InvalidRequest);
    }
    let mut bytes = Vec::new();
    if std::io::stdin()
        .take(LIMIT as u64 + 1)
        .read_to_end(&mut bytes)
        .is_err()
    {
        return Response::error(Error::InvalidRequest);
    }
    match Request::decode(&bytes, now()) {
        Ok(request) => pulse_macos::handle(&target, &cache, request),
        Err(error) => Response::error(error),
    }
}
fn main() {
    // A stalled stdin or system call cannot retain this child indefinitely.
    std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_secs(15));
        std::process::exit(74);
    });
    let response = run();
    let Ok(mut bytes) = serde_json::to_vec(&response) else {
        std::process::exit(75)
    };
    if bytes.len() >= LIMIT {
        std::process::exit(75)
    }
    bytes.push(b'\n');
    if std::io::stdout().write_all(&bytes).is_err() {
        std::process::exit(75)
    }
}
