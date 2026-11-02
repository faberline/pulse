//! macOS adapter. Task state, grants and budgets remain in the Rust server.
pub mod contract;
#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
mod os;
#[cfg(any(target_os = "macos", test))]
mod private_png;

pub use contract::*;

pub fn list_windows() -> Response {
    #[cfg(target_os = "macos")]
    {
        os::list_response()
    }
    #[cfg(not(target_os = "macos"))]
    {
        Response::error(Error::UnsupportedWindow)
    }
}

pub fn permissions() -> Permissions {
    #[cfg(target_os = "macos")]
    {
        os::permissions()
    }
    #[cfg(not(target_os = "macos"))]
    {
        Permissions {
            accessibility: false,
            screen_capture: false,
        }
    }
}

pub fn handle(
    target: &pulse_core::ComputerTarget,
    cache: &std::path::Path,
    request: Request,
) -> Response {
    if !target.valid() || !cache.is_absolute() || request.validate(contract::now()).is_err() {
        return Response::error(Error::InvalidRequest);
    }
    #[cfg(target_os = "macos")]
    {
        os::handle(target, cache, request)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (target, cache, request);
        Response::error(Error::UnsupportedWindow)
    }
}
