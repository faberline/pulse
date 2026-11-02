//! The only platform FFI boundary. All calls stay inside the bounded child.
mod ax;

use crate::{contract::*, private_png};
use block2::RcBlock;
use objc2::{AnyThread, rc::Retained};
use objc2_app_kit::{
    NSApplicationActivationPolicy, NSBitmapImageFileType, NSBitmapImageRep, NSRunningApplication,
};
use objc2_core_foundation::{CFRetained, CFRunLoop, kCFRunLoopDefaultMode};
use objc2_core_graphics::{CGImage, CGPreflightScreenCaptureAccess};
use objc2_foundation::{NSDictionary, NSError};
use objc2_screen_capture_kit::{
    SCContentFilter, SCScreenshotManager, SCShareableContent, SCStreamConfiguration, SCWindow,
};
use pulse_core::{
    ComputerBinding, ComputerIntent, ComputerObservation, ComputerRect, ComputerSelection,
    ComputerTarget,
};
use std::{
    path::Path,
    sync::mpsc,
    time::{Duration, Instant},
};

// SAFETY: ScreenCaptureKit supplies an immutable shareable-content snapshot.
// Only ownership is transferred from its completion callback to this child's
// main thread. We do not mutate the snapshot or expose it to concurrent callers.
struct Snapshot(Retained<SCShareableContent>);
unsafe impl Send for Snapshot {}
// SAFETY: CGImage is immutable. Retain/release are thread safe, and a single
// owning reference is transferred to the main thread for PNG encoding.
struct Image(CFRetained<CGImage>);
unsafe impl Send for Image {}

pub(super) fn permissions() -> Permissions {
    // SAFETY: Both functions only query current process grants; neither prompts.
    unsafe {
        Permissions {
            accessibility: objc2_application_services::AXIsProcessTrusted(),
            screen_capture: CGPreflightScreenCaptureAccess(),
        }
    }
}
fn wait<T>(receiver: mpsc::Receiver<Result<T>>, deadline: Instant) -> Result<T> {
    loop {
        match receiver.try_recv() {
            Ok(value) => return value,
            Err(mpsc::TryRecvError::Disconnected) => return Err(Error::CaptureFailed),
            Err(_) => {}
        }
        if Instant::now() >= deadline {
            return Err(Error::EvidenceLimit);
        }
        // Pump main-thread callbacks instead of blocking a framework completion.
        // SAFETY: The process uses CoreFoundation's static default run-loop mode.
        unsafe {
            CFRunLoop::run_in_mode(kCFRunLoopDefaultMode, 0.005, true);
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn snapshot(deadline: Instant) -> Result<Retained<SCShareableContent>> {
    let (sender, receiver) = mpsc::channel();
    let block = RcBlock::new(
        move |content: *mut SCShareableContent, error: *mut NSError| {
            // SAFETY: Completion arguments are valid only for this callback. Retain
            // the snapshot before returning. Send failure drops its owning reference.
            let value = if !error.is_null() {
                Err(Error::TargetUnavailable)
            } else {
                unsafe { Retained::retain(content) }
                    .map(Snapshot)
                    .ok_or(Error::TargetUnavailable)
            };
            let _ = sender.send(value);
        },
    );
    // SAFETY: The API copies its completion block. The snapshot is read-only.
    unsafe {
        SCShareableContent::getShareableContentExcludingDesktopWindows_onScreenWindowsOnly_completionHandler(true,true,&block);
    }
    Ok(wait(receiver, deadline)?.0)
}
fn launch(target: &ComputerTarget) -> Result<i64> {
    let app = NSRunningApplication::runningApplicationWithProcessIdentifier(target.pid)
        .ok_or(Error::TargetUnavailable)?;
    if app.isTerminated()
        || app
            .bundleIdentifier()
            .is_none_or(|s| s.to_string() != target.bundle_id)
    {
        return Err(Error::TargetUnavailable);
    }
    let time = app
        .launchDate()
        .ok_or(Error::TargetUnavailable)?
        .timeIntervalSince1970()
        * 1000.;
    if !time.is_finite() || time <= 0. || time > i64::MAX as f64 {
        return Err(Error::TargetUnavailable);
    }
    Ok(time as i64)
}
fn window(target: &ComputerTarget, deadline: Instant) -> Result<Retained<SCWindow>> {
    let content = snapshot(deadline)?;
    // SAFETY: These are retained immutable framework snapshots. No external
    // pointers or window handles are dereferenced by Rust.
    unsafe {
        let matches: Vec<_> = content
            .windows()
            .iter()
            .filter(|w| w.windowID() == target.window_id)
            .collect();
        if matches.len() != 1 {
            return Err(Error::TargetUnavailable);
        }
        let w = matches.into_iter().next().ok_or(Error::TargetUnavailable)?;
        let app = w.owningApplication().ok_or(Error::TargetUnavailable)?;
        if app.processID() != target.pid
            || app.bundleIdentifier().to_string() != target.bundle_id
            || !w.isOnScreen()
            || w.windowLayer() != 0
        {
            return Err(Error::TargetUnavailable);
        }
        title(&w)?;
        rect(&w)?;
        Ok(w)
    }
}
fn title(window: &SCWindow) -> Result<String> {
    // SAFETY: SCWindow is a live retained snapshot. Title is returned retained.
    let text = unsafe { window.title() }
        .ok_or(Error::TargetUnavailable)?
        .to_string();
    if text.is_empty() || text.len() > 512 {
        return Err(Error::TargetUnavailable);
    }
    Ok(text)
}
fn rect(window: &SCWindow) -> Result<ComputerRect> {
    // SAFETY: Frame is copied from the immutable retained snapshot.
    let frame = unsafe { window.frame() };
    let rect = ComputerRect {
        x: frame.origin.x,
        y: frame.origin.y,
        width: frame.size.width,
        height: frame.size.height,
    };
    if !rect.valid() {
        return Err(Error::EvidenceLimit);
    }
    Ok(rect)
}
fn capture(window: &SCWindow, deadline: Instant) -> Result<(Vec<u8>, u32, u32)> {
    // SAFETY: The filter retains exactly the selected window, never a display.
    unsafe {
        let filter =
            SCContentFilter::initWithDesktopIndependentWindow(SCContentFilter::alloc(), window);
        let scale = filter.pointPixelScale() as f64;
        if !scale.is_finite() || !(1. ..=4.).contains(&scale) {
            return Err(Error::EvidenceLimit);
        }
        let frame = rect(window)?;
        let width = (frame.width * scale).ceil() as usize;
        let height = (frame.height * scale).ceil() as usize;
        if !(1..=8192).contains(&width)
            || !(1..=8192).contains(&height)
            || width * height > 16_777_216
        {
            return Err(Error::EvidenceLimit);
        }
        let config = SCStreamConfiguration::new();
        config.setWidth(width);
        config.setHeight(height);
        config.setShowsCursor(false);
        config.setIgnoreShadowsSingleWindow(true);
        let (sender, receiver) = mpsc::channel();
        // Keep borrowed capture parameters alive even if our deadline expires
        // before the framework calls its copied completion block. They are not
        // mutated after submission; only retain/release can run on its queue.
        let lifetime = (filter.clone(), config.clone());
        let block = RcBlock::new(move |image: *mut CGImage, error: *mut NSError| {
            let _keep_alive = &lifetime;
            // The callback's borrowed image is retained before crossing threads.
            let value = if !error.is_null() {
                Err(Error::CaptureFailed)
            } else {
                std::ptr::NonNull::new(image)
                    .map(|p| Image(CFRetained::retain(p)))
                    .ok_or(Error::CaptureFailed)
            };
            let _ = sender.send(value);
        });
        SCScreenshotManager::captureImageWithFilter_configuration_completionHandler(
            &filter,
            &config,
            Some(&block),
        );
        let Image(image) = wait(receiver, deadline)?;
        if CGImage::width(Some(&image)) != width || CGImage::height(Some(&image)) != height {
            return Err(Error::CaptureFailed);
        }
        let rep = NSBitmapImageRep::initWithCGImage(NSBitmapImageRep::alloc(), &image);
        let data = rep
            .representationUsingType_properties(NSBitmapImageFileType::PNG, &NSDictionary::new())
            .ok_or(Error::CaptureFailed)?;
        if data.length() > 4 * 1024 * 1024 {
            return Err(Error::EvidenceLimit);
        }
        // The representation is immutable NSData. Copy its bounded bytes while
        // its owning reference is alive; no borrowed pointer crosses this call.
        Ok((
            data.as_bytes_unchecked().to_vec(),
            width as u32,
            height as u32,
        ))
    }
}
fn observe(
    target: &ComputerTarget,
    binding: &ComputerBinding,
    cache: &Path,
    expected: Option<i64>,
) -> Result<Observation> {
    use sha2::{Digest, Sha256};
    if !binding.valid() || !target.valid() {
        return Err(Error::InvalidRequest);
    }
    if !permissions().can_observe() {
        return Err(Error::PermissionsRequired);
    }
    let deadline = Instant::now() + Duration::from_secs(8);
    let started = launch(target)?;
    if expected.is_some_and(|t| t != started) {
        return Err(Error::TargetChanged);
    }
    let selected = window(target, deadline)?;
    let title = title(&selected)?;
    let frame = rect(&selected)?;
    let catalog = ax::Catalog::new(target.pid, deadline);
    let ax_window = catalog.window(&title, frame)?;
    let (elements, truncated) = catalog.collect(&ax_window)?;
    let observed_at = now();
    let (png, width, height) = capture(&selected, deadline)?;
    let current = window(target, deadline)?;
    if launch(target)? != started
        || self::title(&current)? != title
        || rect(&current)? != frame
        || Instant::now() >= deadline
        || !permissions().can_observe()
    {
        return Err(Error::TargetChanged);
    }
    let current_ax = catalog.window(&title, frame)?;
    let (after, after_truncated) = catalog.collect(&current_ax)?;
    if elements != after || truncated != after_truncated {
        return Err(Error::TargetChanged);
    }
    let path = private_png::save(&png, cache)?;
    let view = ComputerObservation {
        binding: binding.clone(),
        target: target.clone(),
        process_launched_at: started,
        title,
        frame,
        elements,
        truncated,
        nonce: uuid::Uuid::new_v4().to_string(),
        observed_at,
        expires_at: observed_at + 120_000,
        image: pulse_core::ComputerImageRef {
            digest: format!("{:x}", Sha256::digest(&png)),
            length: png.len() as u32,
            width,
            height,
        },
    };
    if !view.valid(now()) {
        return Err(Error::EvidenceLimit);
    }
    Ok(Observation { view, path })
}
fn same_view(a: &ComputerObservation, b: &ComputerObservation) -> bool {
    a.target == b.target
        && a.process_launched_at == b.process_launched_at
        && a.title == b.title
        && a.frame == b.frame
        && a.elements == b.elements
        && a.truncated == b.truncated
        && a.image == b.image
}
fn act(
    target: &ComputerTarget,
    binding: &ComputerBinding,
    intent: &ComputerIntent,
    cache: &Path,
) -> Response {
    let response = |state, error, observation| Response {
        action: Some(ActionReceipt {
            binding: binding.clone(),
            source_nonce: intent.observation.nonce.clone(),
            state,
        }),
        error,
        observation,
        ..Response::empty()
    };
    let prepare = || -> Result<_> {
        if intent.observation.target != *target {
            return Err(Error::InvalidRequest);
        }
        let fresh = observe(
            target,
            binding,
            cache,
            Some(intent.observation.process_launched_at),
        )?;
        if !same_view(&fresh.view, &intent.observation) {
            return Err(Error::TargetChanged);
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        let selected = window(target, deadline)?;
        if title(&selected)? != intent.observation.title
            || rect(&selected)? != intent.observation.frame
        {
            return Err(Error::TargetChanged);
        }
        let catalog = ax::Catalog::new(target.pid, deadline);
        let window = catalog.window(&intent.observation.title, intent.observation.frame)?;
        let element = catalog.resolve(&window, &intent.element_id)?;
        let (elements, truncated) = catalog.collect(&window)?;
        if elements != intent.observation.elements
            || truncated != intent.observation.truncated
            || launch(target)? != intent.observation.process_launched_at
            || !permissions().can_observe()
            || Instant::now() >= deadline
            || now() >= intent.observation.expires_at
        {
            return Err(Error::TargetChanged);
        }
        Ok(element)
    };
    let element = match prepare() {
        Ok(e) => e,
        Err(e) => return response(ActionState::NotExecuted, Some(e), None),
    };
    // The only effect call. An AX failure is Unknown: the app may have handled it.
    if ax::effect(&element, &intent.effect).is_err() {
        return response(
            ActionState::Unknown,
            Some(Error::AccessibilityUnavailable),
            None,
        );
    }
    match observe(
        target,
        binding,
        cache,
        Some(intent.observation.process_launched_at),
    ) {
        Ok(after) => response(ActionState::Completed, None, Some(after)),
        Err(_) => response(ActionState::Unknown, Some(Error::CaptureFailed), None),
    }
}
fn list() -> Result<Vec<pulse_core::ComputerWindowChoice>> {
    if !permissions().screen_capture {
        return Err(Error::PermissionsRequired);
    }
    let limits = [
        "PULSE_COMPUTER_BUNDLE_ID",
        "PULSE_COMPUTER_PID",
        "PULSE_COMPUTER_WINDOW_ID",
    ]
    .map(|key| std::env::var(key).ok());
    let content = snapshot(Instant::now() + Duration::from_secs(8))?;
    let mut found = Vec::new();
    // SAFETY: Read-only retained snapshots. Apply operator filters before title
    // access. No list or application names are sent to the server or model.
    unsafe {
        for window in content.windows().iter() {
            if found.len() >= 64 {
                break;
            }
            if !window.isOnScreen() || window.windowLayer() != 0 {
                continue;
            }
            let Some(owner) = window.owningApplication() else {
                continue;
            };
            let bundle = owner.bundleIdentifier().to_string();
            let pid = owner.processID();
            let id = window.windowID();
            if bundle == "com.faberline.pulse" || pid == std::process::id() as i32 {
                continue;
            }
            if limits[0].as_ref().is_some_and(|v| *v != bundle)
                || limits[1].as_ref().is_some_and(|v| *v != pid.to_string())
                || limits[2].as_ref().is_some_and(|v| *v != id.to_string())
            {
                continue;
            }
            let target = ComputerTarget {
                bundle_id: bundle,
                pid,
                window_id: id,
            };
            if !target.valid() {
                continue;
            }
            let Some(app) = NSRunningApplication::runningApplicationWithProcessIdentifier(pid)
            else {
                continue;
            };
            if app.activationPolicy() != NSApplicationActivationPolicy::Regular
                || app.isTerminated()
                || app
                    .bundleIdentifier()
                    .is_none_or(|s| s.to_string() != target.bundle_id)
            {
                continue;
            }
            let (Ok(started), Ok(title)) = (launch(&target), title(&window)) else {
                continue;
            };
            let mut app_name = app
                .localizedName()
                .map(|s| s.to_string())
                .unwrap_or_else(|| owner.applicationName().to_string());
            if app_name.is_empty() {
                app_name = target.bundle_id.clone();
            }
            found.push(pulse_core::ComputerWindowChoice {
                selection: ComputerSelection {
                    target,
                    process_launched_at: started,
                },
                app_name: bounded(&app_name, 128),
                title,
            });
        }
    }
    found.sort_by(|a, b| (&a.app_name, &a.title).cmp(&(&b.app_name, &b.title)));
    while serde_json::to_vec(&found)
        .map_err(|_| Error::EvidenceLimit)?
        .len()
        > 48_000
    {
        found.pop();
    }
    Ok(found)
}
pub(super) fn list_response() -> Response {
    if objc2::MainThreadMarker::new().is_none() {
        return Response::error(Error::InvalidRequest);
    }
    objc2::rc::autoreleasepool(|_| match list() {
        Ok(windows) => Response {
            windows: Some(windows),
            ..Response::empty()
        },
        Err(e) => Response::error(e),
    })
}
pub(super) fn handle(target: &ComputerTarget, cache: &Path, request: Request) -> Response {
    if objc2::MainThreadMarker::new().is_none() {
        return Response::error(Error::InvalidRequest);
    }
    // All AppKit objects and the run loop belong to this child's main thread.
    objc2::rc::autoreleasepool(|_| {
        let result = (|| -> Result<Response> {
            Ok(match request {
                Request::Status {} => Response::empty(),
                Request::Probe {
                    expected_process_launched_at,
                } => {
                    let expected = launch_time(&expected_process_launched_at)?;
                    if !permissions().can_observe() {
                        return Err(Error::PermissionsRequired);
                    }
                    if launch(target)? != expected {
                        return Err(Error::TargetChanged);
                    }
                    let deadline = Instant::now() + Duration::from_secs(8);
                    let selected = window(target, deadline)?;
                    ax::Catalog::new(target.pid, deadline)
                        .window(&title(&selected)?, rect(&selected)?)?;
                    if launch(target)? != expected {
                        return Err(Error::TargetChanged);
                    }
                    Response {
                        selection: Some(ComputerSelection {
                            target: target.clone(),
                            process_launched_at: expected,
                        }),
                        ..Response::empty()
                    }
                }
                Request::Observe {
                    binding,
                    expected_process_launched_at,
                } => Response {
                    observation: Some(observe(
                        target,
                        &binding,
                        cache,
                        expected_process_launched_at
                            .as_deref()
                            .map(launch_time)
                            .transpose()?,
                    )?),
                    ..Response::empty()
                },
                Request::Act { binding, intent } => act(target, &binding, &intent, cache),
            })
        })();
        result.unwrap_or_else(Response::error)
    })
}
