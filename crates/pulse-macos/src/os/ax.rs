use crate::contract::{Error, Result, bounded};
use objc2_application_services::{AXError, AXUIElement, AXValue, AXValueType};
use objc2_core_foundation::{CFArray, CFBoolean, CFNumber, CFRetained, CFString, CFType};
use objc2_core_foundation::{CGPoint, CGSize};
use pulse_core::{ComputerEffect, ComputerElement, ComputerRect, computer_element_path};
use std::{collections::VecDeque, ptr::NonNull, time::Instant};

pub(super) struct Catalog {
    pid: i32,
    deadline: Instant,
}
impl Catalog {
    pub(super) fn new(pid: i32, deadline: Instant) -> Self {
        Self { pid, deadline }
    }
    fn check(&self) -> Result<()> {
        if Instant::now() >= self.deadline {
            Err(Error::EvidenceLimit)
        } else {
            Ok(())
        }
    }
    fn value(&self, element: &AXUIElement, key: &str) -> Result<Option<CFRetained<CFType>>> {
        self.check()?;
        let mut value = std::ptr::null();
        // SAFETY: element is retained and the attribute is a valid CFString.
        // The Copy rule transfers +1 ownership into CFRetained exactly once.
        let code = unsafe {
            element.copy_attribute_value(&CFString::from_str(key), NonNull::from(&mut value))
        };
        if matches!(code, AXError::AttributeUnsupported | AXError::NoValue) {
            return Ok(None);
        }
        if code != AXError::Success {
            return Err(Error::AccessibilityUnavailable);
        }
        Ok(NonNull::new(value.cast_mut()).map(|p| unsafe { CFRetained::from_raw(p) }))
    }
    fn text(&self, element: &AXUIElement, key: &str) -> Result<Option<String>> {
        Ok(self
            .value(element, key)?
            .and_then(|v| v.downcast::<CFString>().ok())
            .map(|s| s.to_string()))
    }
    fn children(
        &self,
        element: &AXUIElement,
        key: &str,
        limit: usize,
    ) -> Result<Vec<CFRetained<AXUIElement>>> {
        self.check()?;
        let key = CFString::from_str(key);
        let mut count = 0;
        // SAFETY: FFI out-pointers reference initialized stack slots.
        let code = unsafe { element.attribute_value_count(&key, NonNull::from(&mut count)) };
        if matches!(code, AXError::AttributeUnsupported | AXError::NoValue) {
            return Ok(vec![]);
        }
        if code != AXError::Success || count < 0 || count as usize > limit {
            return Err(Error::EvidenceLimit);
        }
        if count == 0 {
            return Ok(vec![]);
        }
        let mut pointer = std::ptr::null();
        let code =
            unsafe { element.copy_attribute_values(&key, 0, count, NonNull::from(&mut pointer)) };
        if code != AXError::Success {
            return Err(Error::AccessibilityUnavailable);
        }
        let pointer = NonNull::new(pointer.cast_mut()).ok_or(Error::AccessibilityUnavailable)?;
        // SAFETY: AX's Copy API returns an owned CFArray of CFType objects. We
        // check its length and downcast every retained member before using it.
        let array = unsafe { CFRetained::<CFArray>::from_raw(pointer) };
        if array.len() != count as usize {
            return Err(Error::AccessibilityUnavailable);
        }
        unsafe { array.cast_unchecked::<CFType>() }
            .iter()
            .map(|value| {
                value
                    .downcast::<AXUIElement>()
                    .map_err(|_| Error::AccessibilityUnavailable)
            })
            .collect()
    }
    fn owner(&self, element: &AXUIElement) -> Result<()> {
        self.check()?;
        let mut pid = 0;
        // SAFETY: A retained AX element and a valid pid out-pointer.
        if unsafe { element.pid(NonNull::from(&mut pid)) } != AXError::Success || pid != self.pid {
            return Err(Error::TargetChanged);
        }
        Ok(())
    }
    pub(super) fn window(
        &self,
        title: &str,
        frame: ComputerRect,
    ) -> Result<CFRetained<AXUIElement>> {
        // SAFETY: Both constructors return owned CF objects. The selected PID
        // is verified against its app instance before this catalog is created.
        let (app, system) = unsafe {
            (
                AXUIElement::new_application(self.pid),
                AXUIElement::new_system_wide(),
            )
        };
        if unsafe { system.set_messaging_timeout(0.25) } != AXError::Success {
            return Err(Error::AccessibilityUnavailable);
        }
        let mut matches = Vec::new();
        for window in self.children(&app, "AXWindows", 32)? {
            self.owner(&window)?;
            if self.text(&window, "AXTitle")?.as_deref() != Some(title) {
                continue;
            }
            let position = self
                .value(&window, "AXPosition")?
                .and_then(|v| v.downcast::<AXValue>().ok());
            let size = self
                .value(&window, "AXSize")?
                .and_then(|v| v.downcast::<AXValue>().ok());
            let (Some(position), Some(size)) = (position, size) else {
                continue;
            };
            let mut point = CGPoint::ZERO;
            let mut extent = CGSize::ZERO;
            // SAFETY: AXValue checks its tag and copies only a CGPoint/CGSize
            // into correctly sized and aligned stack values.
            let valid = unsafe {
                position.value(AXValueType::CGPoint, NonNull::from(&mut point).cast())
                    && size.value(AXValueType::CGSize, NonNull::from(&mut extent).cast())
            };
            if valid
                && (point.x - frame.x).abs() < 0.5
                && (point.y - frame.y).abs() < 0.5
                && (extent.width - frame.width).abs() < 0.5
                && (extent.height - frame.height).abs() < 0.5
            {
                matches.push(window)
            }
        }
        if matches.len() != 1 {
            return Err(Error::UnsupportedWindow);
        }
        Ok(matches.remove(0))
    }
    fn safe(&self, element: &AXUIElement) -> Result<()> {
        self.owner(element)?;
        if self.text(element, "AXSubrole")?.as_deref() == Some("AXSecureTextField")
            || self.text(element, "AXRole")?.as_deref() == Some("AXSecureTextField")
        {
            return Err(Error::TargetChanged);
        }
        Ok(())
    }
    pub(super) fn resolve(
        &self,
        window: &AXUIElement,
        id: &str,
    ) -> Result<CFRetained<AXUIElement>> {
        if !computer_element_path(id) {
            return Err(Error::InvalidRequest);
        }
        // SAFETY: Borrowed AX objects are retained before returning ownership.
        let mut element = unsafe { CFRetained::retain(NonNull::from(window)) };
        for part in id.split('.').skip(1) {
            self.safe(&element)?;
            let index = part.parse::<usize>().map_err(|_| Error::InvalidRequest)?;
            element = self
                .children(&element, "AXChildren", 128)?
                .get(index)
                .cloned()
                .ok_or(Error::TargetChanged)?;
        }
        self.safe(&element)?;
        Ok(element)
    }
    pub(super) fn collect(&self, window: &AXUIElement) -> Result<(Vec<ComputerElement>, bool)> {
        // SAFETY: A borrowed live AX element is retained for the bounded walk.
        let window = unsafe { CFRetained::retain(NonNull::from(window)) };
        let mut queue = VecDeque::from([(window, "window".to_owned(), 0)]);
        let mut visited = Vec::<CFRetained<AXUIElement>>::new();
        let mut output = Vec::new();
        let mut truncated = false;
        let mut controls = 0;
        while let Some((element, path, depth)) = queue.pop_front() {
            self.check()?;
            if visited.contains(&element) {
                continue;
            }
            if visited.len() >= 128 {
                truncated = true;
                break;
            }
            visited.push(element.clone());
            self.owner(&element)?;
            let role = self.text(&element, "AXRole")?.unwrap_or_default();
            let subrole = self.text(&element, "AXSubrole")?.unwrap_or_default();
            // Never read a secure control's label, value, actions or children.
            if subrole == "AXSecureTextField" || role == "AXSecureTextField" {
                continue;
            }
            let enabled = self
                .value(&element, "AXEnabled")?
                .and_then(|v| v.downcast::<CFBoolean>().ok())
                .is_some_and(|v| v.as_bool());
            let mut label = self.text(&element, "AXTitle")?.unwrap_or_default();
            if label.is_empty() {
                label = self.text(&element, "AXDescription")?.unwrap_or_default()
            }
            let mut text = None;
            let mut press = false;
            let mut set_text = false;
            if role == "AXButton" && enabled && controls < 16 {
                let mut pointer = std::ptr::null();
                // SAFETY: Copy rule and checked CF member types, as above.
                if unsafe { element.copy_action_names(NonNull::from(&mut pointer)) }
                    == AXError::Success
                    && let Some(pointer) = NonNull::new(pointer.cast_mut())
                {
                    let names = unsafe { CFRetained::<CFArray>::from_raw(pointer) };
                    if names.len() <= 16 {
                        press = unsafe { names.cast_unchecked::<CFType>() }.iter().any(|v| {
                            v.downcast::<CFString>()
                                .is_ok_and(|s| s.to_string() == "AXPress")
                        });
                    }
                }
            }
            if matches!(role.as_str(), "AXTextField" | "AXTextArea") {
                let count = self
                    .value(&element, "AXNumberOfCharacters")?
                    .and_then(|v| v.downcast::<CFNumber>().ok())
                    .and_then(|v| v.as_i64());
                if count.is_some_and(|n| (0..=1024).contains(&n))
                    && let Some(value) = self.text(&element, "AXValue")?
                    && value.len() <= 1024
                {
                    text = Some(value);
                    let mut writable = 0;
                    set_text = enabled
                        && controls < 16
                        && unsafe {
                            element.is_attribute_settable(
                                &CFString::from_static_str("AXValue"),
                                NonNull::from(&mut writable),
                            )
                        } == AXError::Success
                        && writable != 0;
                }
            } else if role == "AXStaticText" {
                text = self.text(&element, "AXValue")?.map(|s| bounded(&s, 512));
            }
            if press || set_text {
                controls += 1
            }
            if !label.is_empty() || text.is_some() || press || set_text {
                output.push(ComputerElement {
                    id: path.clone(),
                    role: bounded(&role, 64),
                    label: bounded(&label, 256),
                    value: text,
                    can_press: press,
                    can_set_text: set_text,
                });
            }
            if depth >= 8 {
                truncated = true;
                continue;
            }
            for (index, child) in self
                .children(&element, "AXChildren", 128)?
                .into_iter()
                .enumerate()
            {
                queue.push_back((child, format!("{path}.{index}"), depth + 1));
            }
        }
        if serde_json::to_vec(&output)
            .map_err(|_| Error::EvidenceLimit)?
            .len()
            > 32_768
        {
            return Err(Error::EvidenceLimit);
        }
        Ok((output, truncated))
    }
}
pub(super) fn effect(element: &AXUIElement, effect: &ComputerEffect) -> Result<()> {
    // SAFETY: The caller freshly resolved this exact non-secure element and
    // verified its current capability, app identity and original approved view.
    // One fixed AX command is sent. Every error is Unknown, never retried here.
    let code = unsafe {
        match effect {
            ComputerEffect::Press => element.perform_action(&CFString::from_static_str("AXPress")),
            ComputerEffect::SetText { text } => element.set_attribute_value(
                &CFString::from_static_str("AXValue"),
                &CFString::from_str(text),
            ),
        }
    };
    if code == AXError::Success {
        Ok(())
    } else {
        Err(Error::AccessibilityUnavailable)
    }
}
