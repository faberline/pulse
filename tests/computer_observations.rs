use pulse_core::*;
use serde_json::json;

fn observation() -> ComputerObservation {
    serde_json::from_value(json!({
        "binding":{"operation_id":"read-1","task_id":"task","revision":"1","device":uuid::Uuid::new_v4().to_string(),"epoch":"2"},
        "target":{"bundle_id":"com.faberline.PulseComputerFixture","pid":42,"window_id":7},
        "process_launched_at":1,"title":"Owned fixture","frame":{"x":10.0,"y":20.0,"width":400.0,"height":300.0},
        "elements":[{"id":"window.0","role":"AXButton","label":"Count once","value":null,"can_press":true,"can_set_text":false}],
        "truncated":false,"nonce":uuid::Uuid::new_v4().to_string(),"observed_at":1000,"expires_at":121000,
        "image":{"digest":"a".repeat(64),"length":100,"width":800,"height":600}
    })).unwrap()
}
#[test]
fn typed_computer_observation_rejects_wrong_roles_paths_dates_and_limits() {
    let base = observation();
    assert!(base.valid(1100));
    for edit in [
        "target",
        "binding",
        "epoch",
        "nonce",
        "future",
        "expiry",
        "launch",
        "frame",
        "image",
        "role",
        "secure",
        "path",
        "duplicate",
        "control_limit",
        "node_limit",
        "text_limit",
    ] {
        let mut o = base.clone();
        match edit {
            "target" => o.target.bundle_id = "com.*".into(),
            "binding" => o.binding.device = "not-a-device".into(),
            "epoch" => o.binding.epoch = 0,
            "nonce" => o.nonce = "not-a-nonce".into(),
            "future" => {
                o.observed_at = 100_000;
                o.expires_at = 220_000;
            }
            "expiry" => o.expires_at = 120_999,
            "launch" => o.process_launched_at = 1001,
            "frame" => o.frame.width = f64::NAN,
            "image" => o.image.width = 8193,
            "role" => o.elements[0].role = "AXTextField".into(),
            "secure" => {
                o.elements[0].role = "AXSecureTextField".into();
                o.elements[0].can_press = false;
            }
            "path" => o.elements[0].id = "window.00".into(),
            "duplicate" => o.elements.push(o.elements[0].clone()),
            "control_limit" => {
                o.elements = (0..17)
                    .map(|i| ComputerElement {
                        id: format!("window.{}.{}", i / 128, i % 128),
                        ..o.elements[0].clone()
                    })
                    .collect()
            }
            "node_limit" => {
                o.elements = (0..129)
                    .map(|i| ComputerElement {
                        id: format!("window.{}.{}", i / 128, i % 128),
                        can_press: false,
                        ..o.elements[0].clone()
                    })
                    .collect()
            }
            "text_limit" => o.elements[0].label = "文".repeat(86),
            _ => unreachable!(),
        }
        assert!(!o.valid(1100), "{edit}");
    }
    assert!(!base.valid(base.expires_at));
    let mut text = base;
    text.elements[0].role = "AXTextField".into();
    text.elements[0].can_press = false;
    text.elements[0].can_set_text = true;
    assert!(!text.valid(1100));
    text.elements[0].value = Some("Visible field".into());
    assert!(text.valid(1100));
}
#[test]
fn native_source_metadata_has_canonical_counters_and_no_private_path_field() {
    let value = serde_json::to_value(observation()).unwrap();
    assert_eq!(value["binding"]["epoch"], "2");
    assert_eq!(value["binding"]["revision"], "1");
    assert!(value["image"].get("path").is_none());
    for invalid in [
        json!(1),
        json!("01"),
        json!("-1"),
        json!("18446744073709551616"),
    ] {
        let mut input = value.clone();
        input["binding"]["revision"] = invalid;
        assert!(serde_json::from_value::<ComputerObservation>(input).is_err());
    }
    let mut input = value;
    input["image"]["path"] = json!("/private/forged");
    assert!(serde_json::from_value::<ComputerObservation>(input).is_err());
}

#[test]
fn computer_scope_has_one_canonical_process_instance_and_archived_evidence_cannot_mix_tools() {
    let observation = observation();
    let selection = observation.selection();
    let scope = selection.scope().unwrap();
    assert!(read_scope(&scope));
    assert_eq!(ComputerSelection::from_scope(&scope), Some(selection));
    for scope in [
        "computer:com.app:01:2:3",
        "computer:com.app:+1:2:3",
        "computer:com.app:1:2:03",
        "computer:com.app:0:2:3",
        "computer:com.app:1:2:0",
        "computer:com.app:2147483648:2:3",
        "computer:com.*:1:2:3",
        "computer:com.app:1:2:3:extra",
        "computer:com.app:1:2:9223372036854775808",
    ] {
        assert!(!read_scope(scope), "{scope}");
    }
    let mut value = serde_json::to_value(ReadOutcome::text("fixture".into(), false)).unwrap();
    value["computer"] = serde_json::to_value(observation).unwrap();
    let outcome: ReadOutcome = serde_json::from_value(value.clone()).unwrap();
    assert!(outcome.valid());
    let browser = serde_json::json!({"session_id":uuid::Uuid::new_v4().to_string(),"nonce":uuid::Uuid::new_v4().to_string(),"url":"https://example.com/","title":"fixture","png_digest":"a".repeat(64),"elements":[],"observed_at":1000,"expires_at":121000});
    assert!(
        serde_json::from_value::<BrowserObservation>(browser.clone())
            .unwrap()
            .valid()
    );
    value["observation"] = browser;
    assert!(
        !serde_json::from_value::<ReadOutcome>(value)
            .unwrap()
            .valid()
    );
}
