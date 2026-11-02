use pulse_macos::{
    Request,
    contract::{launch_time, now},
};
use serde_json::{Value, json};

fn binding() -> Value {
    json!({"operation_id":"read-1","task_id":"task","revision":"1","device":uuid::Uuid::new_v4().to_string(),"epoch":"2"})
}
fn decode(value: &Value) -> bool {
    Request::decode(&serde_json::to_vec(value).unwrap(), now()).is_ok()
}
fn action() -> Value {
    let source = binding();
    let mut destination = source.clone();
    destination["operation_id"] = "approved:request".into();
    let time = now();
    json!({"kind":"act","binding":destination,"intent":{"observation":{"binding":source,"target":{"bundle_id":"com.faberline.fixture","pid":42,"window_id":7},"process_launched_at":time-1000,"title":"Owned fixture","frame":{"x":0,"y":0,"width":100,"height":100},"elements":[{"id":"window.0","role":"AXButton","label":"Count","value":null,"can_press":true,"can_set_text":false},{"id":"window.1","role":"AXTextField","label":"Text","value":"","can_press":false,"can_set_text":true}],"truncated":false,"nonce":uuid::Uuid::new_v4().to_string(),"observed_at":time,"expires_at":time+120000,"image":{"digest":"a".repeat(64),"length":100,"width":1,"height":1}},"element_id":"window.0","effect":{"kind":"press"}}})
}
#[test]
fn read_status_and_probe_reject_effects_extra_fields_and_bad_bindings() {
    assert!(decode(&json!({"kind":"status"})));
    assert!(decode(&json!({"kind":"observe","binding":binding()})));
    for value in [
        json!({"kind":"click"}),
        json!({"kind":"type","text":"send"}),
        json!({"kind":"script"}),
        json!({"kind":"status","target":"other"}),
        json!({"kind":"observe"}),
    ] {
        assert!(!decode(&value), "{value}");
    }
    for field in ["revision", "epoch"] {
        for value in [
            json!(0),
            json!(true),
            json!("0"),
            json!("01"),
            json!("+1"),
            json!("18446744073709551616"),
        ] {
            let mut b = binding();
            b[field] = value;
            assert!(!decode(&json!({"kind":"observe","binding":b})));
        }
    }
    for field in ["operation_id", "task_id", "device"] {
        let mut b = binding();
        b[field] = "".into();
        assert!(!decode(&json!({"kind":"observe","binding":b})));
    }
    assert!(Request::decode(b"{\"kind\":\"status\",\"kind\":\"observe\"}", now()).is_err());
    assert!(Request::decode(&vec![b' '; 65537], now()).is_err());
}
#[test]
fn launch_time_is_a_canonical_positive_i64_string() {
    for value in [
        json!(0),
        json!(true),
        json!("0"),
        json!("01"),
        json!("+1"),
        json!("9223372036854775808"),
    ] {
        assert!(!decode(
            &json!({"kind":"probe","expected_process_launched_at":value})
        ));
        assert!(!decode(
            &json!({"kind":"observe","binding":binding(),"expected_process_launched_at":value})
        ));
    }
    assert_eq!(launch_time("1"), Ok(1));
    assert!(decode(
        &json!({"kind":"probe","expected_process_launched_at":"1"})
    ));
}
#[test]
fn fixed_actions_accept_only_exact_path_free_sources_and_plain_text() {
    let base = action();
    assert!(decode(&base));
    for text in ["中文 test", ""] {
        let mut v = base.clone();
        v["intent"]["element_id"] = "window.1".into();
        v["intent"]["effect"] = json!({"kind":"set_text","text":text});
        assert!(decode(&v));
    }
    for text in [
        "send\n",
        "\r",
        "\t",
        "\u{e007}",
        "\u{2028}",
        &"文".repeat(342),
    ] {
        let mut v = base.clone();
        v["intent"]["element_id"] = "window.1".into();
        v["intent"]["effect"] = json!({"kind":"set_text","text":text});
        assert!(!decode(&v));
    }
    for id in [
        "window.00",
        "window.-1",
        "window.128",
        "window/0",
        "window..0",
        "other.0",
        "window.0.0.0.0.0.0.0.0.0",
    ] {
        assert!(!pulse_core::computer_element_path(id));
    }
    assert!(pulse_core::computer_element_path("window.0.127"));
    for extra in [
        json!({"kind":"press","text":null}),
        json!({"kind":"press","grant":true}),
        json!({"kind":"set_text","text":"ok","path":"/private/forged"}),
    ] {
        assert!(serde_json::from_value::<pulse_core::ComputerEffect>(extra).is_err());
    }
}
#[test]
fn actions_reject_changed_bindings_stale_images_secure_controls_and_forged_authority() {
    let base = action();
    for field in ["task_id", "revision", "device", "epoch", "operation_id"] {
        let mut v = base.clone();
        v["binding"][field] = if field == "operation_id" {
            "read-1".into()
        } else if field == "revision" || field == "epoch" {
            "9".into()
        } else {
            "other".into()
        };
        assert!(!decode(&v), "{field}");
    }
    for (path, value) in [
        ("/intent/observation/expires_at", json!(1)),
        ("/intent/observation/observed_at", json!(i64::MAX - 120000)),
        ("/intent/observation/target/window_id", json!(0)),
        ("/intent/observation/image/width", json!(8193)),
        ("/intent/observation/nonce", json!("forged")),
        ("/intent/observation/elements/0/id", json!("window.00")),
        (
            "/intent/observation/elements/0/role",
            json!("AXSecureTextField"),
        ),
        ("/intent/element_id", json!("window.1")),
        (
            "/intent/effect",
            json!({"kind":"set_text","text":"replace a button"}),
        ),
    ] {
        let mut v = base.clone();
        *v.pointer_mut(path).unwrap() = value;
        assert!(!decode(&v), "{path}");
    }
    for (parent, field, value) in [
        ("", "helper", json!("/private/forged")),
        ("/intent", "grant", json!(true)),
        (
            "/intent/observation/image",
            "path",
            json!("/private/forged"),
        ),
        ("/intent/observation/binding", "grant", json!(true)),
        (
            "/intent/observation/elements/0",
            "path",
            json!("/private/forged"),
        ),
    ] {
        let mut v = base.clone();
        v.pointer_mut(parent)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert(field.into(), value);
        assert!(!decode(&v), "{parent}/{field}");
    }
}
#[test]
fn native_targets_and_frames_have_no_wildcards_or_unbounded_coordinates() {
    let mut target = pulse_core::ComputerTarget {
        bundle_id: "com.faberline.fixture".into(),
        pid: 1,
        window_id: 1,
    };
    assert!(target.valid());
    for bundle in [
        "*",
        "com.*",
        "com..fixture",
        "com.fixture:1",
        "com.fixture/path",
        "com.faberline.pulse",
    ] {
        target.bundle_id = bundle.into();
        assert!(!target.valid());
    }
    for n in [f64::NAN, f64::INFINITY, 0., -1., 8193.] {
        assert!(
            !pulse_core::ComputerRect {
                x: 0.,
                y: 0.,
                width: n,
                height: 1.
            }
            .valid()
        );
    }
}
