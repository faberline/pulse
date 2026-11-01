use super::*;

#[test]
fn native_tool_schemas_bind_current_source_without_grants_and_require_a_fresh_inspect_after_effect()
{
    let now = crate::now();
    let observation:pulse_core::ComputerObservation=serde_json::from_value(json!({
        "binding":{"operation_id":"inspect","task_id":"task","revision":"1","device":uuid::Uuid::new_v4().to_string(),"epoch":"1"},
        "target":{"bundle_id":"com.faberline.PulseComputerFixture","pid":42,"window_id":7},"process_launched_at":1,
        "title":"Fixture","frame":{"x":0.,"y":0.,"width":400.,"height":300.},
        "elements":[{"id":"window.0","role":"AXButton","label":"Count","value":null,"can_press":true,"can_set_text":false},
            {"id":"window.1","role":"AXTextField","label":"Message","value":"before","can_press":false,"can_set_text":true}],
        "truncated":false,"nonce":uuid::Uuid::new_v4().to_string(),"observed_at":now,"expires_at":now+120_000,
        "image":{"digest":"a".repeat(64),"length":100,"width":1,"height":1}
    })).unwrap();
    let selection = observation.selection();
    let scope = selection.scope().unwrap();
    let mut outcome = ReadOutcome::text("Observed fixture".into(), false);
    if let ReadOutcome::Completed { computer, .. } = &mut outcome {
        *computer = Some(Box::new(observation.clone()));
    }
    let mut history = vec![Exchange {
        call: ToolCall {
            call_id: "read".into(),
            operation: ReadOp::ComputerInspect {
                selection: selection.clone(),
            },
        },
        outcome,
    }];
    for openai in [true, false] {
        let tools = scoped_tools(openai, &scope, &history);
        assert_eq!(tools.len(), 3);
        let key = if openai { "parameters" } else { "input_schema" };
        assert_eq!(tools[1]["name"], "computer_press");
        assert_eq!(tools[2]["name"], "computer_type");
        assert_eq!(
            tools[1][key]["properties"]["nonce"]["enum"],
            json!([observation.nonce])
        );
        assert_eq!(
            tools[1][key]["properties"]["element_id"]["enum"],
            json!(["window.0"])
        );
        assert_eq!(
            tools[2][key]["properties"]["element_id"]["enum"],
            json!(["window.1"])
        );
        assert_eq!(
            tools[1][key]["properties"]["selection"]["properties"]["target"]["properties"]["pid"]["enum"],
            json!([42])
        );
        assert!(tools[1][key]["properties"].get("grant").is_none());
    }
    let args = json!({"selection":selection,"nonce":observation.nonce,"element_id":"window.0"});
    let press = proposal(&json!("press"), &json!("computer_press"), args.clone()).unwrap();
    assert!(matches!(
        &press.operation,
        ReadOp::ComputerAction {
            grant: None,
            effect: pulse_core::ComputerEffect::Press,
            ..
        }
    ));
    for (field, value) in [
        ("grant", json!(null)),
        ("effect", json!({"kind":"press"})),
        ("script", json!("forged")),
        ("path", json!("/private/forged")),
    ] {
        let mut bad = args.clone();
        bad[field] = value;
        assert!(proposal(&json!("press"), &json!("computer_press"), bad).is_err());
    }
    for text in ["submit\n", "\r", "\u{e007}", &"a".repeat(1025)] {
        let mut bad = args.clone();
        bad["element_id"] = json!("window.1");
        bad["text"] = json!(text);
        assert!(proposal(&json!("type"), &json!("computer_type"), bad).is_err());
    }
    let mut text = args;
    text["element_id"] = json!("window.1");
    text["text"] = json!("中文 test");
    assert!(proposal(&json!("type"), &json!("computer_type"), text).is_ok());
    history.push(Exchange {
        call: press,
        outcome: ReadOutcome::text("Effect completed; get a fresh inspect".into(), false),
    });
    for openai in [true, false] {
        assert_eq!(scoped_tools(openai, &scope, &history).len(), 1);
    }
}
