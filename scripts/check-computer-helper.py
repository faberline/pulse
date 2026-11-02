"""Offline native process check. No target app is launched or captured."""
import json
import pathlib
import subprocess
import sys
import tempfile
import time
import uuid

helper = pathlib.Path(sys.argv[1]).resolve(strict=True)


def call(arguments, payload=None):
    result = subprocess.run(
        [str(helper), *arguments], input=payload, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, timeout=5, check=True,
    )
    assert len(result.stdout) <= 65_536
    value = json.loads(result.stdout)
    assert value["version"] == "pulse-computer-observe-6"
    assert type(value["permissions"]["accessibility"]) is bool
    assert type(value["permissions"]["screen_capture"]) is bool
    assert value.get("observation") is None
    assert value.get("selection") is None
    return value


status = call(["--status"])
assert status.get("error") is None
# Malformed requests never enter a system prompt. Never add a valid permission
# request here: this script is deliberately safe for unattended CI.
for arguments in [["--request-permission"], ["--request-permission", "all"], ["--request-permission", "accessibility", "--grant"]]:
    assert call(arguments)["error"] == "invalid_request"
with tempfile.TemporaryDirectory(prefix="pulse-computer-check-") as directory:
    arguments = ["--bundle-id", "com.faberline.PulseComputerFixture", "--pid", "2147483647", "--window-id", "1", "--cache", directory]
    binding = {"operation_id": "probe-no-target", "task_id": "owned-fixture", "revision": "1", "device": str(uuid.uuid4()), "epoch": "1"}
    denied = call(arguments, json.dumps({"kind": "observe", "binding": binding}).encode())
    assert denied["error"] in ["permissions_required", "target_unavailable"]
    denied = call(arguments, json.dumps({"kind": "probe", "expected_process_launched_at": "1"}).encode())
    assert denied["error"] in ["permissions_required", "target_unavailable"]
    now = int(time.time() * 1000)
    source = dict(binding, operation_id="source-no-target")
    intent = {
        "observation": {
            "binding": source,
            "target": {"bundle_id": arguments[1], "pid": 2147483647, "window_id": 1},
            "process_launched_at": 1, "title": "Missing owned fixture",
            "frame": {"x": 0, "y": 0, "width": 100, "height": 100},
            "elements": [{"id": "window.0", "role": "AXButton", "label": "Count",
                          "value": None, "can_press": True, "can_set_text": False}],
            "truncated": False, "nonce": str(uuid.uuid4()), "observed_at": now,
            "expires_at": now + 120000,
            "image": {"digest": "a" * 64, "length": 100, "width": 1, "height": 1},
        },
        "element_id": "window.0", "effect": {"kind": "press"},
    }
    denied = call(arguments, json.dumps({"kind": "act", "binding": binding, "intent": intent}).encode())
    assert denied["error"] in ["permissions_required", "target_unavailable"]
    assert denied["action"] == {"binding": binding,
                                "source_nonce": intent["observation"]["nonce"], "state": "not_executed"}
    for payload in [b'{"kind":"click"}', b'{"kind":"type","text":"send"}', b'{"kind":"script"}', b'{"kind":"act"}', b" " * 16_385]:
        assert call(arguments, payload)["error"] == "invalid_request"
    for launched in [0, True, "0", "01", "+1", "9223372036854775808"]:
        payload = {"kind": "observe", "binding": binding, "expected_process_launched_at": launched}
        assert call(arguments, json.dumps(payload).encode())["error"] == "invalid_request"
    assert list(pathlib.Path(directory).iterdir()) == []
print("Computer helper: native preflight and rejection checks passed; no capture or permission request.")

if len(sys.argv) > 2:
    executor = pathlib.Path(sys.argv[2]).resolve(strict=True)
    result = subprocess.run([str(executor), "--computer-status"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5, check=True)
    assert len(result.stdout) <= 65_536
    value = json.loads(result.stdout)
    assert value["installed"] is True and "error" not in value
    assert type(value["permissions"]["accessibility"]) is bool
    assert type(value["permissions"]["screen_capture"]) is bool
    assert value.get("observation") is None
    print("Rust native bridge: permission probe passed without pairing, capture or model calls.")
