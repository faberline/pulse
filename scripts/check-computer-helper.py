"""Offline native process check. No target app is launched or captured."""
import json
import pathlib
import subprocess
import sys
import tempfile
import uuid

helper = pathlib.Path(sys.argv[1]).resolve(strict=True)


def call(arguments, payload=None):
    result = subprocess.run(
        [str(helper), *arguments], input=payload, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, timeout=5, check=True,
    )
    assert len(result.stdout) <= 65_536
    value = json.loads(result.stdout)
    assert value["version"] == "pulse-computer-observe-2"
    assert type(value["permissions"]["accessibility"]) is bool
    assert type(value["permissions"]["screen_capture"]) is bool
    assert value.get("observation") is None
    return value


status = call(["--status"])
assert status.get("error") is None
with tempfile.TemporaryDirectory(prefix="pulse-computer-check-") as directory:
    arguments = ["--bundle-id", "com.faberline.PulseComputerFixture", "--pid", "2147483647", "--window-id", "1", "--cache", directory]
    binding = {"operation_id": "probe-no-target", "task_id": "owned-fixture", "revision": "1", "device": str(uuid.uuid4()), "epoch": "1"}
    denied = call(arguments, json.dumps({"kind": "observe", "binding": binding}).encode())
    assert denied["error"] in ["permissions_required", "target_unavailable"]
    for payload in [b'{"kind":"click"}', b'{"kind":"type","text":"send"}', b'{"kind":"script"}', b" " * 16_385]:
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
