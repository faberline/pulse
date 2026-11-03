#!/usr/bin/env python3
"""Owned-process CLI/server smoke. --offline has no model credentials or API calls."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import subprocess
import tempfile
import time

REPO = Path(__file__).resolve().parent.parent


def snapshot(database):
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        return {
            "tasks": [json.loads(r[0]) for r in db.execute("SELECT data FROM tasks ORDER BY id")],
            "runs": [json.loads(r[0]) for r in db.execute("SELECT data FROM runs ORDER BY id")],
            "budget": [dict(r) for r in db.execute("SELECT call_id,task_id,reserved,charged,state FROM budget ORDER BY call_id")],
            "reads": [dict(r) for r in db.execute("SELECT id,task_id,request,state,result FROM link_reads ORDER BY id")],
        }


def saved_replies(state):
    # A claimed run can legitimately have a null checkpoint before its first reply.
    return [r for r in state["runs"] if (r.get("checkpoint") or {}).get("phase") == "reply_saved"]


class Scene:
    def __init__(self, root, provider, live=True):
        self.root = root
        self.root.mkdir(mode=0o700)
        self.database = root / "server.db"
        self.children = []
        self.logs = []
        self.repo = root / "repo"
        self.repo.mkdir(mode=0o700)
        for name in ["README.md", "Cargo.toml"]:
            file = self.repo / name
            file.write_bytes((REPO / name).read_bytes())
            file.chmod(0o600)
        self.env = {k: v for k, v in os.environ.items() if not k.startswith(("PULSE_", "OPENAI_", "ANTHROPIC_"))}
        self.env.update(PULSE_PROVIDER=provider, PULSE_ENV_FILE=str(REPO / ".env" if live else root / "no-provider.env"),
                        PULSE_LOCAL_CONTROL_DIR=str(root / "control"))

    def start(self, name, arguments, input_pipe=False):
        log = open(self.root / f"{name}.log", "w")
        self.logs.append(log)
        process = subprocess.Popen([str(x) for x in arguments], cwd=REPO, env=self.env,
                                   stdin=subprocess.PIPE if input_pipe else subprocess.DEVNULL,
                                   stdout=log, stderr=subprocess.STDOUT, text=True)
        self.children.append(process)
        return process

    def wait(self, predicate, seconds=120):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if any(p.poll() is not None for p in self.children):
                raise RuntimeError("An owned process exited before the check completed")
            value = predicate()
            if value:
                return value
            time.sleep(0.2)
        raise TimeoutError("Scene result unconfirmed; no automatic restart or model retry")

    def server(self, name, port):
        self.start(name, [REPO / "target/debug/pulse-server", "serve", self.database, port])

        def listener():
            match = re.search(r"Pulse listener: (http://127\.0\.0\.1:\d+)", (self.root / f"{name}.log").read_text())
            return match.group(1) if match else None
        return self.wait(listener)

    def pair(self, role):
        with open(self.root / f"{role}-pair.log", "w") as log:
            subprocess.run([str(REPO / "target/debug/pulse-server"), f"pair-{role}",
                            str(self.database), str(self.root / f"{role}.pair")],
                           cwd=REPO, env=self.env, stdin=subprocess.DEVNULL,
                           stdout=log, stderr=subprocess.STDOUT, check=True, timeout=120)

    def local(self, name, origin, paired):
        args = [REPO / "target/debug/pulse-executor", "--server", origin,
                "--dev-loopback", "--repo", self.repo, "--data-dir", self.root / "local"]
        if paired:
            args += ["--pair-code-file", self.root / "local.pair"]
        self.start(name, args)
        self.wait(lambda: "paired=true, connected=true" in (self.root / f"{name}.log").read_text())

    def cli(self, name, origin, paired=False, watch=False):
        args = [REPO / "target/debug/pulse-cli"]
        if watch:
            args.append("--watch")
        args += ["--server", origin, "--dev-loopback", "--data-dir", self.root / "chat"]
        if paired:
            args += ["--pair-code-file", self.root / "chat.pair"]
        process = self.start(name, args, input_pipe=True)
        self.wait(lambda: "server 已連線。" in (self.root / f"{name}.log").read_text())
        return process

    def stop(self):
        # Reverse ownership order: CLI, local process, then server.
        for process in reversed(self.children):
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=40)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
        self.children.clear()
        for log in self.logs:
            log.close()
        self.logs.clear()


def check(root, provider):
    scene = Scene(root, provider)
    try:
        origin = scene.server("server", "0")
        scene.pair("chat")
        scene.pair("local")
        scene.local("local", origin, True)
        chat = scene.cli("cli", origin, paired=True)
        chat.stdin.write("A：請讀 README.md，列出文件中的三個元件。\nB：請讀 Cargo.toml，列出 workspace 成員。\n")
        chat.stdin.flush()

        def replies():
            state = snapshot(scene.database)
            if len(state["budget"]) > 8:
                raise RuntimeError("Smoke call count exceeded the expected bounded flow")
            saved = saved_replies(state)
            return (state, saved) if len(saved) == 2 else None
        before, runs = scene.wait(replies, seconds=180)
        assert len(before["tasks"]) == 2
        assert all(r["checkpoint"].get("reply", "").strip() for r in runs)
        assert all(row["state"] == "settled" for row in before["budget"])
        for task in before["tasks"]:
            expected = "README.md" if "README.md" in task["goal"] else "Cargo.toml"
            reads = [r for r in before["reads"] if r["task_id"] == task["id"] and r["state"] == "completed"]
            exact = [r for r in reads if json.loads(r["request"]).get("path") == expected]
            assert exact, "The model reply has no requested local file read"
            assert any(json.loads(r["result"]).get("digest") == hashlib.sha256((scene.repo / expected).read_bytes()).hexdigest() for r in exact)
        scene.wait(lambda: all(r["checkpoint"]["reply"] in (scene.root / "cli.log").read_text() for r in runs))
        chat.stdin.write(":quit\n")
        chat.stdin.flush()
        assert chat.wait(timeout=40) == 0
        assert "server 已收件；這不表示任務已完成。" in (scene.root / "cli.log").read_text()
        scene.stop()

        # Reopen the same DB, origin, role journals and NativeVault identities.
        scene.server("server-reopen", origin.rsplit(":", 1)[1])
        scene.local("local-reopen", origin, False)
        scene.cli("cli-watch", origin, watch=True)
        scene.wait(lambda: all(r["checkpoint"]["reply"] in (scene.root / "cli-watch.log").read_text() for r in runs))
        quiet_until = time.monotonic() + 5
        scene.wait(lambda: time.monotonic() >= quiet_until)
        after = snapshot(scene.database)
        assert before["budget"] == after["budget"], "Restart added a model call or changed its charge"
        assert before["reads"] == after["reads"], "Restart changed persisted read records"
        assert {t["id"] for t in before["tasks"]} == {t["id"] for t in after["tasks"]}
        return {"provider": provider, "tasks": len(before["tasks"]),
                "model_calls": len(before["budget"]), "local_reads": len(before["reads"]),
                "charged_micro_usd": sum(r["charged"] for r in before["budget"]),
                "unknown_calls": sum(r["state"] != "settled" for r in before["budget"]),
                "cli_pipe_and_reopen": "passed", "root": str(root)}
    finally:
        scene.stop()


def check_offline(root):
    scene = Scene(root, "openai", live=False)
    try:
        origin = scene.server("server", "0")
        scene.pair("chat")
        scene.pair("local")
        scene.local("local", origin, True)
        chat = scene.cli("cli", origin, paired=True)
        chat.stdin.write("A：請讀 README.md，列出文件中的三個元件。\nB：請讀 Cargo.toml，列出 workspace 成員。\n")
        chat.stdin.close()
        assert chat.wait(timeout=40) == 0
        scene.children.remove(chat)

        def blocked():
            state = snapshot(scene.database)
            return state if len(state["tasks"]) == 2 and all(t["reason"] == "provider_not_configured" for t in state["tasks"]) else None
        before = scene.wait(blocked)
        assert not before["budget"] and not before["reads"]
        assert "server 已收件；這不表示任務已完成。" in (scene.root / "cli.log").read_text()
        scene.stop()
        scene.server("server-reopen", origin.rsplit(":", 1)[1])
        scene.local("local-reopen", origin, False)
        scene.cli("cli-watch", origin, watch=True)
        scene.wait(lambda: "README.md" in (scene.root / "cli-watch.log").read_text() and "Cargo.toml" in (scene.root / "cli-watch.log").read_text())
        after = snapshot(scene.database)
        assert {t["id"] for t in before["tasks"]} == {t["id"] for t in after["tasks"]}
        assert not after["budget"] and not after["reads"]
        return {"provider": "unconfigured", "tasks": 2, "model_calls": 0, "local_reads": 0,
                "cli_pipe_and_reopen": "passed", "model_reply_validation": "unrun", "root": str(root)}
    finally:
        scene.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--live", action="store_true", help="Requires Human approval for external API payload and costs")
    mode.add_argument("--offline", action="store_true", help="No model keys or model API calls; localhost only")
    parser.add_argument("--provider", choices=["openai", "anthropic", "both"], default="both")
    args = parser.parse_args()
    root = Path(tempfile.mkdtemp(prefix="pulse-server-cli-live-"))
    report = {"root": str(root), "providers": [], "status": "incomplete",
              "base_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()}
    report["source_sha256"] = {
        name: hashlib.sha256((REPO / name).read_bytes()).hexdigest()
        for name in ["crates/pulse-link/src/cli.rs", "crates/pulse-link/src/bin/pulse-cli.rs",
                     "crates/pulse-link/src/client.rs", "crates/pulse-link/src/protocol.rs",
                     "crates/pulse-link/src/remote.rs", "crates/pulse-link/src/executor.rs", "Cargo.lock"]
    }
    for name in ["pulse-server", "pulse-executor", "pulse-cli"]:
        report[name + "_sha256"] = hashlib.sha256((REPO / "target/debug" / name).read_bytes()).hexdigest()
    try:
        print(json.dumps({"root": str(root), "status": "starting"}), flush=True)
        for provider in (["offline"] if args.offline else (["openai", "anthropic"] if args.provider == "both" else [args.provider])):
            result = check_offline(root / provider) if args.offline else check(root / provider, provider)
            report["providers"].append(result)
            print(json.dumps(result, ensure_ascii=False), flush=True)
        report["status"] = "passed"
    finally:
        (root / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(json.dumps({"report": str(root / "report.json"), "status": report["status"]}), flush=True)


if __name__ == "__main__":
    main()
