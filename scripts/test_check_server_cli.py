import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("check_server_cli", Path(__file__).with_name("check-server-cli.py"))
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)


class RunCheckpointTests(unittest.TestCase):
    def test_claimed_null_checkpoint_is_pending_and_does_not_end_the_scene(self):
        ready = {"checkpoint": {"phase": "reply_saved", "reply": "fixture"}}
        state = {"runs": [{"checkpoint": None}, {}, {"checkpoint": {"phase": "waiting"}}, ready]}
        self.assertEqual(check.saved_replies(state), [ready])


if __name__ == "__main__":
    unittest.main()
