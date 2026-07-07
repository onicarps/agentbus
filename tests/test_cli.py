"""CLI publish/poll/status tests."""

from __future__ import annotations

import json

from click.testing import CliRunner

from agentbus.cli import main


def test_cli_publish_poll_status(tmp_path):
    ws = str(tmp_path)
    runner = CliRunner()
    ensure = runner.invoke(main, ["token", "ensure", "--workspace", ws, "--quiet"])
    assert ensure.exit_code == 0, ensure.output
    payload = json.dumps(
        {"from": "agy", "to": "grok", "summary": "CLI fallback test"}
    )
    pub = runner.invoke(
        main,
        [
            "publish",
            "--workspace",
            ws,
            "--topic",
            "okf/handoff",
            "--payload",
            payload,
            "--producer-id",
            "agy",
        ],
    )
    assert pub.exit_code == 0, pub.output
    pub_data = json.loads(pub.output)
    assert pub_data["event_id"] == 1

    poll = runner.invoke(
        main,
        ["poll", "--workspace", ws, "--topic", "okf/handoff", "--since-id", "0"],
    )
    assert poll.exit_code == 0
    poll_data = json.loads(poll.output)
    assert len(poll_data["events"]) == 1

    st = runner.invoke(main, ["status", "--workspace", ws, "--producer-id", "agy"])
    assert st.exit_code == 0
    assert json.loads(st.output)["event_count"] == 1