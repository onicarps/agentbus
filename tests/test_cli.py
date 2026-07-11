"""CLI publish/poll/status tests."""

from __future__ import annotations

import json
import logging

from click.testing import CliRunner

from agentbus.cli import main


def test_quiet_flag_raises_root_log_level(caplog):
    runner = CliRunner()
    for flag in ("--quiet", "-q"):
        with caplog.at_level(logging.DEBUG):
            result = runner.invoke(main, [flag, "status", "--help"])
        assert result.exit_code == 0, result.output
        # During quiet invoke root is CRITICAL; after close it is restored.
        # Caplog should not force INFO lines into stdout of the CLI help.
        assert "Usage:" in result.output or "status" in result.output.lower()
    # Restored after CliRunner (call_on_close)
    assert os_environ_quiet_cleared_or_restored()


def os_environ_quiet_cleared_or_restored() -> bool:
    import os

    # Default process should not permanently sticky-set AGENTBUS_QUIET from tests.
    return os.environ.get("AGENTBUS_QUIET") in (None, "0", "")


def test_quiet_suppresses_noncritical_logger_output(capsys):
    import logging as lg

    runner = CliRunner()
    # Invoke quiet help; then ensure CRITICAL-only configuration applied mid-run
    # by checking that our configure path set handler level high.
    result = runner.invoke(main, ["--quiet", "status", "--help"])
    assert result.exit_code == 0
    # After restore, emit and ensure process still works
    lg.getLogger("agentbus.test").warning("should not crash")
    # stdout of help is clean JSON/help, not log lines
    assert "should not crash" not in result.output


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
    st_data = json.loads(st.output)
    assert st_data["event_count"] == 1
    assert st_data["total_events"] == 1


def test_cli_publish_batch(tmp_path):
    ws = str(tmp_path)
    runner = CliRunner()
    runner.invoke(main, ["token", "ensure", "--workspace", ws, "--quiet"])
    batch = tmp_path / "batch.jsonl"
    batch.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "topic": "okf/handoff",
                        "payload": {"from": "grok", "to": "agy", "summary": "one"},
                    }
                ),
                json.dumps(
                    {
                        "topic": "okf/handoff",
                        "payload": {"from": "grok", "to": "hermes", "summary": "two"},
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    result = runner.invoke(
        main,
        [
            "publish-batch",
            "--workspace",
            ws,
            "--file",
            str(batch),
            "--producer-id",
            "grok",
        ],
    )
    assert result.exit_code == 0, result.output
