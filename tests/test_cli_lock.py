"""CLI lock acquire/release/status round-trip."""

from __future__ import annotations

import json

from click.testing import CliRunner

from agentbus.cli import main


def test_cli_lock_acquire_release_status(tmp_path):
    ws = str(tmp_path)
    resource = str(tmp_path / "log.md")
    runner = CliRunner()

    ensure = runner.invoke(main, ["token", "ensure", "--workspace", ws, "--quiet"])
    assert ensure.exit_code == 0, ensure.output

    acquire = runner.invoke(
        main,
        [
            "lock",
            "acquire",
            "--workspace",
            ws,
            "--resource",
            resource,
            "--owner-id",
            "hermes",
        ],
    )
    assert acquire.exit_code == 0, acquire.output
    data = json.loads(acquire.output)
    assert data["acquired"] is True
    lease_id = data["lease_id"]

    status = runner.invoke(
        main,
        ["lock", "status", "--workspace", ws, "--resource", resource],
    )
    assert status.exit_code == 0
    st = json.loads(status.output)
    assert st["locked"] is True
    assert st["current_owner"] == "hermes"

    conflict = runner.invoke(
        main,
        [
            "lock",
            "acquire",
            "--workspace",
            ws,
            "--resource",
            resource,
            "--owner-id",
            "grok",
        ],
    )
    assert conflict.exit_code == 0
    assert json.loads(conflict.output)["acquired"] is False

    release = runner.invoke(
        main,
        [
            "lock",
            "release",
            "--workspace",
            ws,
            "--resource",
            resource,
            "--lease-id",
            lease_id,
            "--owner-id",
            "hermes",
        ],
    )
    assert release.exit_code == 0
    assert json.loads(release.output)["released"] is True

    final = runner.invoke(
        main,
        ["lock", "status", "--workspace", ws, "--resource", resource],
    )
    assert json.loads(final.output)["locked"] is False