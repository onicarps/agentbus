"""Probe wrapper tests (subprocess mock; no live swarm required)."""

from __future__ import annotations

import json
import stat
import textwrap
from pathlib import Path

from agentbus_ops.probe import HealthSnapshot, probe_health


def test_health_snapshot_from_dict():
    snap = HealthSnapshot.from_dict(
        {
            "ok": True,
            "level": "healthy",
            "exit_code": 0,
            "workspace": "/ws",
            "checked_at": "2026-07-20T00:00:00Z",
            "latest_event_id": 10,
            "notes": ["skip_disabled:x"],
            "disabled_services": ["hermes-wake-ingress"],
        }
    )
    assert snap.level == "healthy"
    assert snap.ok is True
    assert snap.latest_event_id == 10
    d = snap.to_dict()
    assert d["sre_status"] == "healthy"


def test_probe_health_json_wrap(tmp_path: Path):
    script = tmp_path / "probe.sh"
    payload = {
        "ok": False,
        "level": "degraded",
        "sre_status": "degraded",
        "exit_code": 1,
        "workspace": str(tmp_path),
        "checked_at": "2026-07-20T00:00:00Z",
        "latest_event_id": 7,
        "notes": ["missing:watch"],
        "disabled_services": [],
    }
    script.write_text(
        textwrap.dedent(
            f"""\
            #!/usr/bin/env bash
            echo '{json.dumps(payload)}'
            exit 1
            """
        ),
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    snap = probe_health(tmp_path, health_script=script, timeout=10)
    assert snap.level == "degraded"
    assert snap.exit_code == 1
    assert snap.notes == ["missing:watch"]
    assert snap.latest_event_id == 7


def test_probe_missing_script(tmp_path: Path):
    snap = probe_health(tmp_path, health_script=tmp_path / "nope.sh")
    assert snap.level == "critical"
    assert any(n.startswith("health_script_missing:") for n in snap.notes)


def test_probe_text_fallback(tmp_path: Path):
    script = tmp_path / "probe.sh"
    script.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            echo "SRE_STATUS: critical" >&2
            exit 2
            """
        ),
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    snap = probe_health(tmp_path, health_script=script, timeout=10)
    assert snap.level == "critical"
    assert "json_mode_fallback" in snap.notes


def test_env_health_script_override(tmp_path: Path, monkeypatch):
    script = tmp_path / "env_probe.sh"
    payload = {
        "ok": True,
        "level": "healthy",
        "exit_code": 0,
        "workspace": str(tmp_path),
        "checked_at": "t",
        "notes": [],
        "disabled_services": [],
    }
    script.write_text(
        f"#!/usr/bin/env bash\necho '{json.dumps(payload)}'\nexit 0\n",
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("SRE_HEALTH_SCRIPT", str(script))
    # No health_script arg — should pick env via default_health_script
    from agentbus_ops.probe import default_health_script

    assert default_health_script(tmp_path) == script
    # Clear for other tests
    monkeypatch.delenv("SRE_HEALTH_SCRIPT", raising=False)
