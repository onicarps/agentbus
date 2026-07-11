import pytest

pytest.importorskip("IPython")


def test_magic_registration():
    from IPython.testing.globalipapp import get_ipython

    from agentbus.jupyter.magic import load_ipython_extension

    ip = get_ipython()
    assert ip is not None
    load_ipython_extension(ip)
    assert "agentbus" in ip.magics_manager.magics["line"]


def test_agentbus_start_stop_status(capsys):
    from IPython.testing.globalipapp import get_ipython

    from agentbus.jupyter.magic import load_ipython_extension

    ip = get_ipython()
    assert ip is not None
    load_ipython_extension(ip)

    ip.run_line_magic("agentbus", "status")
    out = capsys.readouterr().out
    assert "running=" in out

    ip.run_line_magic("agentbus", "start 0.05")
    out = capsys.readouterr().out
    # Either started (running loop) or guided message (no loop in this harness)
    assert "started" in out or "No running asyncio loop" in out

    ip.run_line_magic("agentbus", "stop")
    out = capsys.readouterr().out
    assert "stopped" in out.lower()

    ip.run_line_magic("agentbus", "notacommand")
    out = capsys.readouterr().out
    assert "Unknown command" in out

    ip.run_line_magic("agentbus", "start not-a-float")
    out = capsys.readouterr().out
    assert "Invalid interval" in out
