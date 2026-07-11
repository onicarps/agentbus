import pytest

from agentbus.jupyter.magic import load_ipython_extension


def test_magic_registration():
    pytest.importorskip("IPython")
    from IPython.testing.globalipapp import get_ipython

    ip = get_ipython()
    assert ip is not None
    load_ipython_extension(ip)
    assert "agentbus" in ip.magics_manager.magics["line"]
