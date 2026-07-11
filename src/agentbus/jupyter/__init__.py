"""Jupyter / IPython integration for non-blocking AgentBus polling."""

from agentbus.jupyter.client import AsyncAgentBus

__all__ = ["AsyncAgentBus", "load_ipython_extension"]


def load_ipython_extension(ipython) -> None:
    """IPython entrypoint: ``%load_ext agentbus.jupyter``."""
    from agentbus.jupyter.magic import load_ipython_extension as _load

    _load(ipython)
