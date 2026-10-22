"""Isolated AgentBus reference-monitor broker."""

__all__ = ["BrokerApplication", "BrokerServer", "run_broker"]


def __getattr__(name: str):
    if name in __all__:
        from agentbus.broker import server

        return getattr(server, name)
    raise AttributeError(name)

