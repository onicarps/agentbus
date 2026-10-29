"""Declarative MCP client adapters (manifests, probes, config patching).

The adapter layer answers one question without side effects: *which* MCP client
installations exist on this host and workspace, and what would it take to make
them talk to AgentBus.  Manifests are packaged YAML data files so a new client
never requires Python changes.
"""

from __future__ import annotations

from agentbus.adapters.manifest import (
    Installation,
    Manifest,
    ManifestError,
    install_target,
    load_manifests,
    manifest_root,
)
from agentbus.adapters.probe import ClientProbe, probe_installations

__all__ = [
    "ClientProbe",
    "Installation",
    "Manifest",
    "ManifestError",
    "install_target",
    "load_manifests",
    "manifest_root",
    "probe_installations",
]
