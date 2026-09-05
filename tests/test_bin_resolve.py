"""Platform binary resolver (wheel layout)."""

from __future__ import annotations

import stat


from agentbus import bin_resolve


def test_platform_dir_known():
    # Just ensure it returns a non-empty string on this CI host
    d = bin_resolve.platform_dir()
    assert d in {
        "linux-x64",
        "linux-arm64",
        "darwin-x64",
        "darwin-arm64",
        "win32-x64",
    }


def test_resolve_bundled_binary(tmp_path, monkeypatch):
    # Point package root at tmp by monkeypatching package_bin_root
    plat = bin_resolve.platform_dir()
    root = tmp_path / "bin"
    (root / plat).mkdir(parents=True)
    exe = root / plat / "agentbus-go-worker"
    exe.write_text("#!/bin/sh\necho ok\n")
    exe.chmod(exe.stat().st_mode | stat.S_IEXEC)

    monkeypatch.setattr(bin_resolve, "package_bin_root", lambda: root)
    found = bin_resolve.resolve_bundled_binary("agentbus-go-worker")
    assert found == exe


def test_resolve_go_binary_env(tmp_path, monkeypatch):
    fake = tmp_path / "custom-worker"
    fake.write_text("x")
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("AGENTBUS_GO_WORKER", str(fake))
    p = bin_resolve.resolve_go_binary("agentbus-go-worker", env_var="AGENTBUS_GO_WORKER")
    assert p == fake


def test_resolve_go_binary_env_fallback(tmp_path, monkeypatch):
    # If AGENTBUS_GO_WORKER points to a missing file, fall back to dev candidates / PATH
    monkeypatch.setenv("AGENTBUS_GO_WORKER", "/nonexistent/path/to/agentbus-go-worker")
    monkeypatch.setattr(bin_resolve, "resolve_bundled_binary", lambda name: None)
    dev_fake = tmp_path / "dev-worker"
    dev_fake.write_text("x")
    dev_fake.chmod(dev_fake.stat().st_mode | stat.S_IEXEC)
    p = bin_resolve.resolve_go_binary(
        "agentbus-go-worker",
        env_var="AGENTBUS_GO_WORKER",
        dev_candidates=[dev_fake],
    )
    assert p == dev_fake
