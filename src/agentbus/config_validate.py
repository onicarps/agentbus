"""Pre-flight config validation — ingress ↔ runner ↔ worker pairing (#682 class).

Codifies Mode A / webhook triad rules so ops can catch "ingress on, runner off"
before ``agentbus down`` / ``up``. Used by CLI ``agentbus validate-config``.
"""

from __future__ import annotations

import re
import shlex
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml

from agentbus.rbac import load_rbac_config, roles_path
from agentbus.runner.config import default_queue_path, load_runner_config
from agentbus.swarm import ServiceSpec, SwarmConfig, load_swarm_config, swarm_dir, swarm_yaml_path

Severity = Literal["error", "warning"]

_INGRESS_SUFFIX = "-wake-ingress"
_RUNNER_SUFFIX = "-runner"
_RUNTIME_FROM_CMD = re.compile(r"--runtime\s+(\S+)", re.IGNORECASE)
_CONFIG_FROM_CMD = re.compile(r"--config\s+(\S+)", re.IGNORECASE)


@dataclass
class Finding:
    """One validation finding."""

    severity: Severity
    code: str
    message: str
    path: str | None = None
    runtime: str | None = None
    service: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return {k: v for k, v in d.items() if v is not None}


@dataclass
class ValidationReport:
    """Result of workspace config validation."""

    workspace: Path
    ok: bool
    findings: list[Finding] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def errors(self) -> list[Finding]:
        return [f for f in self.findings if f.severity == "error"]

    @property
    def warnings(self) -> list[Finding]:
        return [f for f in self.findings if f.severity == "warning"]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "workspace": str(self.workspace.resolve()),
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "errors": [f.to_dict() for f in self.errors],
            "warnings": [f.to_dict() for f in self.warnings],
            "summary": self.summary,
        }


@dataclass
class _IngressService:
    name: str
    runtime: str
    enabled: bool
    command: str


@dataclass
class _RunnerService:
    name: str
    enabled: bool
    command: str
    config_path: Path | None
    runtime_hint: str | None  # from service name prefix when name ends with -runner


@dataclass
class _WorkerFile:
    path: Path
    producer_id: str | None
    wake_mode: str  # file | webhook | unknown
    webhook_url: str | None
    runtime_hint: str | None


def _error(
    findings: list[Finding],
    code: str,
    message: str,
    *,
    path: str | None = None,
    runtime: str | None = None,
    service: str | None = None,
) -> None:
    findings.append(
        Finding(
            severity="error",
            code=code,
            message=message,
            path=path,
            runtime=runtime,
            service=service,
        )
    )


def _warn(
    findings: list[Finding],
    code: str,
    message: str,
    *,
    path: str | None = None,
    runtime: str | None = None,
    service: str | None = None,
) -> None:
    findings.append(
        Finding(
            severity="warning",
            code=code,
            message=message,
            path=path,
            runtime=runtime,
            service=service,
        )
    )


def _load_swarm_lenient(workspace: Path) -> SwarmConfig | None:
    """Load swarm.yaml; return None if missing (caller adds finding).

    Unlike ``load_swarm_config``, allows all services to be disabled so
    pre-flight still runs against a fully parked swarm.
    """
    path = swarm_yaml_path(workspace)
    if not path.is_file():
        return None
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("swarm.yaml root must be a mapping")
    version = str(raw.get("version") or "1.0")
    services_raw = raw.get("services") or {}
    if not isinstance(services_raw, dict) or not services_raw:
        raise ValueError("swarm.yaml must define a non-empty 'services' mapping")
    services: dict[str, ServiceSpec] = {}
    for name, defn in services_raw.items():
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"invalid service name: {name!r}")
        if any(ch in name for ch in ("/", "\\", "..")) or name in {".", ".."}:
            raise ValueError(f"invalid service name (path-unsafe): {name!r}")
        if isinstance(defn, str):
            command, env, cwd, enabled = defn, {}, None, True
        elif isinstance(defn, dict):
            enabled = bool(defn.get("enabled", True))
            command = defn.get("command")
            if not command or not isinstance(command, str):
                raise ValueError(f"service '{name}' requires string 'command'")
            env = {str(k): str(v) for k, v in (defn.get("env") or {}).items()}
            cwd = defn.get("cwd")
            if cwd is not None:
                cwd = str(cwd)
        else:
            raise ValueError(f"service '{name}' must be a string command or mapping")
        services[name] = ServiceSpec(
            name=name, command=command, env=env, cwd=cwd, enabled=enabled
        )
    return SwarmConfig(version=version, services=services, path=path)


def _parse_runtime_from_ingress_cmd(command: str) -> str | None:
    m = _RUNTIME_FROM_CMD.search(command)
    if m:
        return m.group(1).strip().strip("\"'").lower()
    try:
        parts = shlex.split(command)
    except ValueError:
        return None
    for i, p in enumerate(parts):
        if p == "--runtime" and i + 1 < len(parts):
            return parts[i + 1].lower()
        if p.startswith("--runtime="):
            return p.split("=", 1)[1].lower()
    return None


def _parse_config_from_cmd(command: str) -> str | None:
    m = _CONFIG_FROM_CMD.search(command)
    if m:
        return m.group(1).strip().strip("\"'")
    try:
        parts = shlex.split(command)
    except ValueError:
        return None
    for i, p in enumerate(parts):
        if p == "--config" and i + 1 < len(parts):
            return parts[i + 1]
        if p.startswith("--config="):
            return p.split("=", 1)[1]
    return None


def _is_ingress_service(name: str, command: str) -> bool:
    if name.endswith(_INGRESS_SUFFIX):
        return True
    return "wake-ingress" in command


def _is_runner_service(name: str, command: str) -> bool:
    if name.endswith(_RUNNER_SUFFIX):
        return True
    # ``agentbus run --config …`` headless runner (not worker up)
    if re.search(r"\bagentbus\s+run\b", command) or re.search(
        r"\bagentbus\.cli\b.*\brun\b", command
    ):
        return True
    return False


def _runtime_from_ingress_name(name: str) -> str | None:
    if name.endswith(_INGRESS_SUFFIX):
        return name[: -len(_INGRESS_SUFFIX)].lower() or None
    return None


def _runtime_from_runner_name(name: str) -> str | None:
    if name.endswith(_RUNNER_SUFFIX):
        return name[: -len(_RUNNER_SUFFIX)].lower() or None
    return None


def _collect_ingress(services: dict[str, ServiceSpec]) -> list[_IngressService]:
    out: list[_IngressService] = []
    for name, spec in services.items():
        if not _is_ingress_service(name, spec.command):
            continue
        runtime = _parse_runtime_from_ingress_cmd(spec.command) or _runtime_from_ingress_name(
            name
        )
        if not runtime:
            # still record with empty runtime so we can error
            runtime = ""
        out.append(
            _IngressService(
                name=name,
                runtime=runtime,
                enabled=spec.enabled,
                command=spec.command,
            )
        )
    return out


def _resolve_config_path(workspace: Path, rel: str | None) -> Path | None:
    if not rel:
        return None
    p = Path(rel)
    if p.is_absolute():
        return p
    candidate = (workspace / p).resolve()
    if candidate.is_file():
        return candidate
    # relative to cwd style
    alt = Path(rel).expanduser().resolve()
    return alt if alt.is_file() else candidate


def _collect_runners(workspace: Path, services: dict[str, ServiceSpec]) -> list[_RunnerService]:
    out: list[_RunnerService] = []
    for name, spec in services.items():
        if not _is_runner_service(name, spec.command):
            continue
        cfg_rel = _parse_config_from_cmd(spec.command)
        out.append(
            _RunnerService(
                name=name,
                enabled=spec.enabled,
                command=spec.command,
                config_path=_resolve_config_path(workspace, cfg_rel),
                runtime_hint=_runtime_from_runner_name(name),
            )
        )
    return out


def _discover_workers(workspace: Path) -> list[_WorkerFile]:
    adir = swarm_dir(workspace)
    if not adir.is_dir():
        return []
    workers: list[_WorkerFile] = []
    for path in sorted(adir.glob("worker*.yaml")):
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except (OSError, yaml.YAMLError):
            workers.append(
                _WorkerFile(
                    path=path,
                    producer_id=None,
                    wake_mode="unknown",
                    webhook_url=None,
                    runtime_hint=None,
                )
            )
            continue
        if not isinstance(raw, dict):
            continue
        producer = raw.get("producer_id") or raw.get("worker_id")
        producer_s = str(producer).strip() if producer else None
        wake_mode = str(raw.get("wake_mode") or "file").strip().lower()
        if wake_mode not in {"file", "webhook"}:
            wake_mode = "unknown"
        webhook_url = raw.get("webhook_url")
        webhook_s = str(webhook_url).strip() if webhook_url else None
        # Heuristic runtime: worker.factory.yaml → factory; worker.yaml → None
        runtime_hint = None
        stem = path.stem  # worker.factory or worker
        if stem.startswith("worker.") and len(stem) > len("worker."):
            runtime_hint = stem[len("worker.") :].lower()
        elif producer_s:
            runtime_hint = producer_s.lower()
        workers.append(
            _WorkerFile(
                path=path,
                producer_id=producer_s,
                wake_mode=wake_mode,
                webhook_url=webhook_s,
                runtime_hint=runtime_hint,
            )
        )
    return workers


def _queue_nonempty(path: Path) -> bool:
    if not path.is_file():
        return False
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    for line in text.splitlines():
        if line.strip():
            return True
    return False


def validate_workspace_config(workspace: Path) -> ValidationReport:
    """Validate swarm / runner / worker / roles pairing for *workspace*.

    Hard errors → ``ok=False``. Soft issues are warnings; ``ok`` stays True
    if only warnings are present.
    """
    workspace = workspace.resolve()
    findings: list[Finding] = []
    summary: dict[str, Any] = {
        "swarm_path": str(swarm_yaml_path(workspace)),
        "swarm_exists": swarm_yaml_path(workspace).is_file(),
        "roles_path": str(roles_path(workspace)),
        "roles_exists": roles_path(workspace).is_file(),
        "ingress": [],
        "runners": [],
        "workers": [],
    }

    try:
        swarm = _load_swarm_lenient(workspace)
    except ValueError as exc:
        _error(
            findings,
            "SWARM_INVALID",
            f"invalid swarm.yaml: {exc}",
            path=str(swarm_yaml_path(workspace)),
        )
        return ValidationReport(
            workspace=workspace, ok=False, findings=findings, summary=summary
        )

    if swarm is None:
        _error(
            findings,
            "SWARM_MISSING",
            f"missing {swarm_yaml_path(workspace)} — create swarm.yaml "
            "(see examples/swarm.yaml)",
            path=str(swarm_yaml_path(workspace)),
        )
        return ValidationReport(
            workspace=workspace, ok=False, findings=findings, summary=summary
        )

    if not any(s.enabled for s in swarm.services.values()):
        _warn(
            findings,
            "SWARM_ALL_DISABLED",
            "swarm.yaml has no enabled services (all set enabled: false)",
            path=str(swarm.path),
        )

    ingress_list = _collect_ingress(swarm.services)
    runner_list = _collect_runners(workspace, swarm.services)
    workers = _discover_workers(workspace)

    # Index runners by runtime (name hint + loaded config)
    runners_by_runtime: dict[str, list[tuple[_RunnerService, Any]]] = {}
    for rsvc in runner_list:
        cfg = None
        load_err: str | None = None
        if rsvc.config_path is None:
            if rsvc.enabled:
                _error(
                    findings,
                    "RUNNER_CONFIG_MISSING",
                    f"enabled runner service '{rsvc.name}' has no --config path "
                    f"in command: {rsvc.command!r}",
                    service=rsvc.name,
                )
        elif not rsvc.config_path.is_file():
            if rsvc.enabled:
                _error(
                    findings,
                    "RUNNER_CONFIG_NOT_FOUND",
                    f"enabled runner service '{rsvc.name}' config not found: "
                    f"{rsvc.config_path}",
                    path=str(rsvc.config_path),
                    service=rsvc.name,
                )
            else:
                _warn(
                    findings,
                    "RUNNER_CONFIG_NOT_FOUND",
                    f"disabled runner service '{rsvc.name}' config not found: "
                    f"{rsvc.config_path}",
                    path=str(rsvc.config_path),
                    service=rsvc.name,
                )
        else:
            try:
                cfg = load_runner_config(rsvc.config_path)
            except (ValueError, FileNotFoundError, OSError) as exc:
                load_err = str(exc)
                if rsvc.enabled:
                    _error(
                        findings,
                        "RUNNER_CONFIG_INVALID",
                        f"enabled runner '{rsvc.name}' config invalid: {exc}",
                        path=str(rsvc.config_path),
                        service=rsvc.name,
                    )
                else:
                    _warn(
                        findings,
                        "RUNNER_CONFIG_INVALID",
                        f"disabled runner '{rsvc.name}' config invalid: {exc}",
                        path=str(rsvc.config_path),
                        service=rsvc.name,
                    )

        runtime = None
        if cfg is not None:
            runtime = (cfg.intake.runtime or rsvc.runtime_hint or cfg.producer_id or "").lower()
            # producer_id / accept_to already enforced by load_runner_config
            if not cfg.producer_id:
                _error(
                    findings,
                    "RUNNER_PRODUCER_EMPTY",
                    f"runner '{rsvc.name}' has empty producer_id",
                    path=str(rsvc.config_path),
                    service=rsvc.name,
                )
            if not cfg.accept_to:
                _error(
                    findings,
                    "RUNNER_ACCEPT_TO_EMPTY",
                    f"runner '{rsvc.name}' has empty accept_to",
                    path=str(rsvc.config_path),
                    service=rsvc.name,
                )
        elif rsvc.runtime_hint:
            runtime = rsvc.runtime_hint

        entry = {
            "service": rsvc.name,
            "enabled": rsvc.enabled,
            "config": str(rsvc.config_path) if rsvc.config_path else None,
            "runtime": runtime,
            "intake_mode": cfg.intake.mode if cfg else None,
            "producer_id": cfg.producer_id if cfg else None,
            "load_error": load_err,
        }
        summary["runners"].append(entry)

        if runtime:
            runners_by_runtime.setdefault(runtime, []).append((rsvc, cfg))

    for ing in ingress_list:
        summary["ingress"].append(
            {
                "service": ing.name,
                "enabled": ing.enabled,
                "runtime": ing.runtime or None,
            }
        )
        if not ing.runtime:
            if ing.enabled:
                _error(
                    findings,
                    "INGRESS_RUNTIME_UNKNOWN",
                    f"enabled ingress '{ing.name}' has no --runtime and name "
                    f"does not end with '-wake-ingress'",
                    service=ing.name,
                )
            continue

        matched = runners_by_runtime.get(ing.runtime, [])
        enabled_matched = [(s, c) for s, c in matched if s.enabled]

        if ing.enabled:
            # Rule 1: ingress on ⇒ runner on + webhook_queue + matching runtime
            if not enabled_matched:
                # Check if a disabled runner exists for clearer message
                if matched:
                    names = ", ".join(s.name for s, _ in matched)
                    _error(
                        findings,
                        "INGRESS_WITHOUT_RUNNER",
                        f"ingress '{ing.name}' is enabled for runtime "
                        f"'{ing.runtime}' but matching runner(s) are disabled "
                        f"({names}) — queue will stagnate (#682)",
                        service=ing.name,
                        runtime=ing.runtime,
                    )
                else:
                    _error(
                        findings,
                        "INGRESS_WITHOUT_RUNNER",
                        f"ingress '{ing.name}' is enabled for runtime "
                        f"'{ing.runtime}' but no matching '*-runner' service "
                        f"found — queue will stagnate (#682)",
                        service=ing.name,
                        runtime=ing.runtime,
                    )
            else:
                for rsvc, cfg in enabled_matched:
                    if cfg is None:
                        continue
                    if cfg.intake.mode != "webhook_queue":
                        _error(
                            findings,
                            "INGRESS_RUNNER_MODE_MISMATCH",
                            f"ingress '{ing.name}' enabled but runner "
                            f"'{rsvc.name}' intake.mode is "
                            f"'{cfg.intake.mode}' (need webhook_queue) — #682",
                            path=str(rsvc.config_path) if rsvc.config_path else None,
                            service=rsvc.name,
                            runtime=ing.runtime,
                        )
                    elif cfg.intake.runtime and cfg.intake.runtime.lower() != ing.runtime:
                        _error(
                            findings,
                            "INGRESS_RUNNER_RUNTIME_MISMATCH",
                            f"ingress runtime '{ing.runtime}' does not match "
                            f"runner '{rsvc.name}' intake.runtime "
                            f"'{cfg.intake.runtime}'",
                            path=str(rsvc.config_path) if rsvc.config_path else None,
                            service=rsvc.name,
                            runtime=ing.runtime,
                        )
        else:
            # Soft: residual queue while ingress off
            qpath = default_queue_path(workspace, ing.runtime)
            if _queue_nonempty(qpath):
                _warn(
                    findings,
                    "STALE_QUEUE_INGRESS_OFF",
                    f"ingress '{ing.name}' is disabled but queue has lines: "
                    f"{qpath} (historical or stagnant backlog)",
                    path=str(qpath),
                    service=ing.name,
                    runtime=ing.runtime,
                )

    # Rule 2: enabled runner with webhook_queue ⇒ prefer ingress enabled
    for runtime, pairs in runners_by_runtime.items():
        for rsvc, cfg in pairs:
            if not rsvc.enabled or cfg is None:
                continue
            if cfg.intake.mode != "webhook_queue":
                continue
            rt = (cfg.intake.runtime or runtime).lower()
            ing_enabled = any(
                i.enabled and i.runtime == rt for i in ingress_list
            )
            if not ing_enabled:
                _warn(
                    findings,
                    "RUNNER_WEBHOOK_WITHOUT_INGRESS",
                    f"runner '{rsvc.name}' uses intake.mode=webhook_queue for "
                    f"runtime '{rt}' but matching wake-ingress is missing or "
                    f"disabled — queue will not fill via HTTP",
                    path=str(rsvc.config_path) if rsvc.config_path else None,
                    service=rsvc.name,
                    runtime=rt,
                )

    # Rule 4: worker wake_mode: webhook implies full triad
    for w in workers:
        summary["workers"].append(
            {
                "path": str(w.path),
                "producer_id": w.producer_id,
                "wake_mode": w.wake_mode,
                "runtime": w.runtime_hint,
            }
        )
        if w.wake_mode == "unknown":
            _warn(
                findings,
                "WORKER_PARSE",
                f"could not parse worker config: {w.path}",
                path=str(w.path),
            )
            continue
        if w.wake_mode != "webhook":
            continue
        rt = (w.runtime_hint or "").lower()
        if not rt:
            _warn(
                findings,
                "WORKER_RUNTIME_UNKNOWN",
                f"worker {w.path.name} has wake_mode=webhook but no runtime "
                f"hint (filename worker.<runtime>.yaml or producer_id)",
                path=str(w.path),
            )
            continue

        ing_on = any(i.enabled and i.runtime == rt for i in ingress_list)
        if not ing_on:
            _error(
                findings,
                "WORKER_WEBHOOK_WITHOUT_INGRESS",
                f"worker {w.path.name} has wake_mode=webhook for runtime "
                f"'{rt}' but matching wake-ingress is missing or disabled "
                f"(#682 triad)",
                path=str(w.path),
                runtime=rt,
            )

        matched_runners = runners_by_runtime.get(rt, [])
        enabled_wq = [
            (s, c)
            for s, c in matched_runners
            if s.enabled and c is not None and c.intake.mode == "webhook_queue"
        ]
        if not enabled_wq:
            _error(
                findings,
                "WORKER_WEBHOOK_WITHOUT_RUNNER_QUEUE",
                f"worker {w.path.name} has wake_mode=webhook for runtime "
                f"'{rt}' but no enabled runner with intake.mode=webhook_queue "
                f"(#682 triad)",
                path=str(w.path),
                runtime=rt,
            )

        if not w.webhook_url:
            _warn(
                findings,
                "WORKER_WEBHOOK_URL_MISSING",
                f"worker {w.path.name} has wake_mode=webhook but no webhook_url",
                path=str(w.path),
                runtime=rt,
            )

    # Rule 3: roles.yaml — producer must map to an existing role when RBAC on
    rbac = load_rbac_config(workspace)
    if rbac is not None:
        summary["rbac"] = {
            "enabled": True,
            "role_count": len(rbac.roles),
            "producer_count": len(rbac.producers),
        }
        for rsvc in runner_list:
            if not rsvc.enabled or not rsvc.config_path or not rsvc.config_path.is_file():
                continue
            try:
                cfg = load_runner_config(rsvc.config_path)
            except (ValueError, FileNotFoundError, OSError):
                continue
            pid = cfg.producer_id
            role_name = rbac.producers.get(pid)
            if role_name is None:
                _warn(
                    findings,
                    "RBAC_PRODUCER_UNMAPPED",
                    f"runner '{rsvc.name}' producer_id '{pid}' is not mapped "
                    f"in roles.yaml producers (RBAC may deny or default)",
                    path=str(roles_path(workspace)),
                    service=rsvc.name,
                )
            elif role_name not in rbac.roles:
                _error(
                    findings,
                    "RBAC_ROLE_MISSING",
                    f"runner '{rsvc.name}' producer_id '{pid}' maps to role "
                    f"'{role_name}' which is not defined in roles.yaml",
                    path=str(roles_path(workspace)),
                    service=rsvc.name,
                )
    else:
        summary["rbac"] = {
            "enabled": False,
            "reason": "roles.yaml missing or AGENTBUS_DISABLE_RBAC",
        }

    # Prefer load_swarm_config consistency: if strict parse would fail for
    # reasons other than all-disabled, surface that (already handled).
    # Optional: cross-check with strict loader when services enabled.
    if any(s.enabled for s in swarm.services.values()):
        try:
            load_swarm_config(workspace)
        except ValueError as exc:
            _error(
                findings,
                "SWARM_STRICT_INVALID",
                f"swarm.yaml fails strict load (agentbus up): {exc}",
                path=str(swarm.path),
            )

    ok = not any(f.severity == "error" for f in findings)
    return ValidationReport(
        workspace=workspace, ok=ok, findings=findings, summary=summary
    )
