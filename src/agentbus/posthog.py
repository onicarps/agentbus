"""Privacy-preserving, cursor-driven PostHog outbound telemetry.

This module deliberately contains no inbound webhook handling.  External
alerts are a separate security boundary and are outside the v1 exporter.
"""

from __future__ import annotations

import email.utils
import hashlib
import json
import logging
import math
import os
import random
import signal
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from agentbus.store import EventStore
from agentbus.wiretap import redact_text

log = logging.getLogger("agentbus.posthog")

DEFAULT_HOST = "https://us.i.posthog.com"
TRANSIENT_STATUS = frozenset({408, 429, 500, 502, 503, 504})
NON_RETRYABLE_STATUS = frozenset({400, 401, 403, 422})
UUID_NAMESPACE = uuid.NAMESPACE_DNS
MAX_RETRIES = 5


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false")


@dataclass(frozen=True)
class PostHogConfig:
    project_api_key: str
    host: str = DEFAULT_HOST
    enabled: bool = True
    capture_prompts: bool = False
    batch_size: int = 50
    poll_interval_seconds: float = 2.0
    request_timeout_seconds: float = 5.0

    @property
    def capture_url(self) -> str:
        return f"{self.host.rstrip('/')}/batch/"


def load_config(*, require_key: bool = True) -> PostHogConfig:
    key = (os.environ.get("POSTHOG_PROJECT_API_KEY") or "").strip()
    if require_key and not key:
        raise ValueError("POSTHOG_PROJECT_API_KEY is required")
    host = (os.environ.get("POSTHOG_HOST") or DEFAULT_HOST).strip().rstrip("/")
    parsed = urlparse(host)
    allow_insecure = _env_bool("POSTHOG_ALLOW_INSECURE_DEV", False)
    if parsed.scheme != "https" and not (
        allow_insecure
        and parsed.scheme == "http"
        and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    ):
        raise ValueError("POSTHOG_HOST must use HTTPS (HTTP is localhost dev-only)")
    if (
        not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("POSTHOG_HOST must be a hostname URL without credentials/query")
    if parsed.path not in {"", "/"}:
        raise ValueError("POSTHOG_HOST must not include an endpoint path")
    try:
        batch_size = int(os.environ.get("POSTHOG_BATCH_SIZE", "50"))
        interval = float(os.environ.get("POSTHOG_POLL_INTERVAL_SECONDS", "2"))
    except ValueError as exc:
        raise ValueError("PostHog batch size and poll interval must be numeric") from exc
    if not 1 <= batch_size <= 1000:
        raise ValueError("POSTHOG_BATCH_SIZE must be between 1 and 1000")
    if interval < 0.1:
        raise ValueError("POSTHOG_POLL_INTERVAL_SECONDS must be >= 0.1")
    return PostHogConfig(
        project_api_key=key,
        host=host,
        enabled=_env_bool("POSTHOG_TELEMETRY_ENABLED", True),
        capture_prompts=_env_bool("POSTHOG_CAPTURE_PROMPTS", False),
        batch_size=batch_size,
        poll_interval_seconds=interval,
    )


def _fsync_dir(path: Path) -> None:
    if os.name == "nt":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def workspace_id(workspace: Path) -> str:
    path = workspace / ".agentbus" / "workspace_id"
    try:
        value = path.read_text(encoding="utf-8").strip()
        return str(uuid.UUID(value))
    except (OSError, ValueError):
        value = str(uuid.uuid4())
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            # Avoid two concurrent initializers replacing each other's ID.
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            return str(uuid.UUID(path.read_text(encoding="utf-8").strip()))
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(value + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_dir(path.parent)
        return value


@dataclass
class Cursor:
    last_event_id: int = 0
    updated_at: str = ""
    workspace_id: str = ""
    delivered_events_total: int = 0
    quarantined_events_total: int = 0
    last_success_at: str | None = None
    last_error: str | None = None
    started_at: str = ""

    @classmethod
    def load(cls, path: Path, expected_workspace_id: str) -> "Cursor":
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return cls(workspace_id=expected_workspace_id, started_at=_utc_now())
        except (OSError, json.JSONDecodeError, TypeError) as exc:
            raise ValueError(f"invalid PostHog cursor: {exc}") from exc
        if raw.get("workspace_id") != expected_workspace_id:
            raise ValueError("PostHog cursor belongs to a different workspace")
        allowed = {field for field in cls.__dataclass_fields__}
        cursor = cls(**{key: value for key, value in raw.items() if key in allowed})
        for field_name in (
            "last_event_id",
            "delivered_events_total",
            "quarantined_events_total",
        ):
            value = getattr(cursor, field_name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"invalid PostHog cursor field: {field_name}")
        return cursor

    def save(self, path: Path) -> None:
        self.updated_at = _utc_now()
        _atomic_json(path, asdict(self))


def deterministic_uuid(workspace: str, event_id: int, event_name: str) -> str:
    return str(uuid.uuid5(UUID_NAMESPACE, f"{workspace}:{event_id}:{event_name}"))


def _bounded_scalar(value: Any, *, max_length: int = 160) -> Any | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        return redact_text(value, max_len=max_length)
    return None


def _origin_flags(links: Any) -> tuple[bool, bool, int]:
    if not isinstance(links, list):
        return False, False, 0
    schemes = [str(link).split("?", 1)[0] for link in links]
    return (
        any(link.startswith("slack://") for link in schemes),
        any(link.startswith("telegram://") for link in schemes),
        len(links),
    )


def _qa_properties(event: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    action = payload.get("action") if isinstance(payload.get("action"), dict) else {}
    raw_verdict = action.get("result") or payload.get("verdict") or action.get("verdict")
    verdict = str(raw_verdict or "").upper()
    if verdict not in {"GREEN", "RED", "AMBER"}:
        verdict = "UNKNOWN"
    props: dict[str, Any] = {
        "verdict": verdict,
        "initiative": _bounded_scalar(payload.get("initiative")),
        "causation_id": event.get("causation_id"),
    }
    for key in ("mission_id", "target_commit", "coverage"):
        value = _bounded_scalar(action.get(key, payload.get(key)))
        if value is not None:
            props[key] = value
    return {key: value for key, value in props.items() if value is not None}


def _ai_properties(event: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    integer_fields = (
        "$ai_input_tokens",
        "$ai_output_tokens",
        "$ai_cache_read_tokens",
    )
    def number(key: str) -> float:
        try:
            value = float(payload.get(key) or 0)
        except (TypeError, ValueError):
            return 0.0
        return value if math.isfinite(value) and value >= 0 else 0.0

    def count(key: str) -> int:
        return min(int(number(key)), 2**63 - 1)

    props: dict[str, Any] = {
        "$ai_trace_id": _bounded_scalar(
            payload.get("$ai_trace_id") or event.get("trace_id") or f"event-{event['event_id']}"
        ),
        "$ai_model": _bounded_scalar(payload.get("$ai_model") or "unknown"),
        "$ai_provider": _bounded_scalar(payload.get("$ai_provider") or "unknown"),
        "$ai_latency_ms": number("$ai_latency_ms"),
        "$ai_cost_usd": number("$ai_cost_usd"),
        "$ai_cost_source": _bounded_scalar(payload.get("$ai_cost_source") or "unavailable"),
        "$ai_is_error": bool(payload.get("$ai_is_error", False)),
        "$ai_error_type": _bounded_scalar(payload.get("$ai_error_type")),
    }
    for key in integer_fields:
        props[key] = count(key)
    return {key: value for key, value in props.items() if value is not None}


def map_event(event: dict[str, Any], ws_id: str) -> dict[str, Any] | None:
    """Map one AgentBus event through a metadata-only schema allowlist."""
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    action = payload.get("action") if isinstance(payload.get("action"), dict) else {}
    action_type = str(action.get("type") or payload.get("type") or "")
    topic = str(event.get("topic") or "")

    if topic == "okf/handoff" and action_type == "qa_verdict":
        name = "swarm_qa_verdict"
        properties = _qa_properties(event, payload)
    elif topic == "okf/handoff":
        name = "swarm_handoff_dispatched"
        slack, telegram, link_count = _origin_flags(payload.get("links"))
        properties = {
            "from_agent": _bounded_scalar(payload.get("from") or event.get("producer_id")),
            "to_agent": _bounded_scalar(payload.get("to")),
            "topic": "okf/handoff",
            "initiative": _bounded_scalar(payload.get("initiative")),
            "has_slack_origin": slack,
            "has_telegram_origin": telegram,
            "link_count": link_count,
            "summary_length": len(str(payload.get("summary") or "")),
        }
        properties = {key: value for key, value in properties.items() if value is not None}
    elif topic == "system/runner" and action_type == "ai_generation":
        name = "$ai_generation"
        properties = _ai_properties(event, payload)
    elif topic == "system/lock" and action_type == "swarm_lock_event":
        name = "swarm_lock_event"
        properties = {}
        for key in ("lock_name", "holder", "action", "duration_held_s", "renew_count"):
            value = _bounded_scalar(payload.get(key))
            if value is not None:
                properties[key] = value
    else:
        return None

    event_id = int(event["event_id"])
    producer = str(event.get("producer_id") or "unknown")
    properties.update(
        {
            "agentbus_event_id": event_id,
            "agentbus_schema_version": str(event.get("schema_version") or ""),
            "$lib": "agentbus",
            "distinct_id": f"agentbus:{ws_id}:{producer}",
        }
    )
    return {
        "event": name,
        "uuid": deterministic_uuid(ws_id, event_id, name),
        "timestamp": event.get("timestamp"),
        "properties": properties,
    }


@dataclass(frozen=True)
class HTTPResult:
    status: int
    headers: dict[str, str]
    body: bytes = b""


def _send(url: str, body: bytes, timeout: float) -> HTTPResult:
    request = Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": "agentbus-posthog/1"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout) as response:  # noqa: S310 - validated HTTPS
            return HTTPResult(
                int(response.status), dict(response.headers.items()), response.read(4096)
            )
    except HTTPError as exc:
        return HTTPResult(int(exc.code), dict(exc.headers.items()), exc.read(4096))


def _retry_after(headers: dict[str, str]) -> float | None:
    raw = next((value for key, value in headers.items() if key.lower() == "retry-after"), None)
    if not raw:
        return None
    try:
        return max(0.0, min(60.0, float(raw)))
    except ValueError:
        try:
            target = email.utils.parsedate_to_datetime(raw)
            return max(0.0, min(60.0, target.timestamp() - time.time()))
        except (TypeError, ValueError, OverflowError):
            return None


class PostHogExporter:
    def __init__(
        self,
        workspace: Path,
        config: PostHogConfig,
        *,
        send: Callable[[str, bytes, float], HTTPResult] = _send,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[], float] = random.random,
        stopping: Callable[[], bool] = lambda: False,
    ) -> None:
        self.workspace = workspace.resolve()
        self.config = config
        self.send = send
        self.sleep = sleep
        self.jitter = jitter
        self.stopping = stopping
        self.ws_id = workspace_id(self.workspace)
        self.cursor_path = self.workspace / ".agentbus" / "posthog_cursor.json"
        self.quarantine_path = self.workspace / ".agentbus" / "posthog_quarantine.jsonl"
        self.cursor = Cursor.load(self.cursor_path, self.ws_id)

    def _capture(self, events: list[dict[str, Any]]) -> HTTPResult:
        body = json.dumps(
            {"api_key": self.config.project_api_key, "batch": events},
            separators=(",", ":"),
        ).encode("utf-8")
        last: HTTPResult | None = None
        for attempt in range(MAX_RETRIES + 1):
            if self.stopping():
                raise RuntimeError("PostHog streamer shutdown requested")
            try:
                last = self.send(self.config.capture_url, body, self.config.request_timeout_seconds)
            except (OSError, URLError, TimeoutError) as exc:
                if attempt >= MAX_RETRIES:
                    raise RuntimeError(f"PostHog request exhausted retries: {type(exc).__name__}") from exc
                delay = min(60.0, 1.0 * (2**attempt)) * self.jitter()
                self.sleep(delay)
                continue
            if last.status == 200:
                return last
            if last.status not in TRANSIENT_STATUS or attempt >= MAX_RETRIES:
                return last
            delay = _retry_after(last.headers)
            if delay is None:
                delay = min(60.0, 1.0 * (2**attempt)) * self.jitter()
            self.sleep(delay)
        assert last is not None
        return last

    def _quarantine(self, source: list[dict[str, Any]], status: int) -> None:
        record = {
            "quarantined_at": _utc_now(),
            "http_status": status,
            "event_ids": [int(event["event_id"]) for event in source],
            "event_count": len(source),
            "reason": "non_retryable_posthog_response",
        }
        self.quarantine_path.parent.mkdir(parents=True, exist_ok=True)
        with self.quarantine_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    def process_once(self) -> dict[str, Any]:
        if not self.config.enabled:
            return {"status": "disabled", "last_event_id": self.cursor.last_event_id}
        store = EventStore(self.workspace, auto_prune=False)
        try:
            page = store.poll_all(self.cursor.last_event_id, self.config.batch_size)
        finally:
            store.close()
        source = page["events"]
        if not source:
            return {"status": "idle", "last_event_id": self.cursor.last_event_id}
        mapped = [item for event in source if (item := map_event(event, self.ws_id))]
        next_cursor = int(page["latest_id"])
        if not mapped:
            self.cursor.last_event_id = next_cursor
            self.cursor.save(self.cursor_path)
            return {"status": "skipped", "source_events": len(source), "last_event_id": next_cursor}

        try:
            response = self._capture(mapped)
        except RuntimeError as exc:
            self.cursor.last_error = redact_text(str(exc), max_len=240)
            self.cursor.save(self.cursor_path)
            return {"status": "retry_exhausted", "last_event_id": self.cursor.last_event_id}
        if response.status == 200:
            self.cursor.last_event_id = next_cursor
            self.cursor.delivered_events_total += len(mapped)
            self.cursor.last_success_at = _utc_now()
            self.cursor.last_error = None
            self.cursor.save(self.cursor_path)
            return {"status": "delivered", "delivered": len(mapped), "last_event_id": next_cursor}
        if response.status in NON_RETRYABLE_STATUS:
            self._quarantine(source, response.status)
            self.cursor.last_event_id = next_cursor
            self.cursor.quarantined_events_total += len(source)
            self.cursor.last_error = f"HTTP {response.status} batch quarantined"
            self.cursor.save(self.cursor_path)
            return {"status": "quarantined", "events": len(source), "last_event_id": next_cursor}
        self.cursor.last_error = f"HTTP {response.status} after retries"
        self.cursor.save(self.cursor_path)
        return {"status": "retry_exhausted", "last_event_id": self.cursor.last_event_id}

    def status(self) -> dict[str, Any]:
        store = EventStore(self.workspace, auto_prune=False)
        try:
            latest = store.latest_event_id()
        finally:
            store.close()
        data = asdict(self.cursor)
        data.update(
            {
                "enabled": self.config.enabled,
                "host": self.config.host,
                "event_lag": max(0, latest - self.cursor.last_event_id),
                "latest_event_id": latest,
                "throughput_events_per_second": self._throughput(),
            }
        )
        return data

    def _throughput(self) -> float:
        try:
            started = datetime.fromisoformat(self.cursor.started_at.replace("Z", "+00:00"))
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        except (AttributeError, TypeError, ValueError):
            return 0.0
        if elapsed <= 0:
            return 0.0
        return round(self.cursor.delivered_events_total / elapsed, 3)

    def send_test(self) -> dict[str, Any]:
        marker = deterministic_uuid(self.ws_id, 0, "agentbus_synthetic_test_event")
        event = {
            "event": "agentbus_synthetic_test_event",
            "uuid": marker,
            "timestamp": _utc_now(),
            "properties": {
                "marker": marker,
                "$lib": "agentbus",
                "distinct_id": f"agentbus:{self.ws_id}:connectivity-test",
            },
        }
        response = self._capture([event])
        if response.status != 200:
            raise RuntimeError(f"PostHog connectivity test returned HTTP {response.status}")
        return {"status": "ok", "host": self.config.host, "marker": marker}


class StreamLock:
    """Cross-platform best-effort advisory singleton lock with a visible PID."""

    def __init__(self, workspace: Path) -> None:
        self.path = workspace / ".agentbus" / "posthog_streamer.pid"
        self.handle: Any = None

    def __enter__(self) -> "StreamLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+", encoding="utf-8")
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.handle.close()
            raise RuntimeError("PostHog streamer is already running") from exc
        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(str(os.getpid()) + "\n")
        self.handle.flush()
        os.fsync(self.handle.fileno())
        return self

    def __exit__(self, *_: Any) -> None:
        if self.handle is not None:
            self.handle.close()
        try:
            self.path.unlink()
        except OSError:
            pass


def run_stream(workspace: Path, config: PostHogConfig, *, once: bool = False) -> None:
    stop_event = threading.Event()
    exporter = PostHogExporter(
        workspace,
        config,
        sleep=stop_event.wait,
        stopping=stop_event.is_set,
    )

    def stop(_signum: int, _frame: Any) -> None:
        stop_event.set()

    previous: dict[int, Any] = {}
    with StreamLock(workspace):
        if not once:
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous[signum] = signal.signal(signum, stop)
        try:
            while not stop_event.is_set():
                result = exporter.process_once()
                if once:
                    return
                if result["status"] in {"idle", "retry_exhausted"}:
                    stop_event.wait(config.poll_interval_seconds)
        finally:
            for signum, handler in previous.items():
                signal.signal(signum, handler)


def _json_objects(text: str) -> list[dict[str, Any]]:
    objects: list[dict[str, Any]] = []
    for line in text.splitlines():
        try:
            value = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(value, dict):
            objects.append(value)
    return objects


def runner_generation_payload(
    *, adapter: str, model: str | None, trace_id: str | None,
    latency_ms: float, is_error: bool, detail: dict[str, Any] | None,
) -> dict[str, Any]:
    """Extract only numeric usage fields from adapter output; never content."""
    candidates: list[dict[str, Any]] = []
    if isinstance(detail, dict):
        candidates.append(detail)
        for key in ("stdout", "stderr"):
            if isinstance(detail.get(key), str):
                candidates.extend(_json_objects(detail[key]))

    aliases = {
        "$ai_input_tokens": ("input_tokens", "prompt_tokens"),
        "$ai_output_tokens": ("output_tokens", "completion_tokens"),
        "$ai_cache_read_tokens": ("cache_read_tokens", "cached_input_tokens"),
    }
    usage: dict[str, int] = {key: 0 for key in aliases}
    cost = 0.0
    cost_source = "unavailable"

    def walk(value: Any) -> None:
        nonlocal cost, cost_source
        if isinstance(value, dict):
            for target, names in aliases.items():
                for name in names:
                    raw = value.get(name)
                    if isinstance(raw, (int, float)) and raw >= 0:
                        usage[target] = max(usage[target], int(raw))
            raw_cost = value.get("cost_usd")
            if isinstance(raw_cost, (int, float)) and raw_cost >= 0:
                cost = max(cost, float(raw_cost))
                cost_source = "adapter_reported"
            for child in value.values():
                if isinstance(child, (dict, list)):
                    walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    for candidate in candidates:
        walk(candidate)
    return {
        "type": "ai_generation",
        "$ai_trace_id": trace_id or "",
        "$ai_model": model or "unknown",
        "$ai_provider": adapter,
        "$ai_latency_ms": round(max(0.0, latency_ms), 2),
        "$ai_cost_usd": cost,
        "$ai_cost_source": cost_source,
        "$ai_is_error": is_error,
        "$ai_error_type": "runner_error" if is_error else None,
        **usage,
    }


def emit_runner_generation(
    store: EventStore, *, runner_id: str, adapter: str, model: str | None,
    wake_event_id: int, trace_id: str | None, latency_ms: float,
    is_error: bool, detail: dict[str, Any] | None,
) -> None:
    if not _env_bool("POSTHOG_TELEMETRY_ENABLED", True):
        return
    if adapter == "echo" or (isinstance(detail, dict) and detail.get("dry_run") is True):
        return
    effective_trace_id = trace_id or f"runner:{runner_id}:{wake_event_id}"
    try:
        store.publish(
            topic="system/runner",
            producer_id="runner-telemetry",
            schema_version="1.0",
            payload={
                **runner_generation_payload(
                    adapter=adapter, model=model, trace_id=effective_trace_id,
                    latency_ms=latency_ms, is_error=is_error, detail=detail,
                ),
                "runner_id": runner_id,
            },
            causation_id=wake_event_id,
            idempotency_key=f"runner-generation:{runner_id}:{wake_event_id}",
            trace_id=effective_trace_id,
            skip_rbac=True,
        )
    except Exception:
        log.exception("could not record runner telemetry event_id=%s", wake_event_id)
