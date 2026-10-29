"""Client config patching: pure, in-memory planning and serialization.

Nothing here writes to disk.  ``plan_patch`` returns the exact bytes a
transaction should install, or raises :class:`ConfigConflictError` when the
existing file cannot be changed without risking unrelated configuration.
"""

from __future__ import annotations

import json
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import unquote, urlsplit, urlunsplit

from agentbus.adapters.manifest import SERVER_NAME, Installation

_TABLE_HEADER = re.compile(r"^\s*\[(?P<name>[^\[\]]+)\]\s*(?:#.*)?$")
_OUR_HEADER = re.compile(
    r"^\s*\[mcp_servers\.agentbus(?:\.env)?\]\s*(?:#.*)?$"
)
_DOTTED_OWNED_KEY = re.compile(
    r"^\s*mcp_servers\s*\.\s*agentbus\s*\.", re.IGNORECASE
)

#: Configuration keys whose values are never rendered outside the process.
_SECRET_KEY = re.compile(
    r"(token|secret|password|passwd|api[_-]?key|apikey|auth(?:orization)?|"
    r"bearer|credential|cookie|private[_-]?key|access[_-]?key|"
    r"sig(?:nature)?|session)",
    re.IGNORECASE,
)
#: Values that look like credentials even when their key does not.
_SECRET_VALUE = re.compile(
    r"(bearer\s+\S+|"
    r"\b(?:sk|pk|ghp|gho|ghs|ghu|xox[abprs])[-_][A-Za-z0-9_-]{8,}|"
    r"[A-Za-z][A-Za-z0-9+.-]*://[^/\s:@]+:[^/\s@]+@)",
    re.IGNORECASE,
)
#: URL query parameters whose value is a credential regardless of spelling.
#: Matching is deliberately broad: over-redacting a diagnostic is safe, while
#: echoing a credential into a terminal, log, or JSON report is not.
_SECRET_URL_PARAM = re.compile(
    r"^(?:"
    r"auth|authorization|token|access[_-]?token|refresh[_-]?token|id[_-]?token|"
    r"key|api[_-]?key|apikey|secret|client[_-]?secret|password|passwd|"
    r"credential|credentials|private[_-]?key|access[_-]?key|"
    r"sig|signature|session|session[_-]?id|bearer"
    r")$",
    re.IGNORECASE,
)
#: ``--flag=https://...`` keeps the flag visible and redacts only the value.
_ASSIGNMENT_PREFIX = re.compile(r"^(?P<flag>--?[^=\s]+)=.*$")
_REDACTED = "[REDACTED]"
_ENV_CONTAINERS = frozenset({"env", "environment", "headers", "http_headers", "env_http_headers"})


class ConfigConflictError(ValueError):
    """The config file cannot be updated safely (malformed or conflicting)."""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(f"{code}: {message}")


def redact_url(text: str) -> str:
    """Redact credentials embedded in a URL, preserving its structure.

    Credentials are frequently smuggled through a URL rather than a flag::

        https://user:password@host/mcp?auth=abc&api_key=def

    A value's spelling proves nothing about whether it is a credential, so
    this function redacts by *position*: HTTP basic-auth userinfo and the
    values of well-known sensitive query parameters. Everything else (scheme,
    host, port, path, unrelated query parameters) is preserved so the output
    stays diagnosable.
    """
    candidate = text.strip()
    prefix = ""
    match = _ASSIGNMENT_PREFIX.match(candidate)
    if match is not None:
        candidate = candidate[match.end("flag") + 1 :]
        prefix = f"{match.group('flag')}="
    if "://" not in candidate:
        return text
    try:
        parts = urlsplit(candidate)
    except ValueError:
        return text
    if not parts.scheme or not parts.netloc:
        return text
    netloc = parts.netloc
    host = netloc.rpartition("@")[2]
    if "@" in netloc:
        # Never echo basic-auth credentials, not even the user name prefix.
        netloc = f"{_REDACTED}@{host}"
    def redact_parameters(value: str) -> str:
        chunks: list[str] = []
        for chunk in value.split("&"):
            name, separator, _value = chunk.partition("=")
            # Fragments can have a route followed by a query string.
            parameter = name.rsplit("?", 1)[-1]
            decoded = unquote(parameter)
            if separator and (_SECRET_URL_PARAM.fullmatch(decoded) or _SECRET_KEY.search(decoded)):
                chunks.append(f"{name}={_REDACTED}")
            else:
                chunks.append(chunk)
        return "&".join(chunks)

    query = redact_parameters(parts.query)
    fragment = redact_parameters(parts.fragment)
    redacted = urlunsplit(
        (parts.scheme, netloc, parts.path, query, fragment)
    )
    return f"{prefix}{redacted}"


def _redact_string(value: str) -> str:
    """Redact a single argument, including any URL it carries."""
    safe = redact_url(value)
    if safe != value:
        return safe
    return _REDACTED if _SECRET_VALUE.search(safe) else safe


def _redact_value(value: Any) -> Any:
    if isinstance(value, str):
        return _redact_string(value)
    if isinstance(value, (list, tuple)):
        return [_redact_value(item) for item in value]
    if isinstance(value, dict):
        return _redact_mapping(value)
    return value


_SECRET_FLAG = re.compile(r"^--?(?:k|key|pat|pass|pw|cred|creds)$", re.IGNORECASE)


def _redact_arg_list(values: Sequence[Any]) -> list[Any]:
    redacted: list[Any] = []
    mask_next = False
    for item in values:
        if mask_next:
            redacted.append(_REDACTED)
            mask_next = False
            continue
        if isinstance(item, str):
            if item.startswith("-") and (_SECRET_KEY.search(item) or _SECRET_FLAG.match(item.partition("=")[0])):
                if "=" in item:
                    flag, _, _value = item.partition("=")
                    redacted.append(f"{flag}={_REDACTED}")
                else:
                    redacted.append(item)
                    mask_next = True
                continue
            key, separator, _value = item.partition("=")
            if separator and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) and _SECRET_KEY.search(key):
                redacted.append(f"{key}={_REDACTED}")
                continue
            header, colon, _value = item.partition(":")
            if colon and "://" not in item and re.fullmatch(r"[A-Za-z0-9_-]+", header.strip()) and _SECRET_KEY.search(header):
                redacted.append(f"{header.strip()}: {_REDACTED}")
                continue
            redacted.append(_redact_string(item))
            continue
        redacted.append(_redact_value(item))
    return redacted


def _redact_mapping(mapping: Mapping[str, Any]) -> dict[str, Any]:
    redacted: dict[str, Any] = {}
    for key, value in mapping.items():
        name = str(key)
        if name.lower() in _ENV_CONTAINERS and isinstance(value, dict):
            # Environment/header values are credentials by default. Keep the
            # key names so operators can diagnose; never the values.
            redacted[name] = {str(k): _REDACTED for k in value}
        elif name.lower() in _ENV_CONTAINERS:
            redacted[name] = (
                [_REDACTED for _ in value]
                if isinstance(value, (list, tuple)) else _REDACTED
            )
        elif _SECRET_KEY.search(name):
            redacted[name] = _REDACTED
        elif name == "args" and isinstance(value, (list, tuple)):
            redacted[name] = _redact_arg_list(value)
        else:
            redacted[name] = _redact_value(value)
    return redacted


def redact_entry(entry: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Return a display-safe copy of an existing client entry.

    Every value inside ``env``/``environment``/``headers`` is replaced with
    ``[REDACTED]`` and any key or value that looks like a credential is
    masked. The result is safe for JSON output, logs, diagnostics, and error
    strings.
    """
    if entry is None:
        return None
    return _redact_mapping(entry)


def entry_env_keys(entry: Mapping[str, Any] | None) -> list[str]:
    """Sorted environment variable names of an entry, without values."""
    if not isinstance(entry, dict):
        return []
    env = entry.get("env")
    if not isinstance(env, dict):
        return []
    return sorted(str(key) for key in env)


def is_agentbus_entry(entry: Any) -> bool:
    """Whether ``entry`` looks like an AgentBus-managed server entry.

    Used to refuse overwriting an ``agentbus`` key that another tool owns.
    AgentBus entries always carry the workspace/producer environment, the
    ``agentbus`` command name, or the ``mcp-serve`` argument.
    """
    if not isinstance(entry, dict):
        return False
    env = entry.get("env")
    if isinstance(env, dict) and (
        "AGENTBUS_WORKSPACE" in env or "AGENTBUS_PRODUCER_ID" in env
    ):
        return True
    command = entry.get("command")
    if isinstance(command, str) and Path(command).name in (
        "agentbus",
        "agentbus.exe",
    ):
        return True
    args = entry.get("args")
    if isinstance(args, list) and "mcp-serve" in args:
        return True
    return False


@dataclass(frozen=True)
class Patch:
    installation_id: str
    path: Path
    format: str
    exists: bool
    changed: bool
    original_bytes: bytes | None
    new_bytes: bytes | None
    reason: str
    previous_entry: Mapping[str, Any] | None = None

    def summary(self) -> dict[str, Any]:
        return {
            "installation": self.installation_id,
            "path": str(self.path),
            "format": self.format,
            "exists": self.exists,
            "changed": self.changed,
            "reason": self.reason,
            "previous_entry": redact_entry(self.previous_entry),
        }


def read_bytes(path: Path) -> bytes | None:
    """Read a config file, returning ``None`` when it does not exist."""
    try:
        from agentbus.safe_file import read_regular_file

        return read_regular_file(path)
    except FileNotFoundError:
        return None
    except PermissionError as exc:
        raise ConfigConflictError("permission_denied", f"{path}: {exc}") from exc
    except OSError as exc:
        raise ConfigConflictError("unreadable", f"{path}: {exc}") from exc


def parse_json(raw: bytes | None, *, path: Path) -> dict[str, Any]:
    if raw is None or not raw.strip():
        return {}
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ConfigConflictError(
            "non_utf8", f"{path}: expected UTF-8 JSON"
        ) from exc
    try:
        from agentbus.jsonc import loads

        document = loads(text)
    except json.JSONDecodeError as exc:
        raise ConfigConflictError("malformed_json", f"{path}: {exc}") from exc
    if not isinstance(document, dict):
        raise ConfigConflictError("not_object", f"{path}: expected a JSON object")
    return document


def serialize_json(document: Mapping[str, Any]) -> bytes:
    return (json.dumps(document, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


_UNSAFE_ENV = re.compile(r"^(PYTHON.*|LD_.*|DYLD_.*|NODE_OPTIONS|NODE_PATH|RUBYOPT|PERL5OPT|BASH_ENV|ENV|PATH|HOME|SSL_CERT_FILE|SSL_CERT_DIR|REQUESTS_CA_BUNDLE)$", re.IGNORECASE)
_INERT_ENTRY_KEYS = {"disabled", "enabled", "startup_timeout_sec", "tool_timeout_sec"}
_MANAGED_ENV = {"AGENTBUS_WORKSPACE", "AGENTBUS_PRODUCER_ID"}


def _unsafe_env_key(key: object) -> bool:
    name = str(key)
    return bool(_UNSAFE_ENV.match(name)) or (
        name.upper().startswith("AGENTBUS_") and name not in _MANAGED_ENV
    )


def _unsafe_extra_keys(actual: Mapping[str, Any], expected: Mapping[str, Any]) -> set[str]:
    extra = set(actual) - set(expected) - _INERT_ENTRY_KEYS
    if actual.get("type") == "stdio":
        extra.discard("type")
    return extra


def entry_matches(actual: Any, expected: Mapping[str, Any]) -> bool:
    if not isinstance(actual, dict) or _unsafe_extra_keys(actual, expected):
        return False
    for key, value in expected.items():
        if key == "env" and isinstance(value, dict):
            current = actual.get(key)
            if not isinstance(current, dict) or any(_unsafe_env_key(k) for k in current):
                return False
            if any(current.get(k) != v for k, v in value.items()):
                return False
        elif key not in actual or actual[key] != value:
            return False
    return True


def _merge_entry(previous: Mapping[str, Any] | None, entry: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(previous or {})
    if _unsafe_extra_keys(merged, entry):
        raise ConfigConflictError("unsafe_entry_key", "existing entry has unrecognized launch settings; edit it manually")
    for key, value in entry.items():
        if key == "env" and isinstance(value, dict):
            existing = merged.get(key, {})
            if not isinstance(existing, dict):
                raise ConfigConflictError("env_not_object", "existing env must be a mapping")
            if any(_unsafe_env_key(k) for k in existing):
                raise ConfigConflictError(
                    "unsafe_env", "existing environment contains unsafe keys; edit it manually"
                )
            merged[key] = {**existing, **value}
        else:
            merged[key] = value
    return merged


def plan_json_patch(
    *,
    installation_id: str,
    path: Path,
    root: str,
    entry: Mapping[str, Any],
    raw: bytes | None = None,
) -> Patch:
    original = read_bytes(path) if raw is None else raw
    document = parse_json(original, path=path)
    container = document.get(root)
    if container is None:
        container = {}
        document[root] = container
    elif not isinstance(container, dict):
        raise ConfigConflictError(
            "root_not_object",
            f"{path}: {root!r} must be a JSON object, found "
            f"{type(container).__name__}",
        )

    previous = container.get(SERVER_NAME)
    if previous is not None and not isinstance(previous, dict):
        raise ConfigConflictError(
            "entry_not_object",
            f"{path}: {root}.{SERVER_NAME} is not a JSON object",
        )
    if previous is not None and previous != entry and not is_agentbus_entry(
        previous
    ):
        raise ConfigConflictError(
            "entry_not_owned",
            f"{path}: {root}.{SERVER_NAME} already exists and is not managed "
            "by AgentBus; refusing to overwrite it",
        )
    if entry_matches(previous, entry):
        return Patch(
            installation_id=installation_id,
            path=path,
            format="json",
            exists=original is not None,
            changed=False,
            original_bytes=original,
            new_bytes=None,
            reason="unchanged",
            previous_entry=previous,
        )
    from agentbus.jsonc import loads as load_jsonc
    from agentbus.jsonc import update_entry as update_jsonc_entry

    container[SERVER_NAME] = _merge_entry(previous, entry)
    new_bytes = (
        update_jsonc_entry(original.decode("utf-8"), root, SERVER_NAME, container[SERVER_NAME]).encode("utf-8")
        if original and original.strip() else serialize_json(document)
    )
    try:
        patched = load_jsonc(new_bytes.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigConflictError("patch_invalid_json", f"{path}: generated JSON is invalid: {exc}") from exc
    if patched != document:
        raise ConfigConflictError("patch_roundtrip_mismatch", f"{path}: patched JSON does not match the planned document; edit manually")
    return Patch(
        installation_id=installation_id,
        path=path,
        format="json",
        exists=original is not None,
        changed=True,
        original_bytes=original,
        new_bytes=new_bytes,
        reason="updated" if original is not None else "created",
        previous_entry=previous,
    )


def _toml_string(value: str) -> str:
    # JSON string escapes are a valid subset of TOML basic strings.
    return json.dumps(value, ensure_ascii=False)


def render_toml_table(table: str, entry: Mapping[str, Any]) -> str:
    lines: list[str] = [f"[{table}]"]
    for key, value in entry.items():
        if key == "env":
            continue
        lines.append(f"{key} = {_render_toml_value(value)}")
    env = entry.get("env")
    if isinstance(env, dict) and env:
        lines.append("")
        lines.append(f"[{table}.env]")
        for key, value in env.items():
            lines.append(f"{key} = {_render_toml_value(value)}")
    return "\n".join(lines) + "\n"


def _render_toml_value(value: Any) -> str:
    if isinstance(value, str):
        return _toml_string(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_render_toml_value(item) for item in value) + "]"
    if isinstance(value, dict):
        inner = ", ".join(
            f"{key} = {_render_toml_value(item)}" for key, item in value.items()
        )
        return "{ " + inner + " }"
    raise ConfigConflictError(
        "unsupported_value", f"cannot render {type(value).__name__} as TOML"
    )


def _strip_owned_table(document: Mapping[str, Any], table: str) -> dict[str, Any]:
    """Return a deep-ish copy without the table this adapter owns."""
    root, _, leaf = table.partition(".")
    clone = dict(document)
    if not leaf:
        clone.pop(root, None)
        return clone
    parent = clone.get(root)
    if not isinstance(parent, dict):
        return clone
    parent_copy = dict(parent)
    parent_copy.pop(leaf, None)
    if parent_copy:
        clone[root] = parent_copy
    else:
        clone.pop(root, None)
    return clone


def _find_table_block(lines: list[str], table: str) -> tuple[int, int] | None:
    """Locate ``[table]`` and the exclusive end of that table's block."""
    start = None
    for index, line in enumerate(lines):
        match = _TABLE_HEADER.match(line)
        if match and match.group("name").strip() == table:
            start = index
            break
    if start is None:
        return None
    end = len(lines)
    for index in range(start + 1, len(lines)):
        if lines[index].lstrip().startswith("[["):
            end = index
            break
        match = _TABLE_HEADER.match(lines[index])
        if not match:
            continue
        name = match.group("name").strip()
        if name == table or name.startswith(table + "."):
            continue  # sub-table of the block we are replacing
        end = index
        break
    if end < len(lines):
        while end > start + 1 and (
            not lines[end - 1].strip() or lines[end - 1].lstrip().startswith("#")
        ):
            end -= 1
    return start, end


def plan_toml_patch(
    *,
    installation_id: str,
    path: Path,
    table: str,
    entry: Mapping[str, Any],
    raw: bytes | None = None,
) -> Patch:
    original = read_bytes(path) if raw is None else raw
    if original is None or not original.strip():
        text = ""
        document: dict[str, Any] = {}
    else:
        try:
            text = original.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ConfigConflictError(
                "non_utf8", f"{path}: expected UTF-8 TOML"
            ) from exc
        try:
            document = tomllib.loads(text)
        except tomllib.TOMLDecodeError as exc:
            raise ConfigConflictError("malformed_toml", f"{path}: {exc}") from exc

    root, _, leaf = table.partition(".")
    current: Any = document
    for part in table.split("."):
        current = current.get(part) if isinstance(current, dict) else None
    if current is not None and not isinstance(current, dict):
        raise ConfigConflictError(
            "entry_not_object", f"{path}: [{table}] is not a table"
        )
    if current is not None and current != entry and not is_agentbus_entry(
        current
    ):
        raise ConfigConflictError(
            "entry_not_owned",
            f"{path}: [{table}] already exists and is not managed by "
            "AgentBus; refusing to overwrite it",
        )
    if entry_matches(current, entry):
        return Patch(
            installation_id=installation_id,
            path=path,
            format="toml",
            exists=original is not None,
            changed=False,
            original_bytes=original,
            new_bytes=None,
            reason="unchanged",
            previous_entry=current,
        )

    merged = _merge_entry(current, entry)
    rendered = render_toml_table(table, merged)
    lines = text.splitlines(keepends=True)
    block = _find_table_block(lines, table)
    if block is None:
        for line in lines:
            if _DOTTED_OWNED_KEY.match(line):
                raise ConfigConflictError(
                    "dotted_key_conflict",
                    f"{path}: mcp_servers.agentbus is defined with dotted keys; "
                    "edit it manually",
                )
        prefix = text
        if prefix and not prefix.endswith("\n"):
            prefix += "\n"
        if prefix.strip():
            prefix += "\n"
        candidate = prefix + rendered
    else:
        start, end = block
        head = "".join(lines[:start])
        tail = "".join(lines[end:])
        if head and not head.endswith("\n"):
            head += "\n"
        if head.strip() and not head.endswith("\n\n"):
            head = head.rstrip("\n") + "\n\n"
        candidate = head + rendered
        if tail:
            candidate += "\n" + tail.lstrip("\n")

    try:
        patched = tomllib.loads(candidate)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigConflictError(
            "patch_invalid_toml", f"{path}: generated TOML is invalid: {exc}"
        ) from exc

    if _strip_owned_table(patched, table) != _strip_owned_table(document, table):
        raise ConfigConflictError(
            "unrelated_toml_changed",
            f"{path}: patching [{table}] would alter unrelated keys; edit manually",
        )
    patched_value: Any = patched
    for part in table.split("."):
        patched_value = patched_value[part]
    if patched_value != merged:
        raise ConfigConflictError(
            "patch_roundtrip_mismatch",
            f"{path}: patched [{table}] does not round-trip",
        )

    return Patch(
        installation_id=installation_id,
        path=path,
        format="toml",
        exists=original is not None,
        changed=True,
        original_bytes=original,
        new_bytes=candidate.encode("utf-8"),
        reason="updated" if original is not None else "created",
        previous_entry=current,
    )


def plan_patch(
    installation: Installation,
    *,
    path: Path,
    entry: Mapping[str, Any],
    raw: bytes | None = None,
) -> Patch:
    if installation.format == "json":
        return plan_json_patch(
            installation_id=installation.installation_id,
            path=path,
            root=installation.root,
            entry=entry,
            raw=raw,
        )
    if installation.format == "toml":
        return plan_toml_patch(
            installation_id=installation.installation_id,
            path=path,
            table=installation.root,
            entry=entry,
            raw=raw,
        )
    raise ConfigConflictError(
        "unsupported_format", f"{installation.installation_id}: {installation.format}"
    )
