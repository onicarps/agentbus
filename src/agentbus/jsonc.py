"""JSON with comments and trailing commas, preserving source offsets."""
import json


def sanitize(text: str, *, trailing_commas: bool = True) -> str:
    chars = list(text)
    i = 0
    string = False
    while i < len(text):
        c = text[i]
        if string:
            if c == "\\":
                i += 2
                continue
            if c == '"':
                string = False
        elif c == '"':
            string = True
        elif text.startswith('//', i):
            end = text.find('\n', i + 2)
            end = len(text) if end < 0 else end
            chars[i:end] = ' ' * (end - i)
            i = end
            continue
        elif text.startswith('/*', i):
            end = text.find('*/', i + 2)
            if end < 0:
                raise json.JSONDecodeError('unterminated comment', text, i)
            end += 2
            chars[i:end] = [c if c in '\r\n' else ' ' for c in text[i:end]]
            i = end
            continue
        i += 1
    cleaned = ''.join(chars)
    if not trailing_commas:
        return cleaned
    # Examine commas outside strings, after comments have been masked.
    string = False
    i = 0
    while i < len(cleaned):
        c = cleaned[i]
        if string:
            if c == '\\':
                i += 2
                continue
            if c == '"':
                string = False
        elif c == '"':
            string = True
        elif c == ',':
            j = i + 1
            while j < len(cleaned) and cleaned[j].isspace():
                j += 1
            k = i - 1
            while k >= 0 and cleaned[k].isspace():
                k -= 1
            if j < len(cleaned) and cleaned[j] in '}]' and k >= 0 and cleaned[k] not in '{[,:' :
                chars[i] = ' '
        i += 1
    return ''.join(chars)


def loads(text: str):
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return json.loads(sanitize(text))


def update_entry(text: str, root: str, name: str, value: dict) -> str:
    """Update one object member while retaining unrelated source and comments.

    ``root`` may be dotted (``mcp.servers``). Missing intermediate objects are
    created with the remaining chain already populated, so the patched
    document is valid in one pass.
    """
    parts = [part for part in str(root).split(".") if part]
    if not parts:
        raise ValueError("root must name at least one object")

    clean = sanitize(text)
    decoder = json.JSONDecoder()
    encoded = json.dumps(value, indent=2, ensure_ascii=False)

    def members(start: int):
        i = start + 1
        while True:
            while clean[i].isspace() or clean[i] == ',':
                i += 1
            if clean[i] == '}':
                return
            key, end = decoder.raw_decode(clean, i)
            i = end
            while clean[i].isspace() or clean[i] == ':':
                i += 1
            _, end = decoder.raw_decode(clean, i)
            yield key, i, end
            i = end

    def locate(start: int, key: str):
        for member, begin, end in members(start):
            if member == key:
                return begin, end
        return None

    def insert(start: int, key: str, val: str) -> str:
        _, end = decoder.raw_decode(clean, start)
        close = end - 1
        body = clean[start + 1:close].rstrip()
        untrimmed = sanitize(text, trailing_commas=False)[start + 1:close].rstrip()
        comma = ',' if body and not untrimmed.endswith(',') else ''
        return text[:close] + '\n' + comma + '\n' + json.dumps(key) + ': ' + val + '\n' + text[close:]

    def nest(chain: list[str]) -> str:
        payload = '{' + json.dumps(name) + ': ' + encoded + '}'
        for part in reversed(chain):
            payload = '{' + json.dumps(part) + ': ' + payload + '}'
        return payload

    start = len(clean) - len(clean.lstrip())
    for index, part in enumerate(parts):
        found = locate(start, part)
        if found is None:
            return insert(start, part, nest(parts[index + 1:]))
        begin, _ = found
        if index == len(parts) - 1:
            entry = locate(begin, name)
            if entry is None:
                return insert(begin, name, encoded)
            entry_begin, entry_end = entry
            return text[:entry_begin] + encoded + text[entry_end:]
        probe = begin
        while clean[probe].isspace():
            probe += 1
        if clean[probe] != '{':
            raise ValueError(f"{root!r}: {part!r} is not a JSON object")
        start = begin
    raise ValueError(f"{root!r}: could not resolve object path")
