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
    """Update one object member while retaining unrelated source and comments."""
    clean = sanitize(text)
    decoder = json.JSONDecoder()

    def members(start: int):
        i = start + 1
        while True:
            while clean[i].isspace() or clean[i] == ',':
                i += 1
            if clean[i] == '}':
                return i, None
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

    start = len(clean) - len(clean.lstrip())
    found = locate(start, root)
    encoded = json.dumps(value, indent=2, ensure_ascii=False)
    if found is None:
        return insert(start, root, '{' + json.dumps(name) + ': ' + encoded + '}')
    begin, _ = found
    found_entry = locate(begin, name)
    if found_entry is None:
        return insert(begin, name, encoded)
    begin, end = found_entry
    return text[:begin] + encoded + text[end:]
