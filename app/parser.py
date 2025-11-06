# parser.py
from typing import Dict, Tuple

def parse_param_values(text: str) -> Dict[str, str]:
    """
    Existing function (kept for backward-compat).
    Parses:
        Section::Parameter::Value::1
    Returns a map of Parameter -> Value.
    """
    mapping: Dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("::")
        if len(parts) < 3:
            continue
        param = parts[1].strip()
        value = parts[2].strip()
        mapping[param] = value
    return mapping


def parse_param_values_and_pages(text: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    NEW: Parse both values and (optional) page numbers.
    Accepts lines like: Section::Parameter::Value::1
    Returns:
        (values_map, pages_map)
        - values_map: Parameter -> Value
        - pages_map:  Parameter -> Page (string; '' if missing/not numeric)
    Keeps the last occurrence if duplicates appear.
    """
    values: Dict[str, str] = {}
    pages: Dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("::")
        if len(parts) < 3:
            continue
        param = parts[1].strip()
        value = parts[2].strip()
        values[param] = value

        page = ""
        if len(parts) >= 4:
            last = parts[-1].strip()
            # store only if it looks like a page indicator (digits)
            if last.isdigit():
                page = last
        pages[param] = page
    return values, pages
