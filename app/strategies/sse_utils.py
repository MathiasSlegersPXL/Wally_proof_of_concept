def parse_last_event_id(value: str | None) -> int | None:
    if value is None or value == "":
        return None

    try:
        parsed = int(value)
    except ValueError:
        return None

    if parsed < 0:
        return None
    return parsed


def format_sse_event(event: str, event_id: str, data: str) -> str:
    lines = [f"event: {event}", f"id: {event_id}"]
    for line in data.splitlines() or [""]:
        lines.append(f"data: {line}")
    return "\n".join(lines) + "\n\n"


def parse_sse_frame(lines: list[str]) -> dict | None:
    if not lines or all(line.startswith(":") for line in lines):
        return None

    event = {"event": "message", "id": None, "data": ""}
    data_lines = []

    for line in lines:
        if line.startswith(":"):
            continue

        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]

        if field == "event":
            event["event"] = value
        elif field == "id":
            event["id"] = value
        elif field == "data":
            data_lines.append(value)

    event["data"] = "\n".join(data_lines)
    return event
