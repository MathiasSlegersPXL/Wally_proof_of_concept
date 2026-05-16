from app.strategies.sse_utils import format_sse_event, parse_last_event_id, parse_sse_frame


def test_parse_last_event_id_accepts_only_non_negative_integers():
    assert parse_last_event_id(None) is None
    assert parse_last_event_id("") is None
    assert parse_last_event_id("abc") is None
    assert parse_last_event_id("-1") is None
    assert parse_last_event_id("42") == 42


def test_format_sse_event_formats_single_line_data():
    event = format_sse_event(event="telemetry", event_id="42", data='{"ok":true}')

    assert event == 'event: telemetry\nid: 42\ndata: {"ok":true}\n\n'


def test_format_sse_event_formats_multiline_data():
    event = format_sse_event(event="telemetry", event_id="42", data="line one\nline two")

    assert event == "event: telemetry\nid: 42\ndata: line one\ndata: line two\n\n"


def test_parse_sse_frame_ignores_heartbeat_comments():
    assert parse_sse_frame([": heartbeat"]) is None


def test_parse_sse_frame_collects_event_id_and_multiline_data():
    event = parse_sse_frame(
        [
            "event: telemetry",
            "id: 42",
            'data: {"message"',
            'data: :"ok"}',
        ]
    )

    assert event == {
        "event": "telemetry",
        "id": "42",
        "data": '{"message"\n:"ok"}',
    }
