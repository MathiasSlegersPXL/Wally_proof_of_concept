import json
import time

from fastapi import APIRouter, Request
from starlette.responses import JSONResponse, StreamingResponse

from app.strategies.common import build_strategy_response, get_generator
from app.strategies.sse_utils import format_sse_event, parse_last_event_id

HEARTBEAT_INTERVAL_MS = 15_000

router = APIRouter(prefix="/api/sse", tags=["sse"])


@router.get("/latest")
async def stream_latest(request: Request):
    generator = get_generator(request)
    if generator is None:
        return JSONResponse(status_code=500, content={"detail": "Generator not started"})

    last_message_id = parse_last_event_id(request.headers.get("last-event-id"))

    return StreamingResponse(
        event_stream(request, generator, last_message_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


async def event_stream(request: Request, generator, last_message_id: int | None):
    while not await request.is_disconnected():
        data = await generator.wait_for_next(
            last_message_id=last_message_id,
            timeout_ms=HEARTBEAT_INTERVAL_MS,
        )

        if data is None:
            yield ": heartbeat\n\n"
            continue

        started_at = time.perf_counter()
        payload = build_strategy_response("sse", data, started_at)
        last_message_id = data.message_id
        yield format_sse_event(
            event="telemetry",
            event_id=str(data.message_id),
            data=json.dumps(payload, separators=(",", ":")),
        )
