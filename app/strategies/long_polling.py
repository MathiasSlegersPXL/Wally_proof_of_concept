import time

from fastapi import APIRouter, Query, Request
from starlette.responses import JSONResponse

from app.strategies.common import build_strategy_response, get_generator

DEFAULT_TIMEOUT_MS = 30_000
MIN_TIMEOUT_MS = 1_000
MAX_TIMEOUT_MS = 60_000

router = APIRouter(prefix="/api/long-polling", tags=["long-polling"])


@router.get("/latest")
async def get_latest(
    request: Request,
    last_message_id: int | None = Query(default=None, ge=0),
    timeout_ms: int = Query(default=DEFAULT_TIMEOUT_MS, ge=MIN_TIMEOUT_MS, le=MAX_TIMEOUT_MS),
):
    started_at = time.perf_counter()
    generator = get_generator(request)
    if generator is None:
        return JSONResponse(status_code=500, content={"detail": "Generator not started"})

    data = await generator.wait_for_next(last_message_id=last_message_id, timeout_ms=timeout_ms)
    if data is None:
        return JSONResponse(status_code=204, content=None)

    return build_strategy_response("long_polling", data, started_at)
