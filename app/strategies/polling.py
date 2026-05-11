import time

from fastapi import APIRouter, Request
from starlette.responses import JSONResponse

from app.strategies.common import build_strategy_response, get_generator

router = APIRouter(prefix="/api/polling", tags=["polling"])


@router.get("/latest")
async def get_latest(request: Request):
    started_at = time.perf_counter()
    generator = get_generator(request)
    if generator is None:
        return JSONResponse(status_code=500, content={"detail": "Generator not started"})

    data = await generator.snapshot()
    if data is None:
        return JSONResponse(status_code=204, content=None)

    return build_strategy_response("polling", data, started_at)
