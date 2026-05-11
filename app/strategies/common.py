import time

from fastapi import Request

from app.data_generator import RobotData, RobotDataGenerator


def get_generator(request: Request) -> RobotDataGenerator | None:
    return request.app.state.generator


def build_strategy_response(strategy: str, data: RobotData, started_at: float) -> dict:
    served_at = int(time.time() * 1000)
    server_processing_ms = round((time.perf_counter() - started_at) * 1000, 3)

    return {
        "strategy": strategy,
        "served_at": served_at,
        "server_processing_ms": server_processing_ms,
        "data": data.to_dict(),
    }
