import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app.data_generator import RobotDataGenerator
from app.strategies.grpc_streaming import GrpcTelemetryService, grpc_config_from_env
from app.strategies.long_polling import router as long_polling_router
from app.strategies.mqtt import MqttPublisherService, mqtt_config_from_env
from app.strategies.polling import router as polling_router
from app.strategies.sse import router as sse_router


generator = RobotDataGenerator(interval_ms=1000)


class SimulationConfigUpdate(BaseModel):
    interval_ms: int = Field(
        ge=RobotDataGenerator.MIN_INTERVAL_MS,
        le=RobotDataGenerator.MAX_INTERVAL_MS,
    )
    seed: int | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.generator = generator
    app.state.mqtt_publisher = MqttPublisherService(generator, mqtt_config_from_env())
    app.state.grpc_service = GrpcTelemetryService(generator, grpc_config_from_env())
    await generator.start()
    await app.state.mqtt_publisher.start()
    await app.state.grpc_service.start()
    yield
    await app.state.grpc_service.stop()
    await app.state.mqtt_publisher.stop()
    await generator.stop()


app = FastAPI(title="Wally POC - Realtime Strategies", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(polling_router)
app.include_router(long_polling_router)
app.include_router(sse_router)


@app.get("/api/health")
async def health_check():
    return {"status": "ok"}


@app.get("/api/simulation/config")
async def get_simulation_config():
    return app.state.generator.config()


@app.post("/api/simulation/config")
async def update_simulation_config(config: SimulationConfigUpdate):
    try:
        await app.state.generator.configure(interval_ms=config.interval_ms, seed=config.seed)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return app.state.generator.config()


static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/")
    async def serve_index():
        return FileResponse(os.path.join(static_dir, "index.html"))
