import os
import time
from dataclasses import dataclass

import grpc

from app.data_generator import RobotData, RobotDataGenerator
from app.grpc_generated import telemetry_pb2, telemetry_pb2_grpc

DEFAULT_GRPC_HOST = "127.0.0.1"
DEFAULT_GRPC_PORT = 50051
GRPC_WAIT_TIMEOUT_MS = 1_000


@dataclass(frozen=True)
class GrpcConfig:
    enabled: bool = False
    host: str = DEFAULT_GRPC_HOST
    port: int = DEFAULT_GRPC_PORT


def grpc_config_from_env() -> GrpcConfig:
    return GrpcConfig(
        enabled=os.getenv("GRPC_ENABLED") == "1",
        host=os.getenv("GRPC_HOST", DEFAULT_GRPC_HOST),
        port=int(os.getenv("GRPC_PORT", str(DEFAULT_GRPC_PORT))),
    )


def robot_data_to_proto(data: RobotData) -> telemetry_pb2.RobotData:
    return telemetry_pb2.RobotData(
        message_id=data.message_id,
        robot_id=data.robot_id,
        server_timestamp=data.server_timestamp,
        status=data.status,
        bricks_placed=data.bricks_placed,
        bricks_per_minute=data.bricks_per_minute,
        error_code=data.error_code or "",
        glue_quality=data.glue_quality,
        created_at=data.created_at,
    )


def build_grpc_response(data: RobotData) -> telemetry_pb2.TelemetryResponse:
    return telemetry_pb2.TelemetryResponse(
        strategy="grpc",
        served_at=int(time.time() * 1000),
        data=robot_data_to_proto(data),
    )


class TelemetryGrpcServicer(telemetry_pb2_grpc.TelemetryServiceServicer):
    def __init__(self, generator: RobotDataGenerator):
        self.generator = generator

    async def StreamTelemetry(self, request, context):
        last_message_id = request.last_message_id or None

        while not context.done():
            data = await self.generator.wait_for_next(
                last_message_id=last_message_id,
                timeout_ms=GRPC_WAIT_TIMEOUT_MS,
            )
            if data is None:
                continue

            last_message_id = data.message_id
            yield build_grpc_response(data)


class GrpcTelemetryService:
    def __init__(self, generator: RobotDataGenerator, config: GrpcConfig):
        self.generator = generator
        self.config = config
        self._server = None

    async def start(self):
        if not self.config.enabled:
            return
        if self._server is not None:
            return

        server = grpc.aio.server()
        telemetry_pb2_grpc.add_TelemetryServiceServicer_to_server(
            TelemetryGrpcServicer(self.generator),
            server,
        )
        server.add_insecure_port(f"{self.config.host}:{self.config.port}")
        await server.start()
        self._server = server

    async def stop(self):
        if self._server is None:
            return

        await self._server.stop(grace=0)
        self._server = None
