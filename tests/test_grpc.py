from app.data_generator import RobotData
from app.strategies.grpc_streaming import (
    DEFAULT_GRPC_HOST,
    DEFAULT_GRPC_PORT,
    build_grpc_response,
    grpc_config_from_env,
    robot_data_to_proto,
)
from scripts.run_polling_benchmark import grpc_message_to_row


def robot_data(message_id: int, created_at: int = 1_000) -> RobotData:
    return RobotData(
        message_id=message_id,
        robot_id="robot-1",
        server_timestamp="2026-05-16T12:00:00.000Z",
        status="running",
        bricks_placed=10,
        bricks_per_minute=14.2,
        error_code=None,
        glue_quality=0.97,
        created_at=created_at,
    )


def test_grpc_config_uses_safe_defaults(monkeypatch):
    monkeypatch.delenv("GRPC_ENABLED", raising=False)
    monkeypatch.delenv("GRPC_HOST", raising=False)
    monkeypatch.delenv("GRPC_PORT", raising=False)

    config = grpc_config_from_env()

    assert config.enabled is False
    assert config.host == DEFAULT_GRPC_HOST
    assert config.port == DEFAULT_GRPC_PORT


def test_grpc_config_reads_environment(monkeypatch):
    monkeypatch.setenv("GRPC_ENABLED", "1")
    monkeypatch.setenv("GRPC_HOST", "0.0.0.0")
    monkeypatch.setenv("GRPC_PORT", "50052")

    config = grpc_config_from_env()

    assert config.enabled is True
    assert config.host == "0.0.0.0"
    assert config.port == 50052


def test_robot_data_to_proto_copies_robot_fields():
    proto = robot_data_to_proto(robot_data(message_id=42))

    assert proto.message_id == 42
    assert proto.robot_id == "robot-1"
    assert proto.status == "running"
    assert proto.error_code == ""
    assert proto.created_at == 1_000


def test_build_grpc_response_matches_strategy_shape():
    response = build_grpc_response(robot_data(message_id=42))

    assert response.strategy == "grpc"
    assert response.served_at > 0
    assert response.data.message_id == 42
    assert response.data.robot_id == "robot-1"


def test_grpc_message_to_row_calculates_data_age_and_size():
    response = build_grpc_response(robot_data(message_id=2, created_at=1_000))

    row, last_message_id = grpc_message_to_row(
        message=response,
        generation_interval_ms=1000,
        last_message_id=1,
        stream_started_at=900,
        received_at=1_025,
    )

    assert last_message_id == 2
    assert row["strategy"] == "grpc"
    assert row["request_started_at"] == 900
    assert row["request_latency_ms"] == -1
    assert row["data_age_ms"] == 25
    assert row["duplicate"] is False
    assert row["missed_messages"] == 0
    assert row["response_bytes"] == response.ByteSize()


def test_grpc_message_to_row_detects_duplicate_and_missed_messages():
    duplicate_response = build_grpc_response(robot_data(message_id=2))
    missed_response = build_grpc_response(robot_data(message_id=5))

    duplicate_row, last_message_id = grpc_message_to_row(
        message=duplicate_response,
        generation_interval_ms=1000,
        last_message_id=2,
        stream_started_at=900,
        received_at=1_025,
    )
    missed_row, last_message_id = grpc_message_to_row(
        message=missed_response,
        generation_interval_ms=1000,
        last_message_id=last_message_id,
        stream_started_at=900,
        received_at=1_030,
    )

    assert duplicate_row["duplicate"] is True
    assert duplicate_row["missed_messages"] == 0
    assert missed_row["duplicate"] is False
    assert missed_row["missed_messages"] == 2
    assert last_message_id == 5
