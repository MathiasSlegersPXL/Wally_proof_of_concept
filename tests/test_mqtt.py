import json

from app.data_generator import RobotData
from app.strategies.mqtt import (
    DEFAULT_MQTT_HOST,
    DEFAULT_MQTT_PORT,
    DEFAULT_MQTT_QOS,
    DEFAULT_MQTT_TOPIC,
    build_mqtt_payload,
    mqtt_config_from_env,
)
from scripts.run_polling_benchmark import mqtt_message_to_row


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


def test_mqtt_config_uses_safe_defaults(monkeypatch):
    monkeypatch.delenv("MQTT_ENABLED", raising=False)
    monkeypatch.delenv("MQTT_HOST", raising=False)
    monkeypatch.delenv("MQTT_PORT", raising=False)
    monkeypatch.delenv("MQTT_TOPIC", raising=False)
    monkeypatch.delenv("MQTT_QOS", raising=False)

    config = mqtt_config_from_env()

    assert config.enabled is False
    assert config.host == DEFAULT_MQTT_HOST
    assert config.port == DEFAULT_MQTT_PORT
    assert config.topic == DEFAULT_MQTT_TOPIC
    assert config.qos == DEFAULT_MQTT_QOS


def test_mqtt_config_reads_environment(monkeypatch):
    monkeypatch.setenv("MQTT_ENABLED", "1")
    monkeypatch.setenv("MQTT_HOST", "mqtt.local")
    monkeypatch.setenv("MQTT_PORT", "1884")
    monkeypatch.setenv("MQTT_TOPIC", "robots/demo/telemetry")
    monkeypatch.setenv("MQTT_QOS", "1")

    config = mqtt_config_from_env()

    assert config.enabled is True
    assert config.host == "mqtt.local"
    assert config.port == 1884
    assert config.topic == "robots/demo/telemetry"
    assert config.qos == 1


def test_build_mqtt_payload_matches_strategy_shape():
    payload = build_mqtt_payload(robot_data(message_id=42))

    assert payload["strategy"] == "mqtt"
    assert "served_at" in payload
    assert payload["data"]["message_id"] == 42
    assert payload["data"]["robot_id"] == "robot-1"


def test_mqtt_message_to_row_calculates_data_age_and_size():
    body = build_mqtt_payload(robot_data(message_id=2, created_at=1_000))
    payload = json.dumps(body, separators=(",", ":")).encode("utf-8")

    row, last_message_id = mqtt_message_to_row(
        payload=payload,
        generation_interval_ms=1000,
        last_message_id=1,
        stream_started_at=900,
        received_at=1_025,
    )

    assert last_message_id == 2
    assert row["strategy"] == "mqtt"
    assert row["request_started_at"] == 900
    assert row["request_latency_ms"] == -1
    assert row["data_age_ms"] == 25
    assert row["duplicate"] is False
    assert row["missed_messages"] == 0
    assert row["response_bytes"] == len(payload)


def test_mqtt_message_to_row_detects_duplicate_and_missed_messages():
    duplicate_payload = json.dumps(build_mqtt_payload(robot_data(message_id=2))).encode("utf-8")
    missed_payload = json.dumps(build_mqtt_payload(robot_data(message_id=5))).encode("utf-8")

    duplicate_row, last_message_id = mqtt_message_to_row(
        payload=duplicate_payload,
        generation_interval_ms=1000,
        last_message_id=2,
        stream_started_at=900,
        received_at=1_025,
    )
    missed_row, last_message_id = mqtt_message_to_row(
        payload=missed_payload,
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
