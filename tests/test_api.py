import asyncio
import json

import pytest
from httpx import ASGITransport, AsyncClient

from app.data_generator import RobotDataGenerator
from app.main import app
from app.strategies.sse import event_stream


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def client():
    test_generator = RobotDataGenerator(interval_ms=1000, seed=1)
    app.state.generator = test_generator
    await test_generator.start()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as test_client:
        test_client.generator = test_generator
        yield test_client
    await test_generator.stop()


@pytest.mark.anyio
async def test_health_endpoint(client):
    response = await client.get("/api/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.anyio
async def test_simulation_config_read_and_update(client):
    response = await client.post("/api/simulation/config", json={"interval_ms": 250, "seed": 7})
    assert response.status_code == 200
    body = response.json()
    assert body["interval_ms"] == 250
    assert body["seed"] == 7
    assert body["message_id"] == 1

    response = await client.get("/api/simulation/config")
    assert response.status_code == 200
    body = response.json()
    assert body["interval_ms"] == 250
    assert body["running"] is True


@pytest.mark.anyio
async def test_simulation_config_rejects_invalid_interval(client):
    response = await client.post("/api/simulation/config", json={"interval_ms": 10})

    assert response.status_code == 422


@pytest.mark.anyio
async def test_polling_latest_returns_standard_payload(client):
    await client.post("/api/simulation/config", json={"interval_ms": 1000, "seed": 1})
    response = await client.get("/api/polling/latest")

    assert response.status_code == 200
    body = response.json()
    assert body["strategy"] == "polling"
    assert "served_at" in body
    assert "server_processing_ms" in body
    assert body["data"]["message_id"] == 1
    assert body["data"]["robot_id"] == "robot-1"


@pytest.mark.anyio
async def test_long_polling_latest_returns_current_data_without_last_message_id(client):
    await client.post("/api/simulation/config", json={"interval_ms": 1000, "seed": 1})
    response = await client.get("/api/long-polling/latest")

    assert response.status_code == 200
    body = response.json()
    assert body["strategy"] == "long_polling"
    assert "served_at" in body
    assert "server_processing_ms" in body
    assert body["data"]["message_id"] == 1


@pytest.mark.anyio
async def test_long_polling_latest_returns_204_when_current_until_timeout(client):
    await client.post("/api/simulation/config", json={"interval_ms": 10000, "seed": 1})
    response = await client.get("/api/long-polling/latest?last_message_id=1&timeout_ms=1000")

    assert response.status_code == 204


@pytest.mark.anyio
async def test_long_polling_latest_returns_future_message_before_timeout(client):
    await client.post("/api/simulation/config", json={"interval_ms": 1000, "seed": 1})

    request_task = asyncio.create_task(
        client.get("/api/long-polling/latest?last_message_id=1&timeout_ms=5000")
    )
    await asyncio.sleep(0)
    await client.generator.generate_once()
    response = await request_task

    assert response.status_code == 200
    assert response.json()["data"]["message_id"] == 2


@pytest.mark.anyio
async def test_long_polling_latest_rejects_invalid_timeout(client):
    response = await client.get("/api/long-polling/latest?timeout_ms=999")

    assert response.status_code == 422


@pytest.mark.anyio
async def test_sse_latest_stream_returns_first_event_with_current_data(client):
    await client.post("/api/simulation/config", json={"interval_ms": 1000, "seed": 1})
    request = FakeSseRequest(disconnect_after_checks=2)

    stream = event_stream(request, client.generator, last_message_id=None)
    event = await anext(stream)
    await stream.aclose()

    parsed = parse_sse_event(event)
    assert parsed["event"] == "telemetry"
    assert parsed["id"] == "1"

    body = json.loads(parsed["data"])
    assert body["strategy"] == "sse"
    assert "served_at" in body
    assert "server_processing_ms" in body
    assert body["data"]["message_id"] == 1


@pytest.mark.anyio
async def test_sse_latest_stream_returns_future_message_after_last_event_id(client):
    await client.post("/api/simulation/config", json={"interval_ms": 1000, "seed": 1})
    request = FakeSseRequest(disconnect_after_checks=3)

    stream = event_stream(request, client.generator, last_message_id=1)
    event_task = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)
    await client.generator.generate_once()
    event = await event_task
    await stream.aclose()

    parsed = parse_sse_event(event)
    body = json.loads(parsed["data"])
    assert parsed["id"] == "2"
    assert body["data"]["message_id"] == 2


class FakeSseRequest:
    def __init__(self, disconnect_after_checks: int):
        self.disconnect_after_checks = disconnect_after_checks
        self.checks = 0

    async def is_disconnected(self):
        self.checks += 1
        return self.checks > self.disconnect_after_checks


def parse_sse_event(raw_event: str) -> dict:
    parsed = {"event": None, "id": None, "data": ""}
    data_lines = []
    for line in raw_event.strip().splitlines():
        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if field == "event":
            parsed["event"] = value
        elif field == "id":
            parsed["id"] = value
        elif field == "data":
            data_lines.append(value)
    parsed["data"] = "\n".join(data_lines)
    return parsed
