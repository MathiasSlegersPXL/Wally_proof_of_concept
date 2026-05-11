import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from app.data_generator import RobotDataGenerator
from app.main import app


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
