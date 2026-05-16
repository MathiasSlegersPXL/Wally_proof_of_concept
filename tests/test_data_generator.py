import asyncio

import pytest

from app.data_generator import RobotDataGenerator


def test_start_is_idempotent():
    async def run():
        generator = RobotDataGenerator(interval_ms=1000, seed=1)
        await generator.start()
        first_task = generator._task
        await generator.start()
        assert generator._task is first_task
        await generator.stop()

    asyncio.run(run())


def test_generates_increasing_message_ids():
    generator = RobotDataGenerator(interval_ms=1000, seed=1)
    generator._generate()
    first = generator.latest
    generator._generate()
    second = generator.latest

    assert first is not None
    assert second is not None
    assert second.message_id == first.message_id + 1


def test_configure_resets_message_ids_and_latest_data():
    async def run():
        generator = RobotDataGenerator(interval_ms=1000, seed=1)
        generator._generate()
        generator._generate()

        await generator.configure(interval_ms=250, seed=2)

        assert generator.interval_ms == 250
        assert generator.seed == 2
        assert generator.latest is not None
        assert generator.latest.message_id == 1

    asyncio.run(run())


def test_wait_for_next_returns_existing_newer_message_immediately():
    async def run():
        generator = RobotDataGenerator(interval_ms=1000, seed=1)
        await generator.configure(interval_ms=1000, seed=1)

        data = await generator.wait_for_next(last_message_id=0, timeout_ms=1000)

        assert data is not None
        assert data.message_id == 1

    asyncio.run(run())


def test_wait_for_next_times_out_without_new_message():
    async def run():
        generator = RobotDataGenerator(interval_ms=1000, seed=1)
        await generator.configure(interval_ms=1000, seed=1)

        data = await generator.wait_for_next(last_message_id=1, timeout_ms=100)

        assert data is None

    asyncio.run(run())


def test_wait_for_next_receives_future_message():
    async def run():
        generator = RobotDataGenerator(interval_ms=1000, seed=1)
        await generator.configure(interval_ms=1000, seed=1)

        waiter = asyncio.create_task(generator.wait_for_next(last_message_id=1, timeout_ms=1000))
        await asyncio.sleep(0)
        await generator.generate_once()
        data = await waiter

        assert data is not None
        assert data.message_id == 2

    asyncio.run(run())


def test_configure_wakes_pending_waiters():
    async def run():
        generator = RobotDataGenerator(interval_ms=1000, seed=1)
        await generator.configure(interval_ms=1000, seed=1)

        waiter = asyncio.create_task(generator.wait_for_next(last_message_id=1, timeout_ms=1000))
        await asyncio.sleep(0)
        await generator.configure(interval_ms=500, seed=2)
        data = await waiter

        assert data is not None
        assert data.message_id == 1

    asyncio.run(run())


def test_interval_validation():
    with pytest.raises(ValueError):
        RobotDataGenerator(interval_ms=49)

    async def run():
        generator = RobotDataGenerator(interval_ms=1000)
        with pytest.raises(ValueError):
            await generator.configure(interval_ms=10_001)

    asyncio.run(run())
