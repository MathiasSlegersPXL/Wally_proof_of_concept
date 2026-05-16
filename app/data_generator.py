import asyncio
import random
import time
from dataclasses import asdict, dataclass


@dataclass
class RobotData:
    message_id: int
    robot_id: str
    server_timestamp: str
    status: str
    bricks_placed: int
    bricks_per_minute: float
    error_code: str | None
    glue_quality: float
    created_at: int  # unix ms

    def to_dict(self) -> dict:
        return asdict(self)


class RobotDataGenerator:
    """Generates simulated robot telemetry at a configurable interval."""

    MIN_INTERVAL_MS = 50
    MAX_INTERVAL_MS = 10_000

    def __init__(self, interval_ms: int = 1000, robot_id: str = "robot-1", seed: int | None = 1):
        self.interval_ms = interval_ms
        self.robot_id = robot_id
        self.seed = seed
        self._message_id = 0
        self._bricks_placed = 0
        self._latest: RobotData | None = None
        self._task: asyncio.Task | None = None
        self._running = False
        self._random = random.Random(seed)
        self._condition = asyncio.Condition()
        self._generation_version = 0

        self._validate_interval(interval_ms)

    @property
    def latest(self) -> RobotData | None:
        return self._latest

    @property
    def running(self) -> bool:
        return self._running

    @property
    def message_id(self) -> int:
        return self._message_id

    async def start(self):
        if self._task and not self._task.done():
            return
        self._running = True
        if self._latest is None:
            await self.generate_once()
        self._task = asyncio.create_task(self._generate_loop())

    async def stop(self):
        self._running = False
        async with self._condition:
            self._condition.notify_all()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def configure(self, interval_ms: int, seed: int | None = None):
        self._validate_interval(interval_ms)
        async with self._condition:
            self.interval_ms = interval_ms
            self.seed = seed
            self._random = random.Random(seed)
            self._message_id = 0
            self._bricks_placed = 0
            self._latest = None
            self._generation_version += 1
            self._generate()
            self._condition.notify_all()

    async def snapshot(self) -> RobotData | None:
        async with self._condition:
            return self._latest

    async def wait_for_next(self, last_message_id: int | None, timeout_ms: int) -> RobotData | None:
        timeout_seconds = timeout_ms / 1000

        async with self._condition:
            generation_version = self._generation_version
            if self._has_newer_message(last_message_id):
                return self._latest

            try:
                await asyncio.wait_for(
                    self._condition.wait_for(
                        lambda: self._has_newer_message(last_message_id)
                        or self._has_reset_since(generation_version)
                    ),
                    timeout=timeout_seconds,
                )
            except asyncio.TimeoutError:
                return None

            return self._latest

    async def generate_once(self) -> RobotData:
        async with self._condition:
            self._generate()
            self._condition.notify_all()
            return self._latest

    def config(self) -> dict:
        return {
            "interval_ms": self.interval_ms,
            "robot_id": self.robot_id,
            "message_id": self._message_id,
            "running": self._running,
            "seed": self.seed,
        }

    async def _generate_loop(self):
        while self._running:
            generation_version = self._generation_version
            interval_seconds = self.interval_ms / 1000
            try:
                async with self._condition:
                    await asyncio.wait_for(
                        self._condition.wait_for(
                            lambda: not self._running
                            or self._generation_version != generation_version
                        ),
                        timeout=interval_seconds,
                    )
                continue
            except asyncio.TimeoutError:
                pass

            await self.generate_once()

    def _generate(self):
        self._message_id += 1
        self._bricks_placed += self._random.randint(0, 3)

        now_ms = int(time.time() * 1000)
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%S.", time.gmtime()) + f"{now_ms % 1000:03d}Z"

        statuses = ["running"] * 8 + ["idle", "error"]
        status = self._random.choice(statuses)

        error_code = None
        if status == "error":
            error_code = self._random.choice(["E_STUCK", "E_GLUE_LOW", "E_SENSOR_FAULT", "E_OVERHEAT"])

        self._latest = RobotData(
            message_id=self._message_id,
            robot_id=self.robot_id,
            server_timestamp=now_iso,
            status=status,
            bricks_placed=self._bricks_placed,
            bricks_per_minute=round(self._random.uniform(12.0, 16.0), 1),
            error_code=error_code,
            glue_quality=round(self._random.uniform(0.85, 1.0), 2),
            created_at=now_ms,
        )

    def _validate_interval(self, interval_ms: int):
        if not self.MIN_INTERVAL_MS <= interval_ms <= self.MAX_INTERVAL_MS:
            raise ValueError(
                f"interval_ms must be between {self.MIN_INTERVAL_MS} and {self.MAX_INTERVAL_MS}"
            )

    def _has_newer_message(self, last_message_id: int | None) -> bool:
        return self._latest is not None and (
            last_message_id is None or self._latest.message_id > last_message_id
        )

    def _has_reset_since(self, generation_version: int) -> bool:
        return self._latest is not None and self._generation_version != generation_version
