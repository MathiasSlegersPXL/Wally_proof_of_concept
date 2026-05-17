import asyncio
import json
import os
import time
from dataclasses import dataclass

from app.data_generator import RobotData, RobotDataGenerator

DEFAULT_MQTT_HOST = "127.0.0.1"
DEFAULT_MQTT_PORT = 1883
DEFAULT_MQTT_TOPIC = "wally/robot-1/telemetry"
DEFAULT_MQTT_QOS = 0
MQTT_WAIT_TIMEOUT_MS = 1_000


@dataclass(frozen=True)
class MqttConfig:
    enabled: bool = False
    host: str = DEFAULT_MQTT_HOST
    port: int = DEFAULT_MQTT_PORT
    topic: str = DEFAULT_MQTT_TOPIC
    qos: int = DEFAULT_MQTT_QOS


def mqtt_config_from_env() -> MqttConfig:
    return MqttConfig(
        enabled=os.getenv("MQTT_ENABLED") == "1",
        host=os.getenv("MQTT_HOST", DEFAULT_MQTT_HOST),
        port=int(os.getenv("MQTT_PORT", str(DEFAULT_MQTT_PORT))),
        topic=os.getenv("MQTT_TOPIC", DEFAULT_MQTT_TOPIC),
        qos=int(os.getenv("MQTT_QOS", str(DEFAULT_MQTT_QOS))),
    )


def build_mqtt_payload(data: RobotData) -> dict:
    return {
        "strategy": "mqtt",
        "served_at": int(time.time() * 1000),
        "data": data.to_dict(),
    }


class MqttPublisherService:
    def __init__(self, generator: RobotDataGenerator, config: MqttConfig):
        self.generator = generator
        self.config = config
        self._client = None
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self):
        if not self.config.enabled:
            return
        if self._task and not self._task.done():
            return

        self._client = self._create_client()
        try:
            await asyncio.to_thread(self._client.connect, self.config.host, self.config.port)
            self._client.loop_start()
        except OSError as exc:
            self._client = None
            raise RuntimeError(
                "MQTT is enabled, but the broker is unavailable at "
                f"{self.config.host}:{self.config.port}. Start Mosquitto there, "
                "or unset MQTT_ENABLED to run without MQTT."
            ) from exc
        self._running = True
        self._task = asyncio.create_task(self._publish_loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

        if self._client:
            self._client.loop_stop()
            await asyncio.to_thread(self._client.disconnect)
        self._client = None

    async def _publish_loop(self):
        last_message_id = None
        while self._running:
            data = await self.generator.wait_for_next(
                last_message_id=last_message_id,
                timeout_ms=MQTT_WAIT_TIMEOUT_MS,
            )
            if data is None:
                continue

            payload = json.dumps(build_mqtt_payload(data), separators=(",", ":"))
            await self._publish(payload)
            last_message_id = data.message_id

    async def _publish(self, payload: str):
        if self._client is None:
            return

        publish_info = self._client.publish(
            self.config.topic,
            payload=payload,
            qos=self.config.qos,
            retain=False,
        )
        await asyncio.to_thread(publish_info.wait_for_publish, timeout=5)

    def _create_client(self):
        import paho.mqtt.client as mqtt

        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
