import argparse
import csv
import json
import queue
import statistics
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.strategies.sse_utils import parse_sse_frame

DEFAULT_MQTT_HOST = "127.0.0.1"
DEFAULT_MQTT_PORT = 1883
DEFAULT_MQTT_TOPIC = "wally/robot-1/telemetry"
DEFAULT_MQTT_QOS = 0
DEFAULT_GRPC_HOST = "127.0.0.1"
DEFAULT_GRPC_PORT = 50051


CSV_FIELDS = [
    "strategy",
    "generation_interval_ms",
    "poll_interval_ms",
    "long_poll_timeout_ms",
    "request_started_at",
    "response_received_at",
    "request_latency_ms",
    "data_age_ms",
    "message_id",
    "duplicate",
    "missed_messages",
    "http_status",
    "response_bytes",
]


def main():
    args = parse_args()
    base_url = args.base_url.rstrip("/")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.strategy != "mqtt":
        configure_generator(
            base_url=base_url,
            interval_ms=args.generation_interval_ms,
            seed=args.seed,
        )

    rows = run_benchmark(base_url=base_url, args=args)
    summary = summarize(rows, args)

    label = result_label(args)
    csv_path = output_dir / f"{label}.csv"
    json_path = output_dir / f"{label}.json"

    with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    with json_path.open("w", encoding="utf-8") as json_file:
        json.dump(summary, json_file, indent=2)

    print(f"Wrote {csv_path}")
    print(f"Wrote {json_path}")


def parse_args():
    parser = argparse.ArgumentParser(description="Run an update-strategy benchmark against the Wally PoC API.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--strategy", choices=["polling", "long_polling", "sse", "mqtt", "grpc"], default="polling")
    parser.add_argument("--duration-seconds", type=float, default=60)
    parser.add_argument("--generation-interval-ms", type=int, required=True)
    parser.add_argument("--poll-interval-ms", type=int, default=1000)
    parser.add_argument("--long-poll-timeout-ms", type=int, default=30_000)
    parser.add_argument("--mqtt-host", default=DEFAULT_MQTT_HOST)
    parser.add_argument("--mqtt-port", type=int, default=DEFAULT_MQTT_PORT)
    parser.add_argument("--mqtt-topic", default=DEFAULT_MQTT_TOPIC)
    parser.add_argument("--mqtt-qos", type=int, default=DEFAULT_MQTT_QOS)
    parser.add_argument("--grpc-host", default=DEFAULT_GRPC_HOST)
    parser.add_argument("--grpc-port", type=int, default=DEFAULT_GRPC_PORT)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--output-dir", default="results")
    return parser.parse_args()


def configure_generator(base_url: str, interval_ms: int, seed: int | None):
    body = json.dumps({"interval_ms": interval_ms, "seed": seed}).encode("utf-8")
    request = Request(
        f"{base_url}/api/simulation/config",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        response.read()


def run_benchmark(base_url: str, args: argparse.Namespace) -> list[dict]:
    if args.strategy == "mqtt":
        return run_mqtt_benchmark(base_url=base_url, args=args)
    if args.strategy == "grpc":
        return run_grpc_benchmark(args=args)
    if args.strategy == "sse":
        return run_sse_benchmark(base_url=base_url, args=args)
    if args.strategy == "long_polling":
        return run_long_polling_benchmark(base_url=base_url, args=args)

    return run_polling_benchmark(base_url=base_url, args=args)


def run_polling_benchmark(base_url: str, args: argparse.Namespace) -> list[dict]:
    rows = []
    last_message_id = None
    end_time = time.monotonic() + args.duration_seconds
    poll_interval_seconds = args.poll_interval_ms / 1000
    next_poll = time.monotonic()

    while time.monotonic() < end_time:
        now = time.monotonic()
        if now < next_poll:
            time.sleep(next_poll - now)

        row, last_message_id = request_once(
            url=f"{base_url}/api/polling/latest",
            strategy=args.strategy,
            generation_interval_ms=args.generation_interval_ms,
            poll_interval_ms=args.poll_interval_ms,
            long_poll_timeout_ms="",
            last_message_id=last_message_id,
            request_timeout_seconds=10,
        )
        rows.append(row)
        next_poll += poll_interval_seconds

    return rows


def run_long_polling_benchmark(base_url: str, args: argparse.Namespace) -> list[dict]:
    rows = []
    last_message_id = None
    end_time = time.monotonic() + args.duration_seconds

    while time.monotonic() < end_time:
        params = {"timeout_ms": args.long_poll_timeout_ms}
        if last_message_id is not None:
            params["last_message_id"] = last_message_id

        url = f"{base_url}/api/long-polling/latest?{urlencode(params)}"
        row, last_message_id = request_once(
            url=url,
            strategy=args.strategy,
            generation_interval_ms=args.generation_interval_ms,
            poll_interval_ms="",
            long_poll_timeout_ms=args.long_poll_timeout_ms,
            last_message_id=last_message_id,
            request_timeout_seconds=(args.long_poll_timeout_ms / 1000) + 5,
        )
        rows.append(row)

    return rows


def run_grpc_benchmark(args: argparse.Namespace) -> list[dict]:
    import grpc

    from app.grpc_generated import telemetry_pb2, telemetry_pb2_grpc

    rows = []
    last_message_id = None
    stream_started_at = int(time.time() * 1000)
    end_time = time.monotonic() + args.duration_seconds
    target = f"{args.grpc_host}:{args.grpc_port}"

    try:
        with grpc.insecure_channel(target) as channel:
            stub = telemetry_pb2_grpc.TelemetryServiceStub(channel)
            request = telemetry_pb2.SubscribeRequest(last_message_id=0)
            stream = stub.StreamTelemetry(request, timeout=args.duration_seconds + 10)

            for message in stream:
                if time.monotonic() >= end_time:
                    break

                row, last_message_id = grpc_message_to_row(
                    message=message,
                    generation_interval_ms=args.generation_interval_ms,
                    last_message_id=last_message_id,
                    stream_started_at=stream_started_at,
                    received_at=int(time.time() * 1000),
                )
                rows.append(row)
    except grpc.RpcError:
        rows.append(
            {
                "strategy": "grpc",
                "generation_interval_ms": args.generation_interval_ms,
                "poll_interval_ms": "",
                "long_poll_timeout_ms": "",
                "request_started_at": stream_started_at,
                "response_received_at": int(time.time() * 1000),
                "request_latency_ms": -1,
                "data_age_ms": -1,
                "message_id": None,
                "duplicate": False,
                "missed_messages": 0,
                "http_status": 0,
                "response_bytes": 0,
            }
        )

    return rows


def grpc_message_to_row(
    message,
    generation_interval_ms: int,
    last_message_id: int | None,
    stream_started_at: int,
    received_at: int,
) -> tuple[dict, int | None]:
    message_id = message.data.message_id
    duplicate = False
    missed_messages = 0

    if last_message_id is not None:
        if message_id == last_message_id:
            duplicate = True
        elif message_id > last_message_id + 1:
            missed_messages = message_id - last_message_id - 1
    last_message_id = message_id

    row = {
        "strategy": "grpc",
        "generation_interval_ms": generation_interval_ms,
        "poll_interval_ms": "",
        "long_poll_timeout_ms": "",
        "request_started_at": stream_started_at,
        "response_received_at": received_at,
        "request_latency_ms": -1,
        "data_age_ms": received_at - message.data.created_at,
        "message_id": message_id,
        "duplicate": duplicate,
        "missed_messages": missed_messages,
        "http_status": 200,
        "response_bytes": message.ByteSize(),
    }
    return row, last_message_id


def run_mqtt_benchmark(base_url: str, args: argparse.Namespace) -> list[dict]:
    import paho.mqtt.client as mqtt

    rows = []
    messages = queue.Queue()
    last_message_id = None
    stream_started_at = int(time.time() * 1000)
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    def on_message(client, userdata, message):
        messages.put((int(time.time() * 1000), message.payload))

    client.on_message = on_message

    try:
        client.connect(args.mqtt_host, args.mqtt_port)
        client.subscribe(args.mqtt_topic, qos=args.mqtt_qos)
        client.loop_start()
        time.sleep(0.2)

        configure_generator(
            base_url=base_url,
            interval_ms=args.generation_interval_ms,
            seed=args.seed,
        )

        end_time = time.monotonic() + args.duration_seconds
        while time.monotonic() < end_time:
            timeout = min(0.2, max(0, end_time - time.monotonic()))
            try:
                received_at, payload = messages.get(timeout=timeout)
            except queue.Empty:
                continue

            row, last_message_id = mqtt_message_to_row(
                payload=payload,
                generation_interval_ms=args.generation_interval_ms,
                last_message_id=last_message_id,
                stream_started_at=stream_started_at,
                received_at=received_at,
            )
            rows.append(row)
    except (HTTPError, OSError, URLError, ValueError):
        rows.append(
            {
                "strategy": "mqtt",
                "generation_interval_ms": args.generation_interval_ms,
                "poll_interval_ms": "",
                "long_poll_timeout_ms": "",
                "request_started_at": stream_started_at,
                "response_received_at": int(time.time() * 1000),
                "request_latency_ms": -1,
                "data_age_ms": -1,
                "message_id": None,
                "duplicate": False,
                "missed_messages": 0,
                "http_status": 0,
                "response_bytes": 0,
            }
        )
    finally:
        client.loop_stop()
        client.disconnect()

    return rows


def mqtt_message_to_row(
    payload: bytes,
    generation_interval_ms: int,
    last_message_id: int | None,
    stream_started_at: int,
    received_at: int,
) -> tuple[dict, int | None]:
    body = json.loads(payload.decode("utf-8"))
    data = body["data"]
    message_id = data["message_id"]
    duplicate = False
    missed_messages = 0

    if last_message_id is not None:
        if message_id == last_message_id:
            duplicate = True
        elif message_id > last_message_id + 1:
            missed_messages = message_id - last_message_id - 1
    last_message_id = message_id

    row = {
        "strategy": "mqtt",
        "generation_interval_ms": generation_interval_ms,
        "poll_interval_ms": "",
        "long_poll_timeout_ms": "",
        "request_started_at": stream_started_at,
        "response_received_at": received_at,
        "request_latency_ms": -1,
        "data_age_ms": received_at - data["created_at"],
        "message_id": message_id,
        "duplicate": duplicate,
        "missed_messages": missed_messages,
        "http_status": 200,
        "response_bytes": len(payload),
    }
    return row, last_message_id


def run_sse_benchmark(base_url: str, args: argparse.Namespace) -> list[dict]:
    rows = []
    last_message_id = None
    stream_started_at = int(time.time() * 1000)
    end_time = time.monotonic() + args.duration_seconds
    request = Request(
        f"{base_url}/api/sse/latest",
        headers={"Accept": "text/event-stream"},
        method="GET",
    )

    try:
        with urlopen(request, timeout=args.duration_seconds + 10) as response:
            frame_lines = []
            frame_bytes = 0
            while time.monotonic() < end_time:
                raw_line = response.readline()
                if raw_line == b"":
                    break

                frame_bytes += len(raw_line)
                line = raw_line.decode("utf-8").rstrip("\r\n")
                if line == "":
                    event = parse_sse_frame(frame_lines)
                    if event and event["event"] == "telemetry":
                        row, last_message_id = sse_event_to_row(
                            event=event,
                            strategy=args.strategy,
                            generation_interval_ms=args.generation_interval_ms,
                            last_message_id=last_message_id,
                            response_bytes=frame_bytes,
                            stream_started_at=stream_started_at,
                        )
                        rows.append(row)
                    frame_lines = []
                    frame_bytes = 0
                else:
                    frame_lines.append(line)
    except (HTTPError, URLError):
        rows.append(
            {
                "strategy": args.strategy,
                "generation_interval_ms": args.generation_interval_ms,
                "poll_interval_ms": "",
                "long_poll_timeout_ms": "",
                "request_started_at": stream_started_at,
                "response_received_at": int(time.time() * 1000),
                "request_latency_ms": -1,
                "data_age_ms": -1,
                "message_id": None,
                "duplicate": False,
                "missed_messages": 0,
                "http_status": 0,
                "response_bytes": 0,
            }
        )

    return rows


def sse_event_to_row(
    event: dict,
    strategy: str,
    generation_interval_ms: int,
    last_message_id: int | None,
    response_bytes: int,
    stream_started_at: int,
) -> tuple[dict, int | None]:
    received_at = int(time.time() * 1000)
    body = json.loads(event["data"])
    data = body["data"]
    message_id = data["message_id"]
    duplicate = False
    missed_messages = 0

    if last_message_id is not None:
        if message_id == last_message_id:
            duplicate = True
        elif message_id > last_message_id + 1:
            missed_messages = message_id - last_message_id - 1
    last_message_id = message_id

    row = {
        "strategy": strategy,
        "generation_interval_ms": generation_interval_ms,
        "poll_interval_ms": "",
        "long_poll_timeout_ms": "",
        "request_started_at": stream_started_at,
        "response_received_at": received_at,
        "request_latency_ms": -1,
        "data_age_ms": received_at - data["created_at"],
        "message_id": message_id,
        "duplicate": duplicate,
        "missed_messages": missed_messages,
        "http_status": 200,
        "response_bytes": response_bytes,
    }
    return row, last_message_id


def request_once(
    url: str,
    strategy: str,
    generation_interval_ms: int,
    poll_interval_ms: int | str,
    long_poll_timeout_ms: int | str,
    last_message_id: int | None,
    request_timeout_seconds: float,
) -> tuple[dict, int | None]:
    started_at = int(time.time() * 1000)
    http_status = 0
    message_id = None
    duplicate = False
    missed_messages = 0
    response_bytes = 0
    data_age_ms = -1

    try:
        with urlopen(url, timeout=request_timeout_seconds) as response:
            received_at = int(time.time() * 1000)
            raw_body = response.read()
            response_bytes = len(raw_body)
            http_status = response.status

        if http_status == 200:
            body = json.loads(raw_body.decode("utf-8"))
            data = body["data"]
            message_id = data["message_id"]
            data_age_ms = received_at - data["created_at"]

            if last_message_id is not None:
                if message_id == last_message_id:
                    duplicate = True
                elif message_id > last_message_id + 1:
                    missed_messages = message_id - last_message_id - 1
            last_message_id = message_id
    except HTTPError as exc:
        received_at = int(time.time() * 1000)
        http_status = exc.code
        response_bytes = len(exc.read())
    except URLError:
        received_at = int(time.time() * 1000)

    row = {
        "strategy": strategy,
        "generation_interval_ms": generation_interval_ms,
        "poll_interval_ms": poll_interval_ms,
        "long_poll_timeout_ms": long_poll_timeout_ms,
        "request_started_at": started_at,
        "response_received_at": received_at,
        "request_latency_ms": received_at - started_at if http_status else -1,
        "data_age_ms": data_age_ms,
        "message_id": message_id,
        "duplicate": duplicate,
        "missed_messages": missed_messages,
        "http_status": http_status,
        "response_bytes": response_bytes,
    }
    return row, last_message_id


def summarize(rows: list[dict], args: argparse.Namespace) -> dict:
    success_rows = [row for row in rows if row["http_status"] == 200]
    latencies = [row["request_latency_ms"] for row in success_rows if row["request_latency_ms"] >= 0]
    data_ages = [row["data_age_ms"] for row in success_rows if row["data_age_ms"] >= 0]
    duplicate_count = sum(1 for row in rows if row["duplicate"])
    missed_count = sum(row["missed_messages"] for row in rows)
    empty_count = sum(1 for row in rows if row["http_status"] == 204)
    error_count = sum(1 for row in rows if row["http_status"] not in (200, 204))

    return {
        "strategy": args.strategy,
        "duration_seconds": args.duration_seconds,
        "generation_interval_ms": args.generation_interval_ms,
        "poll_interval_ms": args.poll_interval_ms if args.strategy == "polling" else None,
        "long_poll_timeout_ms": args.long_poll_timeout_ms if args.strategy == "long_polling" else None,
        "mqtt_topic": args.mqtt_topic if args.strategy == "mqtt" else None,
        "grpc_target": f"{args.grpc_host}:{args.grpc_port}" if args.strategy == "grpc" else None,
        "seed": args.seed,
        "requests_sent": len(rows),
        "successful_responses": len(success_rows),
        "empty_responses": empty_count,
        "timeouts": empty_count,
        "errors": error_count,
        "duplicates": duplicate_count,
        "duplicate_rate": round(duplicate_count / len(rows), 4) if rows else 0,
        "missed_messages": missed_count,
        "downloaded_bytes": sum(row["response_bytes"] for row in rows),
        "request_latency_ms": stats(latencies),
        "data_age_ms": stats(data_ages),
    }


def result_label(args: argparse.Namespace) -> str:
    timestamp = int(time.time())
    if args.strategy == "grpc":
        return f"grpc_gen{args.generation_interval_ms}_{timestamp}"
    if args.strategy == "mqtt":
        return f"mqtt_gen{args.generation_interval_ms}_{timestamp}"
    if args.strategy == "sse":
        return f"sse_gen{args.generation_interval_ms}_{timestamp}"
    if args.strategy == "long_polling":
        return f"long_polling_gen{args.generation_interval_ms}_timeout{args.long_poll_timeout_ms}_{timestamp}"
    return f"polling_gen{args.generation_interval_ms}_poll{args.poll_interval_ms}_{timestamp}"


def stats(values: list[int]) -> dict:
    if not values:
        return {"avg": None, "min": None, "max": None}
    return {
        "avg": round(statistics.fmean(values), 2),
        "min": min(values),
        "max": max(values),
    }


if __name__ == "__main__":
    main()
