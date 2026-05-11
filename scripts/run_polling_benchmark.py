import argparse
import csv
import json
import statistics
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


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
    parser.add_argument("--strategy", choices=["polling", "long_polling"], default="polling")
    parser.add_argument("--duration-seconds", type=float, default=60)
    parser.add_argument("--generation-interval-ms", type=int, required=True)
    parser.add_argument("--poll-interval-ms", type=int, default=1000)
    parser.add_argument("--long-poll-timeout-ms", type=int, default=30_000)
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
    latencies = [row["request_latency_ms"] for row in success_rows]
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
