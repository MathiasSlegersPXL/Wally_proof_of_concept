# Wally Proof of Concept

HTTP polling and long-polling baseline for comparing real-time dashboard update strategies against simulated robot telemetry.

Part of the Wally Automation research track — the goal is to quantitatively compare polling, long polling, SSE, WebSocket, and MQTT for operational robot monitoring. This repository currently implements the HTTP-based polling strategies first.

## Architecture

```
RobotDataGenerator ──(latest state / notifications)──> FastAPI ──(HTTP GET)──> Browser or CLI
   (background task)                               polling: immediate response
                                                   long polling: wait for new data
```

- **Data Generator** (`app/data_generator.py`) — runs a background asyncio task that produces robot telemetry at a configurable interval. Stores only the latest state in memory. Deterministic when seeded.
- **FastAPI** (`app/main.py`, `app/strategies/`) — exposes short polling and long polling endpoints and lets the frontend or CLI reconfigure the generator at runtime.
- **Frontend** (`app/static/`) — vanilla JS dashboard for interactive testing: choose strategy, adjust intervals, view live data and metrics, export CSV.
- **Benchmark runner** (`scripts/run_polling_benchmark.py`) — headless CLI tool that runs a timed polling session and writes raw CSV + summary JSON to `results/`.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

## Run the App

```bash
uvicorn app.main:app --reload
```

Open `http://127.0.0.1:8000`.

The dashboard controls let you set:

- **Strategy** — short polling or long polling
- **Data generation interval** — how often the simulated robot produces a new measurement (50–10000 ms)
- **Polling interval** — how often the browser fetches the latest state (100–2000 ms)
- **Long-poll timeout** — how long the server may hold a request while waiting for newer data
- **Seed** — deterministic starting point for the pseudo-random generator; same seed = same data sequence across runs

Pressing **Start Polling** resets the generator and begins a new measurement session. Press **Stop Polling** ends it. **Export CSV** downloads all per-request measurements.

## API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/health` | Liveness check |
| `GET` | `/api/simulation/config` | Read current generator config |
| `POST` | `/api/simulation/config` | Update generator interval and seed |
| `GET` | `/api/polling/latest` | Latest robot state (204 if no data yet) |
| `GET` | `/api/long-polling/latest` | Latest newer robot state, waiting until data or timeout |

### POST /api/simulation/config

```json
{
  "interval_ms": 500,
  "seed": 1
}
```

Allowed interval: `50`–`10000` ms.

### GET /api/polling/latest

```json
{
  "strategy": "polling",
  "served_at": 1778502006821,
  "server_processing_ms": 0.01,
  "data": {
    "message_id": 42,
    "robot_id": "robot-1",
    "server_timestamp": "2026-05-11T12:20:06.454Z",
    "status": "running",
    "bricks_placed": 128,
    "bricks_per_minute": 14.6,
    "error_code": null,
    "glue_quality": 0.97,
    "created_at": 1778502006454
  }
}
```

### GET /api/long-polling/latest

Query parameters:

- `last_message_id` — optional latest message already known by the client
- `timeout_ms` — optional timeout, default `30000`, allowed `1000`-`60000`

If the server already has newer data than `last_message_id`, it responds immediately with `200`. If not, it holds the request until a new message is generated or the timeout expires. Timeout returns `204`.

## Metrics

| Metric | Definition |
|--------|------------|
| **data age** | Time from telemetry creation (`created_at`) to response receipt in the client. The key metric for strategy comparison — captures inherent polling delay. |
| **request latency** | Full HTTP round-trip: `fetch()` start to response arrival. Measures network + server overhead, not strategy delay. |
| **duplicate** | Same `message_id` received more than once. Occurs when polling faster than data generation. |
| **missed messages** | Gaps in the monotonic `message_id` sequence. Occurs when polling slower than data generation. |
| **downloaded bytes** | Approximate response body size. Excludes HTTP headers and TCP overhead. |
| **timeout** | Long-poll request that reached `timeout_ms` without newer data. Recorded as HTTP `204`. |

**Why data age matters more than request latency:** polling at 1000 ms has ~500 ms average data age regardless of how fast each HTTP request is. SSE/WebSocket push data as soon as it is generated, so data age drops to single-digit milliseconds. Data age is what makes strategies meaningfully different.

## Benchmark Runner

Start the API server first, then run the headless benchmark:

```bash
python scripts/run_polling_benchmark.py \
  --strategy polling \
  --generation-interval-ms 1000 \
  --poll-interval-ms 500 \
  --duration-seconds 60
```

| Flag | Default | Description |
|------|---------|-------------|
| `--base-url` | `http://127.0.0.1:8000` | API server location |
| `--strategy` | `polling` | `polling` or `long_polling` |
| `--generation-interval-ms` | (required) | Generator tick interval |
| `--poll-interval-ms` | `1000` | Client poll interval for `polling` |
| `--long-poll-timeout-ms` | `30000` | Server wait timeout for `long_polling` |
| `--duration-seconds` | `60` | How long to run |
| `--seed` | `1` | Deterministic seed |
| `--output-dir` | `results` | Where to write CSV and JSON |

Output examples:

- `results/polling_gen{gen}_poll{poll}_{timestamp}.csv`
- `results/long_polling_gen{gen}_timeout{timeout}_{timestamp}.csv`

Each CSV has raw request rows. Each JSON has summary averages, rates, totals, and timeout counts.

### Recommended scenarios

```bash
# Scenario 1 — Baseline: poll interval = generation interval
python scripts/run_polling_benchmark.py --generation-interval-ms 1000 --poll-interval-ms 1000 --duration-seconds 60

# Scenario 2 — Polling faster than data: expect ~50% duplicates
python scripts/run_polling_benchmark.py --generation-interval-ms 1000 --poll-interval-ms 500 --duration-seconds 60

# Scenario 3 — Polling far faster: high duplicates, high byte overhead
python scripts/run_polling_benchmark.py --generation-interval-ms 1000 --poll-interval-ms 100 --duration-seconds 60

# Scenario 4 — Polling slower than data: expect missed messages
python scripts/run_polling_benchmark.py --generation-interval-ms 250 --poll-interval-ms 1000 --duration-seconds 60

# Scenario 5 — Long polling: expect low duplicate count and fewer requests
python scripts/run_polling_benchmark.py --strategy long_polling --generation-interval-ms 1000 --long-poll-timeout-ms 30000 --duration-seconds 60
```

## Project Structure

```
app/
├── main.py                  # FastAPI app, CORS, lifespan, static mount
├── data_generator.py        # RobotDataGenerator: background telemetry simulation
└── strategies/
    ├── polling.py           # GET /api/polling/latest endpoint
    └── long_polling.py      # GET /api/long-polling/latest endpoint
app/static/
├── index.html               # Polling dashboard UI
├── app.js                   # Polling logic, metrics tracking, CSV export
└── style.css                # Styling
scripts/
└── run_polling_benchmark.py # Headless CLI benchmark
tests/
├── test_api.py
└── test_data_generator.py
results/                     # Benchmark output (gitignored)
```

## Tests

```bash
python -m compileall app scripts tests
pytest
```

## Design Decisions

- **No database** — polling latency and data age are the focus; a database would add noise unrelated to the strategy.
- **No framework in the frontend** — vanilla JS keeps the measurement path transparent and avoids framework overhead in latency numbers.
- **Shared data generator** — the same `RobotDataGenerator` feeds polling and long polling, and will feed SSE, WebSocket, and MQTT strategies later.
- **Long polling waits server-side** — the client passes `last_message_id`; the server waits for a newer generated message or returns `204` on timeout.
- **Deterministic seeding** — same seed = same `message_id` sequence, same status transitions. Makes scenario re-runs directly comparable.

## Known Limits

- The byte metric estimates response body bytes only, not full HTTP/TCP overhead.
- Browser and CLI timing are client-side measurements; OS scheduling and local load influence results.
- This baseline does not yet measure multi-client scalability or server CPU/memory usage.
- The generator uses a single robot; multi-robot scenarios are out of scope for the strategy comparison.
