import argparse
import json
import math
import os
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any

os.environ.setdefault("MPLCONFIGDIR", "/tmp/wally_matplotlib")

try:
    import matplotlib.pyplot as plt
except ImportError as exc:
    raise SystemExit(
        "matplotlib is required to generate figures. Install the project with "
        '`python -m pip install -e ".[dev]"` or run `python -m pip install matplotlib`.'
    ) from exc


DEFAULT_INPUT_DIR = Path("selected_results")
DEFAULT_OUTPUT_DIR = Path("figures")

STRATEGY_ORDER = [
    "polling_250",
    "polling_1000",
    "polling_2000",
    "long_polling",
    "sse",
    "mqtt",
    "grpc",
]

LABELS = {
    "polling_250": "Polling 250 ms",
    "polling_1000": "Polling 1000 ms",
    "polling_2000": "Polling 2000 ms",
    "long_polling": "Long polling",
    "sse": "SSE",
    "mqtt": "MQTT",
    "grpc": "gRPC",
}

COLORS = {
    "polling_250": "#d55e00",
    "polling_1000": "#e69f00",
    "polling_2000": "#f0c808",
    "long_polling": "#009e73",
    "sse": "#0072b2",
    "mqtt": "#56b4e9",
    "grpc": "#cc79a7",
}


@dataclass(frozen=True)
class RunSummary:
    source: Path
    key: str
    strategy: str
    poll_interval_ms: int | None
    duration_seconds: float
    requests_sent: int
    successful_responses: int
    duplicates: int
    duplicate_rate: float
    missed_messages: int
    downloaded_bytes: int
    data_age_avg_ms: float | None
    data_age_max_ms: float | None
    request_latency_avg_ms: float | None


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    runs = load_runs(input_dir)
    if not runs:
        raise SystemExit(f"No JSON result files found in {input_dir}")

    groups = group_runs(runs)
    ordered_keys = [key for key in STRATEGY_ORDER if key in groups]

    configure_matplotlib()
    write_summary_table(groups, ordered_keys, output_dir / "summary_table.csv")
    plot_data_age(groups, ordered_keys, output_dir, args.formats)
    plot_network_usage(groups, ordered_keys, output_dir, args.formats)
    plot_reliability(groups, ordered_keys, output_dir, args.formats)
    plot_request_volume(groups, ordered_keys, output_dir, args.formats)
    plot_polling_interval_tradeoff(groups, output_dir, args.formats)

    print(f"Wrote figures and summary table to {output_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate paper-ready matplotlib figures from selected benchmark JSON files."
    )
    parser.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR), help="Directory containing selected JSON files.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory for generated figures.")
    parser.add_argument(
        "--formats",
        nargs="+",
        default=["png", "svg"],
        choices=["png", "svg", "pdf"],
        help="One or more output formats.",
    )
    return parser.parse_args()


def load_runs(input_dir: Path) -> list[RunSummary]:
    runs = []
    for path in sorted(input_dir.glob("*.json")):
        with path.open(encoding="utf-8") as json_file:
            data = json.load(json_file)
        runs.append(parse_run(path, data))
    return runs


def parse_run(path: Path, data: dict[str, Any]) -> RunSummary:
    strategy = data["strategy"]
    poll_interval_ms = data.get("poll_interval_ms")
    key = result_key(strategy, poll_interval_ms)
    data_age = data.get("data_age_ms") or {}
    request_latency = data.get("request_latency_ms") or {}

    return RunSummary(
        source=path,
        key=key,
        strategy=strategy,
        poll_interval_ms=poll_interval_ms,
        duration_seconds=float(data["duration_seconds"]),
        requests_sent=int(data["requests_sent"]),
        successful_responses=int(data["successful_responses"]),
        duplicates=int(data["duplicates"]),
        duplicate_rate=float(data["duplicate_rate"]),
        missed_messages=int(data["missed_messages"]),
        downloaded_bytes=int(data["downloaded_bytes"]),
        data_age_avg_ms=clean_number(data_age.get("avg")),
        data_age_max_ms=clean_number(data_age.get("max")),
        request_latency_avg_ms=clean_number(request_latency.get("avg")),
    )


def result_key(strategy: str, poll_interval_ms: int | None) -> str:
    if strategy == "polling":
        return f"polling_{poll_interval_ms}"
    return strategy


def clean_number(value: Any) -> float | None:
    if value is None:
        return None
    number = float(value)
    if math.isnan(number):
        return None
    return number


def group_runs(runs: list[RunSummary]) -> dict[str, list[RunSummary]]:
    groups: dict[str, list[RunSummary]] = {}
    for run in runs:
        groups.setdefault(run.key, []).append(run)
    return groups


def configure_matplotlib() -> None:
    plt.rcParams.update(
        {
            "figure.figsize": (8.0, 4.8),
            "figure.dpi": 160,
            "savefig.dpi": 300,
            "font.size": 10,
            "axes.titlesize": 12,
            "axes.labelsize": 10,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "legend.fontsize": 9,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "grid.alpha": 0.25,
            "grid.linestyle": "-",
        }
    )


def write_summary_table(groups: dict[str, list[RunSummary]], keys: list[str], path: Path) -> None:
    lines = [
        "strategy,runs,data_age_avg_ms,data_age_std_ms,downloaded_kib,requests_sent,"
        "duplicate_rate,missed_messages,request_latency_avg_ms"
    ]
    for key in keys:
        runs = groups[key]
        lines.append(
            ",".join(
                [
                    LABELS.get(key, key),
                    str(len(runs)),
                    format_optional(mean(value.data_age_avg_ms for value in runs)),
                    format_optional(stdev(value.data_age_avg_ms for value in runs)),
                    f"{mean(value.downloaded_bytes for value in runs) / 1024:.2f}",
                    f"{mean(value.requests_sent for value in runs):.2f}",
                    f"{mean(value.duplicate_rate for value in runs):.4f}",
                    f"{mean(value.missed_messages for value in runs):.2f}",
                    format_optional(mean(value.request_latency_avg_ms for value in runs)),
                ]
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def plot_data_age(
    groups: dict[str, list[RunSummary]],
    keys: list[str],
    output_dir: Path,
    formats: list[str],
) -> None:
    values = [mean(run.data_age_avg_ms for run in groups[key]) for key in keys]
    errors = [stdev(run.data_age_avg_ms for run in groups[key]) or 0 for key in keys]

    fig, ax = plt.subplots()
    bar_plot(ax, keys, values, ylabel="Gemiddelde data age (ms)", errors=errors)
    ax.set_title("Actualiteit van ontvangen robotdata per strategie")
    ax.set_yscale("log")
    ax.set_ylim(bottom=0.8)
    add_value_labels(ax, values, "{:.1f}")
    save_figure(fig, output_dir / "data_age_by_strategy", formats)


def plot_network_usage(
    groups: dict[str, list[RunSummary]],
    keys: list[str],
    output_dir: Path,
    formats: list[str],
) -> None:
    values = [mean(run.downloaded_bytes for run in groups[key]) / 1024 for key in keys]

    fig, ax = plt.subplots()
    bar_plot(ax, keys, values, ylabel="Gedownloade data (KiB)")
    ax.set_title("Netwerkverbruik tijdens benchmarkrun")
    add_value_labels(ax, values, "{:.1f}")
    save_figure(fig, output_dir / "network_usage_by_strategy", formats)


def plot_reliability(
    groups: dict[str, list[RunSummary]],
    keys: list[str],
    output_dir: Path,
    formats: list[str],
) -> None:
    duplicates = [mean(run.duplicates for run in groups[key]) for key in keys]
    missed = [mean(run.missed_messages for run in groups[key]) for key in keys]
    x_positions = list(range(len(keys)))
    width = 0.38

    fig, ax = plt.subplots()
    duplicate_bars = ax.bar(
        [position - width / 2 for position in x_positions],
        duplicates,
        width=width,
        color="#d55e00",
        label="Dubbele berichten",
    )
    missed_bars = ax.bar(
        [position + width / 2 for position in x_positions],
        missed,
        width=width,
        color="#0072b2",
        label="Gemiste berichten",
    )
    ax.set_xticks(x_positions, [LABELS.get(key, key) for key in keys], rotation=30, ha="right")
    ax.set_ylabel("Aantal berichten")
    ax.set_title("Betrouwbaarheid van ontvangen berichten")
    ax.legend()
    ax.margins(y=0.18)
    add_bar_labels(ax, duplicate_bars, duplicates, "{:.0f}")
    add_bar_labels(ax, missed_bars, missed, "{:.0f}")
    fig.tight_layout()
    save_figure(fig, output_dir / "reliability_by_strategy", formats)


def plot_request_volume(
    groups: dict[str, list[RunSummary]],
    keys: list[str],
    output_dir: Path,
    formats: list[str],
) -> None:
    values = [mean(run.requests_sent for run in groups[key]) for key in keys]

    fig, ax = plt.subplots()
    bar_plot(ax, keys, values, ylabel="Requests / ontvangen events")
    ax.set_title("Client-server interacties tijdens benchmarkrun")
    add_value_labels(ax, values, "{:.0f}")
    save_figure(fig, output_dir / "request_volume_by_strategy", formats)


def plot_polling_interval_tradeoff(
    groups: dict[str, list[RunSummary]],
    output_dir: Path,
    formats: list[str],
) -> None:
    polling_keys = [key for key in ["polling_250", "polling_1000", "polling_2000"] if key in groups]
    if len(polling_keys) < 2:
        return

    intervals = [int(key.split("_")[1]) for key in polling_keys]
    data_age = [mean(run.data_age_avg_ms for run in groups[key]) for key in polling_keys]
    network_kib = [mean(run.downloaded_bytes for run in groups[key]) / 1024 for key in polling_keys]
    duplicate_rate = [mean(run.duplicate_rate for run in groups[key]) * 100 for key in polling_keys]
    missed = [mean(run.missed_messages for run in groups[key]) for key in polling_keys]

    fig, ax_left = plt.subplots()
    ax_right = ax_left.twinx()

    first = ax_left.plot(intervals, data_age, marker="o", color="#0072b2", label="Data age")
    second = ax_right.plot(intervals, network_kib, marker="s", color="#d55e00", label="Netwerkverbruik")
    third = ax_right.plot(intervals, duplicate_rate, marker="^", color="#009e73", label="Duplicate rate")
    fourth = ax_left.plot(intervals, missed, marker="D", color="#cc79a7", label="Gemiste berichten")

    ax_left.set_xlabel("Pollinginterval (ms)")
    ax_left.set_ylabel("Data age / gemiste berichten")
    ax_right.set_ylabel("KiB / duplicate rate (%)")
    ax_left.set_title("Trade-off bij short polling")
    ax_left.set_xticks(intervals)

    lines = first + second + third + fourth
    ax_left.legend(lines, [line.get_label() for line in lines], loc="best")
    fig.tight_layout()
    save_figure(fig, output_dir / "polling_interval_tradeoff", formats)


def bar_plot(
    ax: plt.Axes,
    keys: list[str],
    values: list[float | None],
    ylabel: str,
    errors: list[float] | None = None,
) -> None:
    x_positions = list(range(len(keys)))
    numeric_values = [0 if value is None else value for value in values]
    ax.bar(
        x_positions,
        numeric_values,
        yerr=errors,
        capsize=4 if errors else 0,
        color=[COLORS.get(key, "#777777") for key in keys],
    )
    ax.set_xticks(x_positions, [LABELS.get(key, key) for key in keys], rotation=30, ha="right")
    ax.set_ylabel(ylabel)
    ax.margins(y=0.12)
    ax.set_axisbelow(True)
    ax.grid(axis="x", visible=False)
    ax.grid(axis="y", visible=True)
    ax.figure.tight_layout()


def add_value_labels(ax: plt.Axes, values: list[float | None], fmt: str) -> None:
    for patch, value in zip(ax.patches, values, strict=True):
        if value is None:
            continue
        ax.annotate(
            fmt.format(value),
            (patch.get_x() + patch.get_width() / 2, patch.get_height()),
            ha="center",
            va="bottom",
            xytext=(0, 3),
            textcoords="offset points",
            fontsize=8,
        )


def add_bar_labels(ax: plt.Axes, bars: Any, values: list[float | None], fmt: str) -> None:
    _, upper = ax.get_ylim()
    zero_offset = upper * 0.015
    for patch, value in zip(bars, values, strict=True):
        if value is None:
            continue
        height = patch.get_height()
        y_position = height if height > 0 else zero_offset
        ax.annotate(
            fmt.format(value),
            (patch.get_x() + patch.get_width() / 2, y_position),
            ha="center",
            va="bottom",
            xytext=(0, 3),
            textcoords="offset points",
            fontsize=8,
        )


def save_figure(fig: plt.Figure, path_without_suffix: Path, formats: list[str]) -> None:
    for file_format in formats:
        fig.savefig(path_without_suffix.with_suffix(f".{file_format}"), bbox_inches="tight")
    plt.close(fig)


def mean(values: Any) -> float | None:
    clean_values = [float(value) for value in values if value is not None]
    if not clean_values:
        return None
    return statistics.fmean(clean_values)


def stdev(values: Any) -> float | None:
    clean_values = [float(value) for value in values if value is not None]
    if len(clean_values) < 2:
        return None
    return statistics.stdev(clean_values)


def format_optional(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.2f}"


if __name__ == "__main__":
    main()
