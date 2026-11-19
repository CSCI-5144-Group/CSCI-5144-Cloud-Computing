"""Write a concise benchmark report, raw measurements, and offline charts."""

import csv
from dataclasses import asdict, fields
from html import escape
from statistics import median

from benchmark.workload import Measurement

METRICS = (
    ("throughput_ops_s", "Throughput", "ops/s"),
    ("mean_ms", "Mean latency", "ms"),
    ("p99_ms", "p99 latency", "ms"),
)


# Summarize successful trials with medians and retain failure counts.
def summarize(rows):
    summary = []
    configurations = [("assignment3", nodes) for nodes in (1, 2, 3)]
    if any(row.system == "assignment2" for row in rows):
        configurations.insert(0, ("assignment2", 1))
    for workers in sorted({row.workers for row in rows}):
        for system, nodes in configurations:
            for method in ("PUT", "GET"):
                selected = [
                    row for row in rows
                    if row.workers == workers and row.system == system
                    and row.nodes == nodes and row.method == method
                ]
                valid = [
                    row for row in selected
                    if row.successful == row.attempts and not row.errors
                ]
                summary.append({
                    "system": system, "nodes": nodes, "method": method, "workers": workers,
                    "replication_factor": min(2, nodes), "completed": len(valid),
                    "errors": sum(row.errors for row in selected),
                    **{field: median(getattr(row, field) for row in valid) if valid else None
                       for field, _, _ in METRICS},
                })
    return summary


# Use the same display precision in the table and charts.
def display(value, decimals):
    return f"{value:,.{decimals}f}" if value is not None else "—"


# Draw one scale per chart, leaving missing measurements blank.
def chart(title, field, unit, rows):
    maximum = max((row[field] or 0 for row in rows), default=0) or 1
    bars = []
    for row in rows:
        value = row[field]
        width = 100 * (value or 0) / maximum
        label = display(value, 0 if unit == "ops/s" else 2)
        bars.append(
            f'<li><span>{"A2" if row["system"] == "assignment2" else "A3"} · {row["nodes"]}</span>'
            f'<span class="track"><span class="bar" style="width:{width:.5f}%"></span></span>'
            f'<strong>{label} {unit if value is not None else ""}</strong></li>'
        )
    return f'<section><h2>{escape(title)}</h2><ul>{"".join(bars)}</ul></section>'


# Write three portable outputs with no extra plotting dependency.
def write_report(output, rows, settings, failures):
    output.mkdir(parents=True, exist_ok=False)
    with (output / "trials.csv").open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=[field.name for field in fields(Measurement)])
        writer.writeheader()
        writer.writerows(asdict(row) for row in rows)

    summary = summarize(rows)
    report = [
        "# KV Store Benchmark", "",
        f"- Host: {settings['host']}",
        f"- Workload: {settings['keys']} keys, {settings['value_bytes']} ASCII bytes per value, "
        f"client counts {settings['workers']}, {settings['trials']} planned trials per configuration.",
        f"- Each phase runs for {settings.get('duration', 0):g} measured seconds after "
        f"{settings.get('warmup', 0):g} warm-up seconds. Workers repeatedly traverse disjoint "
        "key partitions until the deadline. In-flight requests finish before the phase ends.",
        "- Each configuration starts on fresh processes. An untimed seed PUT and verification GET "
        "prepare the dataset. Timed PUT updates existing keys. Timed GET verifies the latest values.",
        "- Throughput counts successful operations per second of wall time. Latencies are "
        "client-observed. Each result is the median of the corresponding trial metric. "
        "p99 is calculated per trial using the nearest-rank method.",
        "- The single-node configuration writes one copy. Two and three nodes write two copies. "
        "All containers share the same Docker host.", "",
        "- HTTP access and routine operation logs are suppressed for both systems. "
        "Warnings and errors remain visible. Benchmark progress is printed by the client.",
        "| Clients | System | Nodes | Copies per key | Phase | Valid / planned trials | Throughput (ops/s) | Mean (ms) | p99 (ms) | Errors |",
        "| ---: | :--- | ---: | ---: | :--- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary:
        report.append(
            f"| {row['workers']} | {row['system']} | {row['nodes']} | {row['replication_factor']} | {row['method']} | "
            f"{row['completed']} / {settings['trials']} | "
            f"{display(row['throughput_ops_s'], 0)} | {display(row['mean_ms'], 2)} | "
            f"{display(row['p99_ms'], 2)} | {row['errors']} |"
        )
    if any(row.system == "assignment2" for row in rows):
        report.extend([
            "", "Assignment 2 uses its original server and Dockerfile with operation logging disabled at startup. "
            "Assignment 3 uses a router with routine logs suppressed. Both receive the same workload through "
            "the same client code on the same Docker host. This compares the complete implementations.",
        ])
    if failures:
        report.extend(["", "## Run issues", "", *[f"- {failure}" for failure in failures]])
    report.extend(["", "[Charts](charts.html) · [Per-trial data](trials.csv)", ""])
    (output / "report.md").write_text("\n".join(report), encoding="utf-8")

    charts = [
        chart(f"{workers} clients — {method} {label.lower()}", field, unit,
              [row for row in summary if row["method"] == method and row["workers"] == workers])
        for workers in sorted({row["workers"] for row in summary})
        for method in ("PUT", "GET")
        for field, label, unit in (METRICS[0], METRICS[2])
    ]
    (output / "charts.html").write_text(
        """<!doctype html>
<html lang="en">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>KV Store Benchmark</title>
<style>
body { margin: 2rem auto; padding: 0 1rem; max-width: 65rem; font: 16px system-ui, sans-serif; color: #14232c; }
main { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 25rem), 1fr)); gap: 1rem; }
section { border: 1px solid #c5d0d6; border-radius: .5rem; padding: 0 1rem .25rem; }
h2 { font-size: 1.15rem; }
ul { list-style: none; padding: 0; }
li { display: grid; grid-template-columns: 4em minmax(3em, 1fr) 7em; gap: .5em; align-items: center; margin: .6em 0; }
.track { display: block; background: #e5edf0; min-height: .75em; border-radius: .3em; overflow: hidden; }
.bar { display: block; min-height: .75em; background: #176c70; }
strong { text-align: right; font-variant-numeric: tabular-nums; }
</style>
<h1>KV Store Benchmark</h1>
<p>Median of valid trials. A2 = Assignment 2. A3 = Assignment 3. The number is the KV node count. Compare bar lengths within each chart.</p>
<main>""" + "".join(charts) + '</main><p><a href="trials.csv">Per-trial data</a></p>\n',
        encoding="utf-8",
    )
