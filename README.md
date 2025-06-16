# Group Scholar Review Latency Lab

A local-first simulation CLI for exploring scholarship application review throughput, queue pressure, and cycle-time risk.

## Features

- Discrete day simulation of multi-stage review pipelines
- Capacity, service-time, and arrival-rate modeling
- Cycle-time percentiles, utilization, queue pressure, and backlog summaries
- Stage aging and near-due risk signals for in-flight work
- JSON or text output for briefs and weekly operations updates

## Quickstart

```bash
go run .
```

## Use a custom config

```bash
go run . --config data/sample-config.json
```

## JSON output

```bash
go run . --format json
```

## Write a starter config

```bash
go run . --write-sample /tmp/review-config.json
```

## Config schema

```json
{
  "horizon_days": 60,
  "arrival_rate_per_day": 18,
  "target_cycle_days": 21,
  "near_due_window_days": 3,
  "stages": [
    {"name": "Intake", "capacity_per_day": 20, "min_days": 1, "max_days": 2}
  ]
}
```

## Notes

This tool uses a deterministic seed if you pass `--seed`. If you do not, it will use the current time.
