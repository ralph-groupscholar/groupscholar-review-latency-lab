# Group Scholar Review Latency Lab

A local-first simulation CLI for exploring scholarship application review throughput, queue pressure, and cycle-time risk.

## Features

- Discrete day simulation of multi-stage review pipelines
- Capacity, service-time, and arrival-rate modeling (fixed or Poisson)
- Cycle-time percentiles, utilization, queue pressure, and backlog summaries
- Stage aging and near-due risk signals for in-flight work
- Stage aging percentiles (p50/p90) to spot skewed wait times
- Projected SLA risk bands for in-flight work (min/max remaining service time)
- Constraint-stage summary with throughput gap and capacity recommendation
- Flow balance summary that counts growing/stable/draining stages plus top growth/drain stages
- Action queue for the top three stages needing attention
- Stage service-time averages, p90s, std dev, and CV plus throughput volatility to gauge processing variability
- Stage cycle-time averages, percentiles, and on-time rate to flag SLA erosion
- Queue and active WIP percentiles (p50/p90) to quantify typical vs. worst-case stage load
- Arrival and net-flow volatility (CV) to show demand swings and backlog instability
- WIP trend regression (slope and fit) to flag persistent backlog growth or decline
- Capacity slack (per day and percent) to quantify buffer before saturation
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

## Store runs in Postgres

Set `GS_REVIEW_LATENCY_DB_URL` (or `DATABASE_URL`) to a Postgres connection string, then:

```bash
go run . --init-db
go run . --store
```

`--init-db` creates the `review_latency_lab` schema and seeds a few sample runs if empty.
`--store` saves the current run alongside per-stage summaries.

## Config schema

```json
{
  "horizon_days": 60,
  "arrival_rate_per_day": 18,
  "arrival_mode": "fixed",
  "target_cycle_days": 21,
  "near_due_window_days": 3,
  "stage_near_due_window_days": 1,
  "stages": [
    {"name": "Intake", "capacity_per_day": 20, "min_days": 1, "max_days": 2}
  ]
}
```

## Notes

This tool uses a deterministic seed if you pass `--seed`. If you do not, it will use the current time.
Stage aging in the report is calculated from stage entry and flags near-due work when it is within 1 day of the stage max.
`arrival_mode` accepts `fixed` (deterministic) or `poisson` (randomized around the mean).
`stage_near_due_window_days` adjusts the stage-level near-due window (default: 1 day).
