# Group Scholar Review Latency Lab Progress

## Iteration 1
- Added estimated wait-time and pressure signals per stage to highlight queue risk.
- Extended text and JSON reports to surface the new stage pressure indicators.

## Iteration 2
- Added stage aging metrics (average age, overdue, near-due WIP) to the JSON and text reports.
- Documented stage-level aging risk signals in the README feature list.

## Iteration 2
- Added stage-entry tracking to compute in-stage aging, oldest item age, and stage SLA risk.
- Updated report output and JSON schema to include stage max days and overdue/near-due counts.

## Iteration 3
- Added constraint-stage summary that highlights throughput gaps versus arrivals and the highest-utilization stage.
- Extended text and JSON reports with a focused capacity recommendation for the constraint stage.

## Iteration 4
- Added an action queue that ranks the top stages by urgency signals and provides recommended interventions.
- Extended text and JSON outputs with the action queue for quick operational triage.

## Iteration 5
- Added flow balance classification with growing/stable/draining counts plus top growth/drain stage highlights.
- Extended text and JSON reports with the new flow balance summary and documented the update.

## Iteration 5
- Added projected SLA risk bands for WIP using min/max remaining service time estimates.
- Extended text and JSON outputs with projected late counts to flag likely breaches.

## Iteration 6
- Added stage service-time tracking with average and p90 service durations.
- Extended text/JSON outputs and README to surface processing variability per stage.

## Iteration 7
- Added stage-level near-due window configuration to tune stage aging alerts.
- Updated sample configs and README to document the stage near-due window setting.

## Iteration 7
- Added throughput volatility tracking (daily completions, std dev, coefficient of variation) per stage.
- Updated action queue and text report to flag volatile throughput alongside queue volatility.

## Iteration 8
- Added arrival and net-flow volatility metrics (std dev/CV) per stage plus overall arrival volatility.
- Extended text output and JSON report fields to surface demand swings and backlog instability.

## Iteration 9
- Added WIP trend regression (slope + fit) per stage to flag persistent backlog growth/decline.
- Extended action queue and text/JSON outputs to surface increasing WIP trends.
- Added unit tests for trend regression and classification.

## Iteration 14
- Added Postgres storage for simulation runs and per-stage summaries with init/seed support.
- Added CLI flags for database setup and updated README with storage instructions.
- Added unit tests covering percentiles, flow balance classification, and trend detection.

## Iteration 88
- Added queue/active p50 and p90 percentiles to stage summaries for load distribution insight.
- Stored queue/active percentiles in Postgres stage summaries and updated the text report output.
- Added percentile coverage in tests and documented the new percentile feature.

## Iteration 99
- Added stage cycle-time metrics (avg/p50/p90, on-time rate, over-max count) to quantify in-stage SLA performance.
- Extended text/JSON reports and Postgres storage schema to include the new stage cycle-time signals.
- Added unit tests covering stage cycle-time summary calculations.

## Iteration 102
- Added stage service-time variability metrics (std dev and CV) to summaries, action queue, and text/JSON output.
- Extended Postgres schema/storage to persist service-time variability metrics.
- Added tests covering service-time variability calculations.

## Iteration 103
- Fixed Postgres stage summary storage to include idle/starved days plus recovery and backlog clearance metrics.
- Added schema migrations for service-time variability columns to keep existing databases consistent.
- Updated stage summary inserts to persist recovery throughput and clearance signals.

## Iteration 101
- Added stage cycle-time variability metrics (std dev and CV) to summaries, JSON/text output, and Postgres storage.
- Added schema migrations and storage updates for stage cycle-time variability fields.
- Added tests covering stage cycle-time variability calculations and updated README feature list.

## Iteration 103
- Added capacity slack per day and slack percent metrics to stage summaries and action signals.
- Extended text/JSON reports and Postgres storage to persist capacity slack fields.
- Added test coverage for capacity slack calculations.
