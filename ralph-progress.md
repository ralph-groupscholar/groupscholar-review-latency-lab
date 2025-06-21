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
