# AutoPulse orchestration: Step Functions + EventBridge Scheduler

Serverless: nothing to install or keep running. Each pipeline is a **state
machine** (a list of steps that run in order) started by a **schedule**.

```
EventBridge Scheduler                     Step Functions state machine
---------------------                     ----------------------------
autopulse-collisions-daily                autopulse-collisions-pipeline
  every day 06:00 New York       --->       Glue collisions-ingestion -> Glue collisions-silver
                                            -> Glue collisions-gold -> Lambda validate-data-lake

autopulse-telemetry-every-15-minutes      autopulse-telemetry-pipeline
  every 15 min, Mon-Fri 8:00-17:45 NY --->  Glue telemetry-silver -> Glue telemetry-gold
                                            -> Lambda validate-data-lake
```

Not orchestrated here, because they run by themselves:
- the EC2 streaming host (simulator -> Kafka -> Spark -> Bronze), always on;
- Snowflake: Snowpipe loads each Gold file about a minute after it lands, the
  dynamic tables refresh themselves, a Snowflake task runs the nightly cleanup.

## How a run behaves

- Each Glue step **starts the job and waits until it finishes** (`.sync`).
- A failed Glue job is **retried once after 2 minutes**; if it fails again the
  run stops there (later steps don't run) and the run shows as **Failed** (red).
- The last step runs `validation/data_lake/validate_data_lake.py` as the Lambda
  `autopulse-validate-data-lake`. If any check FAILs, the run is marked Failed and
  the failed checks are shown in the step's error. Its output (`$.validation`)
  lists every check.
- Step Functions console -> state machine -> Executions: every run as a graph,
  with each step's input, output, duration and errors.

## Files

| File | What it is |
|---|---|
| `state_machines/collisions_pipeline.asl.json` | definition of `autopulse-collisions-pipeline` (Amazon States Language) |
| `state_machines/telemetry_pipeline.asl.json` | definition of `autopulse-telemetry-pipeline` |
| `schedules/collisions_daily.json` | settings of the schedule `autopulse-collisions-daily` |
| `schedules/telemetry_every_15_minutes.json` | settings of the schedule `autopulse-telemetry-every-15-minutes` |

IAM roles (in `policies/`): `AutoPulseStepFunctionsRole`, `AutoPulseSchedulerRole`,
`AutoPulseDataLakeValidationRole`.

## Cost

- Step Functions: ~$0.025 per 1,000 state transitions; ~45 runs a day are a few
  hundred transitions (cents per month).
- EventBridge Scheduler: free tier covers it.
- Glue: billed per job run (minimum 1 minute, 2 workers, ~$0.015 per run). The
  telemetry pipeline runs 2 jobs x 40 times a weekday, about $1.20 a day. To
  halve it, change the telemetry schedule to every 30 minutes
  (`cron(0/30 8-17 ? * MON-FRI *)`).

## Setup order (console)

See the steps in the project chat / below, in this order:
1. Lambda `autopulse-validate-data-lake` (role `AutoPulseDataLakeValidationRole`).
2. Role `AutoPulseStepFunctionsRole`, then the two state machines.
3. Role `AutoPulseSchedulerRole`, then the two schedules.

Until the Gold Glue jobs exist, create the schedules **disabled** (or don't
create them yet): a run would fail at the Gold step.
