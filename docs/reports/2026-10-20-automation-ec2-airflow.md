# AutoPulse — Change Report: Automated Pipeline (EC2 + Airflow) (Step 4)

**Date:** 2026-10-20  **Goal:** the whole pipeline runs on its own in AWS, with no manual steps.
**Git:** nothing was committed. All changes are uncommitted edits in your working tree (`git status`, `git diff`).

> **Not yet run in AWS.** `ec2_host.py` and the EC2 bundle were tested against a fake AWS (launch, retry while the instance profile settles, re-launch is a no-op, password and update via SSM, bundle contents and permissions). The Compose file was parsed and checked. The Airflow DAGs and the incremental Spark job compile but haven't run, because neither Airflow nor Spark is available in my workspace.

---

## 1. How it runs now

```
EC2 host (always on)                                     Airflow on the EC2 host
  simulator ─► Kafka ─► Spark streaming ─► S3 Bronze       06:00 daily:   Lambda (API ─► Bronze) ─► Glue collisions Silver
                                                           every 15 min:  Glue telemetry Silver (new Bronze files only)
                                                                    │
                          S3 Silver ─► Snowpipe ─► Snowflake RAW ─► task (5 min) ─► facts/dimensions ─► views
```

After setup, nothing needs to be started by hand. Docker restart policies bring everything back after a reboot or a stop/start of the instance.

---

## 2. New files

| File | Purpose |
|---|---|
| `airflow/dags/autopulse_collisions_daily.py` | 06:00 New York time: invoke the Lambda (latest 30 days of available data), then run the collisions Glue job and wait for it. 2 retries, 5 min apart. Fails if the Lambda returns an error. Schedule and window can be changed with environment variables |
| `airflow/dags/autopulse_vehicle_telemetry.py` | Every 15 min: run the telemetry Glue job. Never overlaps; 30-min run timeout |
| `deploy/ec2/docker-compose.yml` | Host services: Kafka, simulator, Spark streaming (Kafka → Bronze on S3, checkpoint on S3), Postgres, and Airflow 2.10.5 (init, scheduler, web UI bound to localhost only). Restart policies, log rotation, persistent volumes |
| `deploy/ec2/Dockerfile` | Pipeline image: Python 3.12 + Java 17 + pinned PySpark 4.0.1 + `src/` |
| `deploy/ec2/requirements-pipeline.txt` | Pinned packages for that image |
| `deploy/ec2/bootstrap.sh` | First-boot script (EC2 user data): Docker + Compose + Buildx, 2 GB swap, a random Airflow admin password (kept on the instance), downloads the code bundle, `docker compose up` |
| `deploy/ec2/update.sh` | On the host: download the latest bundle, swap the code folder, rebuild and restart changed services, restart Airflow so it picks up the new DAGs |
| `infra/aws/ec2_host.py` | `launch`, `status`, `update`, `logs`, `password`, `stop`, `start`, `terminate`. Launch picks the latest Amazon Linux 2023 and a default-VPC subnet, and creates a security group with **no inbound rules**. It sets IMDSv2 with hop limit 2 so containers can use the instance role, and uses a 30 GB gp3 disk. It retries while a new instance profile settles. `update`, `logs` and `password` run through SSM |
| `docs/reports/2026-10-20-automation-ec2-airflow.md` | This report |

## 3. Modified files

| File | Change |
|---|---|
| `src/batch/transform_vehicle_telemetry.py` | **Now incremental.** It reads Bronze as a stream with `trigger(availableNow=True)`: each run processes only the files that arrived since the last run, **appends** to Silver and quarantine (one file per batch), and stops. Progress is kept in `<CHECKPOINT_ROOT>/vehicle_telemetry_silver`. Validation rules are identical; the logic moved into `transform()`. If Bronze doesn't exist yet, it logs a warning and exits successfully instead of failing |
| `glue/jobs/vehicle_telemetry_silver_job.py` | Also reads `--CHECKPOINT_ROOT` |
| `glue/jobs/collisions_silver_job.py` | Docstring only |
| `infra/aws/deploy.py` | Also builds and uploads the EC2 bundle (`artifacts/ec2/autopulse-ec2-bundle.tar.gz`, plus a versioned copy). Shell scripts are converted to Unix line endings and made executable. Glue jobs now get `--CHECKPOINT_ROOT s3://<bucket>/_checkpoints`. The final message points to `ec2_host.py` |
| `snowflake/07_analytics_views.sql` | `VW_FLEET_STATUS`: a vehicle counts as "online" if it reported within **30 min** (was 5). Telemetry now reaches Snowflake in about 15–25 minutes |
| `infra/aws/README.md` | New **step 10**: EC2 host + Airflow (setup, everyday commands, access, cost) |
| `README.md` | Cloud run commands and status checklist |

---

## 4. Why telemetry Silver had to become incremental

The old job rewrote **all** of Silver on every run. On a 15-minute schedule, each run would write brand-new files containing the entire history. Snowpipe would then re-load that whole history into Snowflake 96 times a day: about 170,000 events per day of data multiplied by every run, which wastes credits and storage. The incremental version writes only new rows, so Snowflake loads each event about once.

Collisions Silver is still a full rebuild. That's fine because it runs once a day on a few thousand rows, and Snowflake's MERGE de-duplicates.

## 5. Decisions to review

| Decision | Why | Alternative |
|---|---|---|
| Telemetry every 15 min | Fresh enough for a fleet dashboard. Glue cost about $2/day | Every 5 min (fresher, ~3× cost) or hourly (cheaper) — set `AUTOPULSE_TELEMETRY_SCHEDULE` |
| Airflow 2.10.5 with LocalExecutor + Postgres | Mature and well-documented, and enough for 2 DAGs | Airflow 3.x, or Amazon's managed Airflow (MWAA, ~$350+/month) |
| t3.large (8 GB) + 2 GB swap | Kafka + Spark + Airflow fit comfortably | t3.xlarge if it's tight |
| No inbound ports; access through SSM | No SSH keys or open ports in a shared account | SSH with a key pair |
| Streaming checkpoints on S3 | Survive replacing the instance | Local disk (slightly faster) |

---

## 6. What you need to do (about 20 min, mostly waiting)

1. **Install the Session Manager plugin** in WSL. The commands are in `infra/aws/README.md` step 10.
2. **One-time Silver reset for telemetry.** The old full-rewrite files would otherwise get duplicates from the first incremental run. Snowflake isn't set up yet, so this is safe:
   ```bash
   aws s3 rm s3://autopulse-julio-leon/silver/vehicle_telemetry/ --recursive
   aws s3 rm s3://autopulse-julio-leon/quarantine/vehicle_telemetry/ --recursive
   ```
3. **Deploy and launch:**
   ```bash
   python infra/aws/deploy.py
   python infra/aws/ec2_host.py launch
   ```
4. After about 10 minutes, run `python infra/aws/ec2_host.py logs`. All services should say `Up`, and `streaming` should show micro-batches.
5. **Open Airflow:** run `python infra/aws/ec2_host.py password` and the port-forward command from `status`, then open http://localhost:8080. To test immediately, trigger `autopulse_collisions_daily` with ▶.
6. Send me any errors. The most likely permission gaps are `ec2:RunInstances`, `iam:PassRole` (attaching the instance role), and `ssm:SendCommand`.

Locally (WSL), `transform_vehicle_telemetry.py` is now incremental too. Its checkpoint is `checkpoints/vehicle_telemetry_silver` unless `CHECKPOINT_ROOT` is set. **Don't run the local streaming or telemetry jobs against the S3 bucket while the EC2 host is running**; they'd write the same data twice.

## 7. Next steps

| When | Work |
|---|---|
| After the host is up | Snowflake setup (`snowflake/README.md`), using `deploy.py --snowflake-*` for the role |
| Then | Retire the Spark Gold jobs after the parity check |
| Then | Dashboard (Streamlit in Snowflake) |
| Wed–Thu 10/28–29 | End-to-end check, cost check, rehearsal |

---

## Addendum (2026-10-20, 16:12): first boot results and fixes

**Result:** the host booted and finished setup in about 7 minutes. Kafka, Postgres, the Airflow scheduler and the web UI are up, and SSM commands work.

**Problem 1, simulator restarting:** `deploy/ec2/requirements-pipeline.txt` pinned `kafka-python==2.0.2`, which is known to fail on Python 3.12+ (its bundled `six` module breaks imports). The container uses Python 3.12. **Fix:** `kafka-python>=2.0.3,<3`.

**Problem 2, streaming restarting:** the cause isn't visible in the summary log yet. **Change:** `infra/aws/ec2_host.py logs --service <name> --lines <n>` shows one service's full recent log.

Files changed: `deploy/ec2/requirements-pipeline.txt`, `infra/aws/ec2_host.py`.

---

## Addendum (2026-10-20, 17:00): telemetry Silver stayed empty: one S3 scheme everywhere

**Symptom:** 4 successful runs of `autopulse-vehicle-telemetry-silver`, 4,150 events in Bronze, 0 rows in Silver. The job's checkpoint had no batches: Spark found **zero files**.

**Cause:** the EC2 streaming job wrote Bronze through `s3a://…`, so its file log (`bronze/vehicle_telemetry/_spark_metadata/`) lists every file as `s3a://autopulse-julio-leon/…`. Glue reads `s3://autopulse-julio-leon/…`. When Spark reads a folder that has such a log, it matches the logged files against the folder path, including the scheme. `s3a` ≠ `s3`, so it found no files, and the job counted as a successful run with nothing to process.

**Earlier symptom, same root:** before the reset at 16:22, the laptop's streaming run had already written batches 0…N to the same log, so the EC2 stream's first batches were skipped as "already committed".

**Fix**
- `src/common/config.py`: paths now **always use `s3://`** (`s3a://` input is converted to `s3://`). Self-managed Spark maps `s3://` to the S3A connector (`fs.s3.impl = S3AFileSystem`), so the EC2 streaming job, local runs and Glue all record and read the same paths. Credentials and region settings are unchanged.
- `infra/aws/ec2_host.py`: new **`reset-telemetry`** command. It stops streaming, deletes telemetry Bronze, Silver, quarantine and both checkpoints, then starts streaming again. Kafka keeps 7 days, so events are re-read. It asks you to type RESET, or skips that with `--yes`. Tested against a fake AWS: it deletes only the telemetry prefixes, including Glue's `_$folder$` markers, and leaves collisions untouched.
- `infra/aws/README.md` step 10: the new commands, and a warning never to run a second streaming writer against the bucket.

**Apply:** `python infra/aws/deploy.py && python infra/aws/ec2_host.py update && python infra/aws/ec2_host.py reset-telemetry`
