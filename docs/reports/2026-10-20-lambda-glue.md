# AutoPulse — Change Report: Lambda + Glue (Step 3)

**Date:** 2026-10-20  **Decision:** use Lambda, Glue, EC2 and Airflow on AWS.
**Git:** nothing was committed. All changes are uncommitted edits in your working tree (`git status`, `git diff`).

> **Not yet deployed or run in AWS.** I can't reach your AWS account from here. The new unit tests (10) pass. The CloudFormation template was checked for structure and references, and the deploy script for shell syntax. The Spark code paths in Glue and the pyarrow write in Lambda will first run when you deploy.

---

## 1. Target architecture

| Step | Runs on | Code |
|---|---|---|
| NYC API → Bronze collisions | **Lambda** `autopulse-collisions-ingestion` | `src/lambda_functions/collisions_ingestion.py` |
| Simulator → Kafka → Bronze telemetry | **EC2** (Docker Compose) | existing `src/streaming/*` |
| Bronze → Silver collisions | **Glue** `autopulse-collisions-silver` | existing `src/batch/transform_collisions.py` |
| Bronze → Silver telemetry + quarantine | **Glue** `autopulse-vehicle-telemetry-silver` | existing `src/batch/transform_vehicle_telemetry.py` |
| Silver → star schema → views | **Snowflake** | `snowflake/*.sql` (unchanged) |
| Scheduling and monitoring | **Airflow on EC2** | *next step* |

The Glue jobs run **the same PySpark code** as local runs. Only a small wrapper and a "glue" mode in `config.py` were added. Gold stays in Snowflake, so there are no Gold Glue jobs.

---

## 2. New files

| File | Purpose |
|---|---|
| `src/ingestion/collisions_api.py` | NYC API client in **pure Python**: paging, retries, latest-date lookup, window logic, string normalization. It moved out of `nyc_collisions_api.py` so Lambda, the local CLI and the tests share it. It uses `urllib`, because the `requests` library isn't in the Lambda runtime |
| `src/lambda_functions/__init__.py`, `collisions_ingestion.py` | Lambda handler. Groups records by crash date and writes one Snappy Parquet file per date partition with **pyarrow**: every column as a string, plus `ingested_at` (UTC). It writes the new file first, then deletes that partition's older files, matching Spark's dynamic overwrite. Records without a crash date go to `__HIVE_DEFAULT_PARTITION__`, as in Spark |
| `glue/jobs/collisions_silver_job.py`, `glue/jobs/vehicle_telemetry_silver_job.py` | Glue entry points. Read `--DATA_ROOT`, set `AUTOPULSE_RUNTIME=glue`, call the existing `main()` |
| `infra/aws/cloudformation/autopulse-compute.yaml` | One stack: the Lambda (Python 3.12 + AWS SDK for pandas layer v31 for pyarrow, 1 GB, 15-min timeout, 14-day logs), 2 Glue 5.0 jobs (2 × G.1X, 30-min timeout, no retries, one run at a time), and roles: `AutoPulseLambdaRole` (can write only `bronze/collisions/`), `AutoPulseGlueJobRole`, `AutoPulseEc2HostRole` + instance profile (S3, invoke the Lambda, start/monitor the Glue jobs, Systems Manager shell). It also has an optional `AutoPulseSnowflakeRole` |
| `infra/aws/deploy.sh` | Zips `src/`, uploads it and the Glue scripts to `s3://…/artifacts/` with a timestamp version, then runs `aws cloudformation deploy`. Re-run it after any code change |
| `tests/test_collisions_ingestion.py` | 10 unit tests (no Spark, network or AWS): paging, record cap, retries, window logic, normalization, partition grouping, partition replacement, and a full handler run against fake S3 |
| `.gitattributes` | Keeps `*.sh` on Unix line endings. Windows line endings would break `deploy.sh` in WSL |
| `docs/reports/2026-10-20-lambda-glue.md` | This report |

## 3. Modified files

| File | Change |
|---|---|
| `src/common/config.py` | New `RUNTIME` setting (`AUTOPULSE_RUNTIME`: `local` default, `glue`, or `lambda`). In Glue and Lambda, S3 paths stay `s3://` instead of `s3a://`, and no log file is written (the code folder is read-only there; output goes to CloudWatch). `build_spark()` in Glue only applies session settings: UTC, standard Parquet timestamps, shuffle partitions, no `_SUCCESS` files. Also new: `split_s3_uri()`, and `is_s3()` now accepts `s3://`. **Local behavior is unchanged** |
| `src/ingestion/nyc_collisions_api.py` | Now a thin Spark CLI over `collisions_api.py`. Same arguments, same Bronze output |
| `infra/aws/README.md` | Step 4b: the EC2 role now comes from the stack. New **step 9**: deploying, permissions, the Snowflake-role-through-the-stack option, test commands, logs, cost |
| `README.md` | Tech stack (Lambda, Glue, CloudFormation), architecture diagram, status checklist |
| `.gitignore` | Added `virtualenv/` (your venv folder, which was not ignored), `build/` (deploy packaging) and `data/quarantine/` |

Unchanged: the Silver and streaming logic, the Snowflake SQL, the data-quality tests.

---

## 4. Decisions to review

| Decision | Why |
|---|---|
| Lambda writes Parquet with pyarrow, not Spark | Spark doesn't run on Lambda. The AWS-managed pandas layer provides pyarrow with no packaging work |
| Lambda only allowed to write `bronze/collisions/` | Least privilege |
| Glue 5.0, 2 × G.1X workers | Smallest standard Spark setup, a few cents per run |
| Code zip versioned by timestamp | CloudFormation only updates the Lambda and Glue jobs when the S3 key changes |
| Airflow on EC2 triggers the Lambda and Glue | Matches your choice to keep Airflow. The EC2 role already has exactly those permissions |
| Snowflake role can be created by the stack | One template your admin can deploy if your login can't create roles |

---

## 5. What you need to do

1. **Pull the changes into WSL.** They're already in your folder. Check `git status`: `virtualenv/` should no longer be listed.
2. **Run the unit tests:** `pytest tests/test_collisions_ingestion.py -q`. They should all pass, and they don't need Spark or AWS.
3. **Deploy:** `bash infra/aws/deploy.sh` (about 2–3 min).
   - If it fails on `iam:CreateRole` or `iam:PassRole`: your login can't create roles. Send your admin `infra/aws/cloudformation/autopulse-compute.yaml` and README step 9.
4. **Try it** (README step 9, "Try it"): invoke the Lambda for a small window, then start the collisions Glue job. Check that new files appear in `bronze/collisions/` and `silver/collisions/`.
5. Send me any errors. For Glue, the useful part is in CloudWatch → `/aws-glue/jobs/error`.

## 6. Next steps

| When | Work |
|---|---|
| Next | **EC2 host:** launch template, Docker Compose with Kafka, simulator, Spark streaming and Airflow, connected through Systems Manager |
| Then | **Airflow DAGs:** daily collisions run (Lambda → Glue), telemetry Silver every 15 min (Glue). Snowpipe and the Snowflake task take it from there |
| Then | Snowflake setup run-through (`snowflake/README.md`), retire the Spark Gold jobs, dashboard |

---

## Addendum (2026-10-20, 15:40): deploy without CloudFormation

**Finding:** your AWS user (`Julio.Diaz@techconsulting.tech`, account 866934333672, which is a shared training account) is **denied CloudFormation**. It **can create IAM roles** (checked by creating and deleting `AutoPulseProbeRole`), and it can read Lambda, Glue, EC2 and CloudWatch.

**Change:** deployment now calls the AWS APIs directly with boto3 instead of using CloudFormation. It creates the same resources and permissions.

| File | Change |
|---|---|
| `infra/aws/deploy.py` | **New.** Creates or updates the 3 IAM roles (plus optional Snowflake role) and the EC2 instance profile, uploads the versioned code zip and Glue scripts, creates or updates the Lambda (it retries while a new role becomes usable, and sets 14-day log retention), and creates or updates both Glue jobs. Safe to re-run |
| `infra/aws/deploy.sh` | Now a 3-line wrapper that runs `deploy.py` |
| `infra/aws/iam/lambda-trust-policy.json`, `glue-trust-policy.json`, `ec2-host-trust-policy.json` | **New.** Who may use each role |
| `infra/aws/iam/lambda-bronze-collisions-policy.json` | **New.** The Lambda can write only `bronze/collisions/` |
| `infra/aws/iam/ec2-host-orchestration-policy.json` | **New.** The EC2 host can invoke the Lambda, run and monitor `autopulse-*` Glue jobs, and read their logs |
| `infra/aws/iam/autopulse-pipeline-s3-policy.json` | Reused as-is for the Glue and EC2 roles' bucket access |
| `infra/aws/cloudformation/autopulse-compute.yaml` | Header added: not used in this account, kept for reference. Delete it if you prefer |
| `infra/aws/README.md` | Step 9 rewritten for `deploy.py`. Step 4b points to it |
| `README.md` | Tech stack and run command updated |
| `requirements.txt` | Added `boto3` |

**Verification:** I ran `deploy.py` against a fake AWS twice. The first run on an empty account created every resource, including a retry while the new role settled. The second run updated everything in place and added the Snowflake role. Every policy placeholder (bucket, account, region) was filled and every document is valid JSON. Running against your real account is still the real test.

**Shared account note:** the account holds many other people's projects. Everything AutoPulse creates is named `AutoPulse*` or `autopulse-*`, and the EC2 and Glue permissions only cover those names.

**Run it:**
```bash
pip install boto3
python infra/aws/deploy.py
```
