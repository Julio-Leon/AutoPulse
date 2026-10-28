# AutoPulse IAM policies

One folder per IAM role. Each role has two JSON files:

- **`trust_policy.json`**: *who* may use the role (Glue, EC2, or Snowflake).
  When you create a role in the console and pick the AWS service, the console
  writes this for you. The file is here so the repo shows it.
- **`permissions_policy.json`**: *what* the role may do. In the console:
  role → **Add permissions → Create inline policy → JSON** → paste the file.

Every permission is limited to the `autopulse-julio-leon` bucket, and within
it to the folders that part of the pipeline actually uses.

| Folder | IAM role name | Used by | Also attach (AWS-managed policy) | Phase |
|---|---|---|---|---|
| `glue_jobs/` | `AutoPulseGlueRole` | All `autopulse-*` Glue jobs: collisions ingestion, Silver and Gold jobs, Gold dimensions | `AWSGlueServiceRole` (Glue basics: logs, scripts, bookmarks) | 3–4, Gold |
| `ec2_streaming_host/` | `AutoPulseEc2Role` | EC2 streaming host: Kafka, simulator, stream to Bronze | `AmazonSSMManagedInstanceCore` (browser shell, no SSH) | 5 |
| `snowflake_s3_access/` | `AutoPulseSnowflakeRole` | Snowflake storage integration (reads Gold) | none | 6 |
| `ec2_airflow_host/` | `AutoPulseAirflowRole` | EC2 Airflow host: runs and watches the Glue jobs, validates the data lake | `AmazonSSMManagedInstanceCore` (browser shell, no SSH) | Orchestration |

## What each role can do

**Glue jobs**: ingestion writes and replaces files under `bronze/collisions/`;
Silver jobs read `bronze/` and write/replace files under `silver/` and
`quarantine/`; Gold jobs read `silver/`, write/replace files under `gold/`, and
read the shared helper `transformations/shared/nyc_grid.py` from `deployment/`.
(Glue jobs reach the NYC Open Data API over the internet without any extra
network setup.)

**EC2 streaming host**: read its code from `deployment/`; write
`bronze/vehicle_telemetry/` and the streaming checkpoints in `_checkpoints/`. It
may also *check* (not read data from) the parent folder names `bronze`,
`bronze/vehicle_telemetry` and `_checkpoints`: Spark's S3 connector looks those
paths up before writing and fails with "Access Denied" if it can't.

**Snowflake**: read-only access to `gold/`.

**Airflow host**: start, watch and stop the `autopulse-*` Glue jobs; read its code
from `deployment/`; list the bucket and read (only the footers of) the Parquet
files in `bronze/`, `silver/`, `quarantine/` and `gold/` to validate them. It
cannot write or delete anything in S3.

## Placeholders

`snowflake_s3_access/trust_policy.json` contains two values that Snowflake
generates (`STORAGE_AWS_IAM_USER_ARN`, `STORAGE_AWS_EXTERNAL_ID`). We fill them
in during Phase 6. All other files are ready to paste as they are
(account 866934333672, region us-east-1).
