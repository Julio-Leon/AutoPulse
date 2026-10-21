# AutoPulse — AWS Setup

Region: **us-east-1 (N. Virginia)**. Replace `BUCKET_NAME` everywhere with your bucket name.

These steps cover the S3 data lake and the access the pipelines need. The
Snowflake role is created in the Snowflake step (it needs values that
Snowflake generates), and the EC2 host is set up after that.

## 1. Create the S3 bucket

AWS Console → S3 → **Create bucket**

| Setting | Value |
| --- | --- |
| Bucket type | General purpose |
| Name | `autopulse-data-lake-<unique-suffix>` (e.g. your initials + digits) |
| Region | us-east-1 |
| Object Ownership | ACLs disabled |
| Block all public access | **On** |
| Bucket versioning | Disabled (keeps storage costs down) |
| Default encryption | SSE-S3 |

## 2. Data lake layout

Spark creates these prefixes automatically on first write; no need to create folders by hand.

```
s3://BUCKET_NAME/
├── bronze/
│   ├── collisions/            partitioned by crash_date_part=YYYY-MM-DD
│   └── vehicle_telemetry/     one file per streaming micro-batch
├── silver/
│   ├── collisions/            ← Snowpipe source
│   └── vehicle_telemetry/     ← Snowpipe source
├── quarantine/
│   └── vehicle_telemetry/
├── gold/
│   ├── hourly_collisions/
│   ├── borough_summary/
│   ├── geographic_hotspots/
│   └── vehicle_fleet_summary/
└── _checkpoints/              (only if CHECKPOINT_ROOT points at S3)
```

## 3. Lifecycle rule (cost control)

S3 → bucket → Management → **Create lifecycle rule**
- Name: `abort-incomplete-uploads`
- Apply to all objects
- Action: *Delete expired object delete markers or incomplete multipart uploads* → after **1 day**

## 4. IAM policy for the pipelines

IAM → Policies → **Create policy** → JSON → paste
[`iam/autopulse-pipeline-s3-policy.json`](iam/autopulse-pipeline-s3-policy.json)
(with your bucket name) → name it `AutoPulsePipelineS3Access`.

This policy is for the EC2 role in (b). For local development you use your
own AWS login, so no extra IAM user is needed.

**a) Local development (WSL) — your own AWS login**

Use whichever way you already sign in to AWS. Pick the matching option:

*Option 1: you sign in through an AWS access portal (IAM Identity Center / SSO)*
```bash
aws configure sso
#   SSO session name:  autopulse
#   SSO start URL:     your access-portal URL (https://....awsapps.com/start)
#   SSO region:        the region shown on the portal (often us-east-1)
#   Then pick the account and role in the browser, CLI default region us-east-1,
#   and name the profile:  autopulse
aws sso login --profile autopulse          # repeat when the session expires (usually 8-12 h)
```
Then add `AWS_PROFILE=autopulse` to `.env`, so Spark uses the same login.

*Option 2: you have an IAM user and may create access keys for yourself*
1. Console → your name (top right) → **Security credentials** → **Create access key** → "Command Line Interface".
2. In WSL run `aws configure`, paste the key and secret, and set region `us-east-1`.

**Check (both options):**
```bash
aws sts get-caller-identity              # shows who you are signed in as
aws s3 ls s3://BUCKET_NAME               # must not say AccessDenied
```
Your login needs the S3 permissions listed in `AutoPulsePipelineS3Access`.
You probably already have them if you created the bucket. If you get
`AccessDenied`, ask the account administrator to grant them to your
user or permission set.

Never put keys in code or commit them. `.env` and `~/.aws/` stay on your machine.

**b) EC2 host — IAM role**

Created for you by the deploy script in step 9 (`AutoPulseEc2HostRole`,
instance profile `AutoPulseEc2HostProfile`). You don't need to create it by hand.

## 5. Snowflake read policy (used in the Snowflake step)

[`iam/autopulse-snowflake-s3-read-policy.json`](iam/autopulse-snowflake-s3-read-policy.json)
gives Snowflake read-only access to `silver/` and `gold/`. Create it now as
`AutoPulseSnowflakeS3Read`; we'll attach it to a role whose trust policy uses
the external ID that Snowflake's storage integration generates.

## 6. Point the pipelines at S3

In `.env`:

```
DATA_ROOT=s3://BUCKET_NAME
AWS_REGION=us-east-1
```

Then run the pipelines as usual. Quick check that the files landed:

```bash
aws s3 ls s3://BUCKET_NAME/ --recursive | head
```

---

The steps below connect Snowflake. Run them together with the scripts in
[`snowflake/`](../../snowflake/README.md); the order is shown there.

## 7. Snowflake role (lets Snowflake read the bucket)

> **If you can't create IAM roles or policies either**, send your AWS
> administrator these three files and ask them to create the role
> `AutoPulseSnowflakeRole` with the read policy attached:
> `iam/autopulse-snowflake-s3-read-policy.json`,
> `iam/autopulse-snowflake-trust-policy.json`, and this section. They will
> need to paste in the two values from step (b) at the end. Snowflake can
> only read a private bucket through a role like this, so there's no
> workaround that avoids it.

**a) Create the role (before `snowflake/02_storage_integration.sql`)**
1. Note your 12-digit AWS account ID (top-right menu in the AWS console).
2. IAM → Roles → **Create role** → Trusted entity type **AWS account** → **This account**.
3. Tick **Require external ID** and enter `0000` (temporary placeholder).
4. Attach the `AutoPulseSnowflakeS3Read` policy (step 5).
5. Name it `AutoPulseSnowflakeRole` → Create.
6. Open the role and copy its **ARN** (`arn:aws:iam::<account-id>:role/AutoPulseSnowflakeRole`).
   Paste it into `snowflake/02_storage_integration.sql` and run that script.

**b) Copy two values from Snowflake**

The last statement of script 02 (`DESC INTEGRATION AUTOPULSE_S3_INT`) shows:
- `STORAGE_AWS_IAM_USER_ARN`
- `STORAGE_AWS_EXTERNAL_ID`

**c) Update the trust policy**

IAM → Roles → `AutoPulseSnowflakeRole` → Trust relationships → **Edit trust policy**.
Paste [`iam/autopulse-snowflake-trust-policy.json`](iam/autopulse-snowflake-trust-policy.json),
replacing the two placeholders with the values from (b). Save.

Check it: `LIST @AUTOPULSE.RAW.S3_SILVER;` in Snowflake (part of script 03)
should list your Parquet files. Changes can take a minute to apply.

## 8. S3 event notification (triggers Snowpipe)

After `snowflake/04_snowpipe.sql`, copy the `notification_channel` value
from `SHOW PIPES` (an SQS ARN like `arn:aws:sqs:us-east-1:...:sf-snowpipe-...`).
Both pipes show the same value.

S3 → bucket → **Properties** → Event notifications → **Create event notification**

| Setting | Value |
| --- | --- |
| Name | `snowflake-snowpipe-silver` |
| Prefix | `silver/` |
| Suffix | `.parquet` |
| Event types | **All object create events** |
| Destination | **SQS queue** → Enter SQS queue ARN → paste the `notification_channel` value |

From now on, every Parquet file Spark writes under `silver/` is loaded
into Snowflake within about a minute.

## 9. Deploy Lambda + Glue

`infra/aws/deploy.py` (boto3) creates or updates everything below. It's safe to
re-run, and you should re-run it after every code change. CloudFormation isn't
allowed in this AWS account, so the script calls the AWS APIs directly. It uses
the same policies, which are in `iam/*.json`.

| Resource | Name | Purpose |
| --- | --- | --- |
| Lambda | `autopulse-collisions-ingestion` | NYC API → `bronze/collisions/` |
| Glue job | `autopulse-collisions-silver` | Bronze → Silver collisions (same code as `src/batch/transform_collisions.py`) |
| Glue job | `autopulse-vehicle-telemetry-silver` | Bronze → Silver telemetry + quarantine |
| IAM role | `AutoPulseLambdaRole` | Write `bronze/collisions/` only (`iam/lambda-*.json`) |
| IAM role | `AutoPulseGlueJobRole` | Read/write the bucket (`iam/glue-trust-policy.json`, `iam/autopulse-pipeline-s3-policy.json`) |
| IAM role + profile | `AutoPulseEc2HostRole` / `AutoPulseEc2HostProfile` | EC2 host: bucket, invoke the Lambda, run the Glue jobs, Systems Manager shell (`iam/ec2-host-*.json`) |
| IAM role (optional) | `AutoPulseSnowflakeRole` | Snowflake read access (replaces the manual step 7) |

```bash
pip install boto3            # once, in your venv
python infra/aws/deploy.py   # or: bash infra/aws/deploy.sh
```

**Snowflake role through the script (instead of step 7):** run
`snowflake/02_storage_integration.sql` with
`arn:aws:iam::866934333672:role/AutoPulseSnowflakeRole`, then:

```bash
python infra/aws/deploy.py \
  --snowflake-iam-user-arn '<STORAGE_AWS_IAM_USER_ARN>' \
  --snowflake-external-id  '<STORAGE_AWS_EXTERNAL_ID>'
```

**Try it:**

```bash
# Ingest a small window into Bronze
aws lambda invoke --function-name autopulse-collisions-ingestion \
  --cli-binary-format raw-in-base64-out \
  --payload '{"days_back": 7, "max_records": 2000}' /dev/stdout

# Bronze -> Silver in Glue (about 1-2 minutes, mostly startup)
aws glue start-job-run --job-name autopulse-collisions-silver
aws glue get-job-runs --job-name autopulse-collisions-silver \
  --query 'JobRuns[0].[JobRunState,ErrorMessage]'
```

Logs are in CloudWatch: `/aws/lambda/autopulse-collisions-ingestion` and
`/aws-glue/jobs/output` (and `/error`).

**Cost:** Lambda is effectively free at this volume. Each Glue run uses
2 × G.1X workers for 1-2 minutes, about $0.03-0.06 per run.

> `cloudformation/autopulse-compute.yaml` describes the same setup as a
> CloudFormation template. It's **not used** in this account (CloudFormation
> is denied); it's kept for accounts where CloudFormation is available.

## 10. EC2 host + Airflow (automated pipeline)

`infra/aws/ec2_host.py` creates one EC2 instance (`autopulse-host`, t3.large,
Amazon Linux 2023). On first boot it installs Docker, downloads the code bundle
that `deploy.py` uploaded, and starts `deploy/ec2/docker-compose.yml`:

| Service | Does |
| --- | --- |
| `kafka` | Event broker (internal only, 7-day retention) |
| `simulator` | 10-vehicle telemetry producer → Kafka |
| `streaming` | Spark Structured Streaming: Kafka → `s3://…/bronze/vehicle_telemetry/` every minute |
| `airflow-scheduler`, `airflow-webserver`, `postgres` | Airflow 2.10 (LocalExecutor) running the DAGs in `airflow/dags/` |

**Airflow DAGs** (on by default):

| DAG | Schedule (New York time) | Steps |
| --- | --- | --- |
| `autopulse_collisions_daily` | 06:00 daily | Lambda `autopulse-collisions-ingestion` (latest 30 days) → Glue `autopulse-collisions-silver` |
| `autopulse_vehicle_telemetry` | every 15 min | Glue `autopulse-vehicle-telemetry-silver` (only new Bronze files) |

Snowpipe and the Snowflake task then load the new Silver files into the star schema.

**One-time setup**

```bash
# Session Manager plugin for the AWS CLI (for shell / Airflow UI access)
curl -fsSL "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/ubuntu_64bit/session-manager-plugin.deb" -o /tmp/smp.deb
sudo dpkg -i /tmp/smp.deb

python infra/aws/deploy.py           # roles, Lambda, Glue, code bundle
python infra/aws/ec2_host.py launch  # the host (about 5-10 min until services are up)
python infra/aws/ec2_host.py logs    # progress; all services should say "Up"
```

**Everyday commands**

```bash
python infra/aws/ec2_host.py status     # IDs + how to connect
python infra/aws/ec2_host.py password   # Airflow admin password
# Airflow UI: run the port-forward command printed by `status`, then open http://localhost:8080

python infra/aws/deploy.py && python infra/aws/ec2_host.py update   # ship code changes
python infra/aws/ec2_host.py stop       # pause (saves ~$2/day); data is kept
python infra/aws/ec2_host.py start
python infra/aws/ec2_host.py logs --service streaming --lines 80   # one service's log
python infra/aws/ec2_host.py reset-telemetry   # start telemetry over (Bronze/Silver + checkpoints)
```

> **Never point a second streaming job (e.g. your laptop) at the same bucket.**
> Spark's streaming file log in `bronze/vehicle_telemetry/_spark_metadata/`
> numbers batches, and a second writer's batches get skipped as "already
> written". If it happens, run `reset-telemetry`.

The host has **no open inbound ports**. Shell and Airflow UI go through AWS
Systems Manager. Containers use the instance role (`AutoPulseEc2HostRole`),
so no access keys are stored on it.

**Cost (approx.):** t3.large ≈ $2/day running, plus about $2/day of Glue for the
15-minute telemetry runs. Stop the host when you're not demoing.
