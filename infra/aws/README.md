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

This one policy is used in two places:

**a) Local development (WSL) — IAM user**
1. IAM → Users → Create user `autopulse-dev` (no console access).
2. Attach `AutoPulsePipelineS3Access`.
3. Security credentials → Create access key → "Local code".
4. In WSL: `aws configure` (or put the keys in `.env`, which is git-ignored). Never commit keys.

**b) EC2 host — IAM role (set up later)**
1. IAM → Roles → Create role → trusted entity **EC2**.
2. Attach `AutoPulsePipelineS3Access`; name it `AutoPulseEC2Role`.
3. Attach the role to the EC2 instance. No access keys are needed on EC2.

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
