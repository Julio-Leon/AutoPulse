# S3 bucket `autopulse-julio-leon`: lifecycle rules

`lifecycle_rules.json` tells S3 to delete old files automatically:

| Rule | Folder | Deleted after |
|---|---|---|
| Vehicle telemetry, Bronze | `bronze/vehicle_telemetry/` | 30 days |
| Vehicle telemetry, Silver | `silver/vehicle_telemetry/` | 30 days |
| Vehicle telemetry, quarantine | `quarantine/vehicle_telemetry/` | 30 days |
| Vehicle telemetry, Gold fact | `gold/fact_vehicle_telemetry/` | 30 days |
| Unfinished uploads (whole bucket) | everything | 7 days |

Only vehicle telemetry events (which include the vehicles' locations) expire.
Kept: `gold/dim_vehicle/` (one row per vehicle), all collisions data, the
streaming checkpoints (`_checkpoints/`) and the deployed code (`deployment/`).

The age is counted from when each file was written. Telemetry files are written
within minutes of their events, so "30 days old" means "events older than 30
days". S3 runs the rules once a day, so a file can stay up to a day or two
past 30 days before it is removed.

Apply (or update) the rules from the project root:

    aws s3api put-bucket-lifecycle-configuration --bucket autopulse-julio-leon --lifecycle-configuration file://infrastructure/s3_bucket/lifecycle_rules.json

This replaces all of the bucket's lifecycle rules with the ones in the file.
Check them: S3 console -> bucket -> Management -> Lifecycle rules, or

    aws s3api get-bucket-lifecycle-configuration --bucket autopulse-julio-leon
