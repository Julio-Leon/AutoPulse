"""
AWS Glue job: Vehicle telemetry Bronze -> Silver (+ quarantine).

Thin wrapper that runs the same PySpark code used locally
(src/batch/transform_vehicle_telemetry.py). The project's src/ package is attached to the
job as a zip (--extra-py-files), built and uploaded by infra/aws/deploy.sh.

Job parameters (set by infra/aws/deploy.py):
    --DATA_ROOT        s3://<bucket>
    --CHECKPOINT_ROOT  s3://<bucket>/_checkpoints   (remembers which Bronze files were processed)
"""

import os
import sys

from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["DATA_ROOT", "CHECKPOINT_ROOT"])

# Configure AutoPulse before its modules are imported (config reads env vars).
os.environ["DATA_ROOT"] = args["DATA_ROOT"]
os.environ["CHECKPOINT_ROOT"] = args["CHECKPOINT_ROOT"]
os.environ["AUTOPULSE_RUNTIME"] = "glue"

from src.batch import transform_vehicle_telemetry  # noqa: E402

transform_vehicle_telemetry.main()
