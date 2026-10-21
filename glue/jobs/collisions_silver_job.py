"""
AWS Glue job: Collisions Bronze -> Silver.

Thin wrapper that runs the same PySpark code used locally
(src/batch/transform_collisions.py). The project's src/ package is attached to the
job as a zip (--extra-py-files), built and uploaded by infra/aws/deploy.sh.

Job parameters (set by infra/aws/deploy.py):
    --DATA_ROOT   s3://<bucket>
"""

import os
import sys

from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["DATA_ROOT"])

# Configure AutoPulse before its modules are imported (config reads env vars).
os.environ["DATA_ROOT"] = args["DATA_ROOT"]
os.environ["AUTOPULSE_RUNTIME"] = "glue"

from src.batch import transform_collisions  # noqa: E402

transform_collisions.main()
