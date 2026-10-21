"""
AutoPulse | Deploy Lambda + Glue jobs + IAM roles to AWS (boto3, no CloudFormation).

Run from WSL, from the project root, with your venv active:

    pip install boto3          # once
    python infra/aws/deploy.py

What it does (safe to re-run: missing things are created, existing ones updated)
  1. Zips src/ -> build/autopulse_src-<version>.zip and uploads it, plus the
     Glue job scripts, to s3://<bucket>/artifacts/
  2. IAM roles (policies live in infra/aws/iam/*.json):
       AutoPulseLambdaRole    Lambda -> write bronze/collisions/ only
       AutoPulseGlueJobRole   Glue   -> read/write the bucket
       AutoPulseEc2HostRole   EC2    -> bucket, invoke the Lambda, run the Glue jobs,
                                        Systems Manager shell (+ instance profile)
       AutoPulseSnowflakeRole Snowflake read-only (only with --snowflake-* options)
  3. Lambda  autopulse-collisions-ingestion   (Python 3.12 + AWS SDK for pandas layer)
  4. Glue    autopulse-collisions-silver, autopulse-vehicle-telemetry-silver (Glue 5.0)
  5. EC2 bundle (src/, airflow/dags/, deploy/ec2/) -> s3://<bucket>/artifacts/ec2/,
     used by the EC2 host at boot and by `python infra/aws/ec2_host.py update`

Options
  --bucket BUCKET                       default autopulse-julio-leon
  --region REGION                       default us-east-1
  --snowflake-iam-user-arn ARN          } from DESC INTEGRATION AUTOPULSE_S3_INT;
  --snowflake-external-id ID            } creates/updates AutoPulseSnowflakeRole
  --nyc-app-token TOKEN                 optional Socrata token for the Lambda
                                        (or env NYC_OPEN_DATA_APP_TOKEN)
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import tarfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
IAM_DIR = PROJECT_ROOT / "infra" / "aws" / "iam"
GLUE_JOBS_DIR = PROJECT_ROOT / "glue" / "jobs"
BUILD_DIR = PROJECT_ROOT / "build"

LAMBDA_ROLE = "AutoPulseLambdaRole"
GLUE_ROLE = "AutoPulseGlueJobRole"
EC2_ROLE = "AutoPulseEc2HostRole"
EC2_INSTANCE_PROFILE = "AutoPulseEc2HostProfile"
SNOWFLAKE_ROLE = "AutoPulseSnowflakeRole"

LAMBDA_FUNCTION = "autopulse-collisions-ingestion"
LAMBDA_HANDLER = "src.lambda_functions.collisions_ingestion.handler"
LAMBDA_LOG_GROUP = f"/aws/lambda/{LAMBDA_FUNCTION}"
# AWS-managed layer with pyarrow for Python 3.12 (aws-sdk-pandas.readthedocs.io/en/stable/layers.html)
PANDAS_LAYER = "arn:aws:lambda:{region}:336392948345:layer:AWSSDKPandas-Python312:31"

# Glue job name -> script in glue/jobs/
GLUE_JOBS = {
    "autopulse-collisions-silver": (
        "collisions_silver_job",
        "Collisions Bronze -> Silver (src/batch/transform_collisions.py)",
    ),
    "autopulse-vehicle-telemetry-silver": (
        "vehicle_telemetry_silver_job",
        "Vehicle telemetry Bronze -> Silver + quarantine (src/batch/transform_vehicle_telemetry.py)",
    ),
}

MANAGED = "arn:aws:iam::aws:policy/"


def log(message: str) -> None:
    print(f"==> {message}", flush=True)


def render_policy(file_name: str, **values: str) -> str:
    """Load infra/aws/iam/<file_name> and fill placeholders such as BUCKET_NAME."""
    text = (IAM_DIR / file_name).read_text(encoding="utf-8")
    for placeholder, value in values.items():
        text = text.replace(placeholder, value)
    return json.dumps(json.loads(text))   # validates the JSON


# Files shipped to the EC2 host (paths relative to the project root).
EC2_BUNDLE_GLOBS = ["src/**/*.py", "airflow/dags/*.py", "deploy/ec2/*"]


def package_ec2_bundle(version: str) -> Path:
    """tar.gz of everything the EC2 host needs to build and run its services."""
    BUILD_DIR.mkdir(exist_ok=True)
    bundle_path = BUILD_DIR / f"autopulse-ec2-bundle-{version}.tar.gz"
    files = sorted({
        path for pattern in EC2_BUNDLE_GLOBS for path in PROJECT_ROOT.glob(pattern)
        if path.is_file() and "__pycache__" not in path.parts and path.name != ".env"
    })
    with tarfile.open(bundle_path, "w:gz") as tar:
        for path in files:
            data = path.read_bytes()
            if path.suffix == ".sh":
                data = data.replace(b"\r\n", b"\n")   # Windows checkouts: keep scripts runnable
            info = tarfile.TarInfo(path.relative_to(PROJECT_ROOT).as_posix())
            info.size = len(data)
            info.mode = 0o755 if path.suffix == ".sh" else 0o644
            info.mtime = int(time.time())
            tar.addfile(info, io.BytesIO(data))
    return bundle_path


def package_source(version: str) -> Path:
    """Zip every .py file under src/ (no caches)."""
    BUILD_DIR.mkdir(exist_ok=True)
    zip_path = BUILD_DIR / f"autopulse_src-{version}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted((PROJECT_ROOT / "src").rglob("*.py")):
            if "__pycache__" not in path.parts:
                zf.write(path, path.relative_to(PROJECT_ROOT).as_posix())
    return zip_path


class Deployer:
    def __init__(self, session, bucket: str, region: str, version: str):
        self.bucket = bucket
        self.region = region
        self.version = version
        self.iam = session.client("iam")
        self.s3 = session.client("s3", region_name=region)
        self.lam = session.client("lambda", region_name=region)
        self.glue = session.client("glue", region_name=region)
        self.logs = session.client("logs", region_name=region)
        self.account_id = session.client("sts").get_caller_identity()["Account"]

    # ---------------------------------------------------------------- IAM
    def ensure_role(self, name: str, trust_policy: str, description: str,
                    managed: list[str] = (), inline: dict[str, str] | None = None) -> str:
        try:
            arn = self.iam.get_role(RoleName=name)["Role"]["Arn"]
            self.iam.update_assume_role_policy(RoleName=name, PolicyDocument=trust_policy)
            log(f"IAM role {name}: exists, updated")
        except self.iam.exceptions.NoSuchEntityException:
            arn = self.iam.create_role(
                RoleName=name, AssumeRolePolicyDocument=trust_policy, Description=description
            )["Role"]["Arn"]
            log(f"IAM role {name}: created")

        for policy_arn in managed:
            self.iam.attach_role_policy(RoleName=name, PolicyArn=policy_arn)
        for policy_name, document in (inline or {}).items():
            self.iam.put_role_policy(RoleName=name, PolicyName=policy_name, PolicyDocument=document)
        return arn

    def ensure_instance_profile(self, profile: str, role: str) -> None:
        try:
            roles = self.iam.get_instance_profile(InstanceProfileName=profile)["InstanceProfile"]["Roles"]
        except self.iam.exceptions.NoSuchEntityException:
            self.iam.create_instance_profile(InstanceProfileName=profile)
            roles = []
        if role not in {r["RoleName"] for r in roles}:
            self.iam.add_role_to_instance_profile(InstanceProfileName=profile, RoleName=role)
        log(f"Instance profile {profile}: ready (role {role})")

    def deploy_roles(self, snowflake_user_arn: str | None, snowflake_external_id: str | None) -> dict:
        values = {
            "BUCKET_NAME": self.bucket,
            "AWS_ACCOUNT_ID": self.account_id,
            "AWS_REGION_NAME": self.region,
        }
        s3_read_write = render_policy("autopulse-pipeline-s3-policy.json", **values)

        roles = {
            "lambda": self.ensure_role(
                LAMBDA_ROLE, render_policy("lambda-trust-policy.json"),
                "AutoPulse collisions-ingestion Lambda",
                managed=[MANAGED + "service-role/AWSLambdaBasicExecutionRole"],
                inline={"AutoPulseBronzeCollisionsWrite": render_policy("lambda-bronze-collisions-policy.json", **values)},
            ),
            "glue": self.ensure_role(
                GLUE_ROLE, render_policy("glue-trust-policy.json"),
                "AutoPulse Glue jobs",
                managed=[MANAGED + "service-role/AWSGlueServiceRole"],
                inline={"AutoPulseDataLakeReadWrite": s3_read_write},
            ),
            "ec2": self.ensure_role(
                EC2_ROLE, render_policy("ec2-host-trust-policy.json"),
                "AutoPulse EC2 host (Kafka, simulator, streaming, Airflow)",
                managed=[MANAGED + "AmazonSSMManagedInstanceCore"],
                inline={
                    "AutoPulseDataLakeReadWrite": s3_read_write,
                    "AutoPulseOrchestration": render_policy("ec2-host-orchestration-policy.json", **values),
                },
            ),
        }
        self.ensure_instance_profile(EC2_INSTANCE_PROFILE, EC2_ROLE)

        if snowflake_user_arn:
            if not snowflake_external_id:
                raise SystemExit("--snowflake-external-id is required with --snowflake-iam-user-arn")
            roles["snowflake"] = self.ensure_role(
                SNOWFLAKE_ROLE,
                render_policy(
                    "autopulse-snowflake-trust-policy.json",
                    STORAGE_AWS_IAM_USER_ARN=snowflake_user_arn,
                    STORAGE_AWS_EXTERNAL_ID=snowflake_external_id,
                ),
                "Snowflake storage integration AUTOPULSE_S3_INT (read-only)",
                inline={"AutoPulseSnowflakeS3Read": render_policy("autopulse-snowflake-s3-read-policy.json", **values)},
            )
        return roles

    # ---------------------------------------------------------------- S3
    def upload_code(self) -> tuple[str, dict[str, str]]:
        zip_path = package_source(self.version)
        zip_key = f"artifacts/{zip_path.name}"
        self.s3.upload_file(str(zip_path), self.bucket, zip_key)
        log(f"Uploaded s3://{self.bucket}/{zip_key} ({zip_path.stat().st_size // 1024} KB)")

        script_keys = {}
        for job_name, (script, _) in GLUE_JOBS.items():
            key = f"artifacts/glue/{script}-{self.version}.py"
            self.s3.upload_file(str(GLUE_JOBS_DIR / f"{script}.py"), self.bucket, key)
            script_keys[job_name] = key
        log(f"Uploaded {len(script_keys)} Glue scripts to s3://{self.bucket}/artifacts/glue/")

        bundle_path = package_ec2_bundle(self.version)
        for key in (f"artifacts/ec2/{bundle_path.name}", "artifacts/ec2/autopulse-ec2-bundle.tar.gz"):
            self.s3.upload_file(str(bundle_path), self.bucket, key)
        log(f"Uploaded EC2 bundle s3://{self.bucket}/artifacts/ec2/autopulse-ec2-bundle.tar.gz "
            f"({bundle_path.stat().st_size // 1024} KB)")
        return zip_key, script_keys

    # ---------------------------------------------------------------- Lambda
    def ensure_lambda(self, role_arn: str, zip_key: str, app_token: str | None) -> None:
        try:
            self.logs.create_log_group(logGroupName=LAMBDA_LOG_GROUP)
        except self.logs.exceptions.ResourceAlreadyExistsException:
            pass
        self.logs.put_retention_policy(logGroupName=LAMBDA_LOG_GROUP, retentionInDays=14)

        settings = dict(
            Role=role_arn,
            Handler=LAMBDA_HANDLER,
            Runtime="python3.12",
            Timeout=900,
            MemorySize=1024,
            Layers=[PANDAS_LAYER.format(region=self.region)],
            Environment={"Variables": {
                "AUTOPULSE_RUNTIME": "lambda",
                "DATA_ROOT": f"s3://{self.bucket}",
                **({"NYC_OPEN_DATA_APP_TOKEN": app_token} if app_token else {}),
            }},
            Description="NYC collisions API -> S3 Bronze (partitioned by crash date)",
        )

        try:
            self.lam.get_function(FunctionName=LAMBDA_FUNCTION)
            exists = True
        except self.lam.exceptions.ResourceNotFoundException:
            exists = False

        if exists:
            self.lam.update_function_code(FunctionName=LAMBDA_FUNCTION, S3Bucket=self.bucket, S3Key=zip_key)
            self.lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_FUNCTION)
            self.lam.update_function_configuration(FunctionName=LAMBDA_FUNCTION, **settings)
            self.lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_FUNCTION)
            log(f"Lambda {LAMBDA_FUNCTION}: updated")
            return

        # A brand-new IAM role takes a few seconds before Lambda can use it.
        for attempt in range(1, 13):
            try:
                self.lam.create_function(
                    FunctionName=LAMBDA_FUNCTION,
                    Code={"S3Bucket": self.bucket, "S3Key": zip_key},
                    **settings,
                )
                break
            except self.lam.exceptions.InvalidParameterValueException as error:
                if "role" not in str(error).lower() or attempt == 12:
                    raise
                log("Waiting for the new IAM role to become usable by Lambda...")
                time.sleep(10)
        self.lam.get_waiter("function_active").wait(FunctionName=LAMBDA_FUNCTION)
        log(f"Lambda {LAMBDA_FUNCTION}: created")

    # ---------------------------------------------------------------- Glue
    def ensure_glue_job(self, name: str, description: str, role_arn: str,
                        script_key: str, zip_key: str) -> None:
        definition = dict(
            Description=description,
            Role=role_arn,
            Command={
                "Name": "glueetl",
                "ScriptLocation": f"s3://{self.bucket}/{script_key}",
                "PythonVersion": "3",
            },
            DefaultArguments={
                "--DATA_ROOT": f"s3://{self.bucket}",
                "--CHECKPOINT_ROOT": f"s3://{self.bucket}/_checkpoints",
                "--extra-py-files": f"s3://{self.bucket}/{zip_key}",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics": "true",
                "--job-language": "python",
            },
            GlueVersion="5.0",
            WorkerType="G.1X",
            NumberOfWorkers=2,
            Timeout=30,
            MaxRetries=0,
            ExecutionProperty={"MaxConcurrentRuns": 1},
        )
        try:
            self.glue.get_job(JobName=name)
            self.glue.update_job(JobName=name, JobUpdate=definition)
            log(f"Glue job {name}: updated")
        except self.glue.exceptions.EntityNotFoundException:
            self.glue.create_job(Name=name, **definition)
            log(f"Glue job {name}: created")


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy AutoPulse Lambda + Glue + IAM roles.")
    parser.add_argument("--bucket", default=os.getenv("BUCKET", "autopulse-julio-leon"))
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"))
    parser.add_argument("--snowflake-iam-user-arn", default=os.getenv("SNOWFLAKE_IAM_USER_ARN"))
    parser.add_argument("--snowflake-external-id", default=os.getenv("SNOWFLAKE_EXTERNAL_ID"))
    parser.add_argument("--nyc-app-token", default=os.getenv("NYC_OPEN_DATA_APP_TOKEN"))
    return parser.parse_args(argv)


def main(argv=None, session=None) -> None:
    args = parse_args(argv)
    version = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    if session is None:
        try:
            import boto3
        except ImportError:
            sys.exit("boto3 is not installed. Run:  pip install boto3")
        session = boto3.Session(region_name=args.region)

    deployer = Deployer(session, args.bucket, args.region, version)
    log(f"AutoPulse deploy | account={deployer.account_id} bucket={args.bucket} "
        f"region={args.region} version={version}")

    roles = deployer.deploy_roles(args.snowflake_iam_user_arn, args.snowflake_external_id)
    zip_key, script_keys = deployer.upload_code()
    deployer.ensure_lambda(roles["lambda"], zip_key, args.nyc_app_token)
    for job_name, (_, description) in GLUE_JOBS.items():
        deployer.ensure_glue_job(job_name, description, roles["glue"], script_keys[job_name], zip_key)

    log("Done.")
    print("\nRoles:")
    for key, arn in roles.items():
        print(f"  {key:<10} {arn}")
    print(f"\nInstance profile for the EC2 host: {EC2_INSTANCE_PROFILE}")
    print("\nNext:")
    print("  python infra/aws/ec2_host.py launch     # first time: create the EC2 host (Kafka, streaming, Airflow)")
    print("  python infra/aws/ec2_host.py update     # later: ship this code to the running host")


if __name__ == "__main__":
    main()
