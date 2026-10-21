"""
Central configuration for AutoPulse.

Every pipeline reads its paths, Kafka settings, and tuning values from here,
so the same code runs locally (WSL) and in the cloud (EC2 + S3) by changing
environment variables only. Values can be set in the shell or in a `.env`
file at the project root (see `.env.example`).

Storage
-------
DATA_ROOT decides where the Bronze / Silver / Gold layers live:

    DATA_ROOT unset                      -> <project>/data          (local)
    DATA_ROOT=s3://my-bucket             -> s3://my-bucket           (S3)

Paths always use `s3://`, on every runtime. Self-managed Spark (WSL, EC2)
maps `s3://` to the hadoop-aws S3A connector. Glue uses `s3://` natively.
Using the same scheme everywhere matters: Spark streaming's file logs
(`_spark_metadata`) store full file paths, and a reader using a different
scheme (s3a vs s3) silently finds no files.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load `.env` if python-dotenv is installed. Real environment variables win.
try:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env", override=False)
except ImportError:  # pragma: no cover - dotenv is optional
    pass


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value.strip() if value and value.strip() else default


# Where the code is running:
#   local  - WSL / EC2 with self-managed Spark (default)
#   glue   - AWS Glue job (Spark is provided; S3 via s3:// natively)
#   lambda - AWS Lambda (no Spark)
RUNTIME = _env("AUTOPULSE_RUNTIME", "local").lower()


def _normalize_root(root: str) -> str:
    root = root.rstrip("/")
    # One scheme everywhere (see the module docstring).
    if root.startswith("s3a://"):
        root = "s3://" + root[len("s3a://"):]
    return root


# --------------------------------------------------
# Storage
# --------------------------------------------------

DATA_ROOT = _normalize_root(_env("DATA_ROOT", str(PROJECT_ROOT / "data")))
CHECKPOINT_ROOT = _normalize_root(
    _env("CHECKPOINT_ROOT", str(PROJECT_ROOT / "checkpoints"))
)

# Logs and simulator state always stay on the machine running the job.
LOG_DIR = PROJECT_ROOT / "logs"
SIMULATOR_STATE_DIR = PROJECT_ROOT / "data" / "raw"

AWS_REGION = _env("AWS_REGION", "us-east-1")


def is_s3(path: str | None = None) -> bool:
    """True when the given path (default: DATA_ROOT) is on S3."""
    return (path or DATA_ROOT).startswith(("s3a://", "s3://"))


def split_s3_uri(uri: str) -> tuple[str, str]:
    """'s3a://bucket/a/b' -> ('bucket', 'a/b')."""
    if not is_s3(uri):
        raise ValueError(f"Not an S3 path: {uri}")
    bucket, _, key = uri.split("://", 1)[1].partition("/")
    return bucket, key.strip("/")


def lake_path(*parts: str) -> str:
    """Build a data-lake path, e.g. lake_path("bronze", "collisions")."""
    return "/".join([DATA_ROOT, *[p.strip("/") for p in parts]])


def checkpoint_path(name: str) -> str:
    return "/".join([CHECKPOINT_ROOT, name.strip("/")])


# Data-lake locations used by the pipelines.
BRONZE_COLLISIONS = lake_path("bronze", "collisions")
SILVER_COLLISIONS = lake_path("silver", "collisions")
GOLD_ROOT = lake_path("gold")

BRONZE_VEHICLE_TELEMETRY = lake_path("bronze", "vehicle_telemetry")
SILVER_VEHICLE_TELEMETRY = lake_path("silver", "vehicle_telemetry")
QUARANTINE_VEHICLE_TELEMETRY = lake_path("quarantine", "vehicle_telemetry")
GOLD_VEHICLE_FLEET_SUMMARY = lake_path("gold", "vehicle_fleet_summary")


# --------------------------------------------------
# Kafka / streaming
# --------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
VEHICLE_TELEMETRY_TOPIC = _env("VEHICLE_TELEMETRY_TOPIC", "vehicle.telemetry.raw")
TRAFFIC_INCIDENTS_TOPIC = _env("TRAFFIC_INCIDENTS_TOPIC", "traffic.incidents.raw")

# How often Spark Structured Streaming writes a micro-batch to Bronze.
# Longer intervals mean fewer, larger Parquet files (better for S3/Snowpipe).
STREAM_TRIGGER_INTERVAL = _env("STREAM_TRIGGER_INTERVAL", "1 minute")

SIMULATOR_NUMBER_OF_VEHICLES = int(_env("SIMULATOR_NUMBER_OF_VEHICLES", "10"))
SIMULATOR_PUBLISH_INTERVAL_SECONDS = float(
    _env("SIMULATOR_PUBLISH_INTERVAL_SECONDS", "5")
)


# --------------------------------------------------
# NYC Open Data
# --------------------------------------------------

NYC_COLLISIONS_API_URL = _env(
    "NYC_COLLISIONS_API_URL",
    "https://data.cityofnewyork.us/resource/h9gi-nx95.json",
)
# Optional Socrata app token; raises API rate limits when set.
NYC_OPEN_DATA_APP_TOKEN = os.getenv("NYC_OPEN_DATA_APP_TOKEN") or None


# --------------------------------------------------
# Fleet business rules
# --------------------------------------------------

MAINTENANCE_INTERVAL_MILES = int(_env("MAINTENANCE_INTERVAL_MILES", "5000"))
# A vehicle is flagged "service_due" within this many miles of its next interval.
MAINTENANCE_DUE_WINDOW_MILES = int(_env("MAINTENANCE_DUE_WINDOW_MILES", "250"))


# --------------------------------------------------
# Spark
# --------------------------------------------------

SPARK_MASTER = _env("SPARK_MASTER", "local[*]")


def _spark_versions() -> tuple[str, str, str]:
    """Return (spark_version, scala_binary_version, hadoop_aws_version)."""
    import pyspark

    spark_version = pyspark.__version__.split("+")[0]
    major = int(spark_version.split(".")[0])

    # Spark 4.x ships Scala 2.13 + Hadoop 3.4.x; Spark 3.5.x ships Scala 2.12 + Hadoop 3.3.4.
    scala = "2.13" if major >= 4 else "2.12"
    default_hadoop_aws = "3.4.1" if major >= 4 else "3.3.4"
    hadoop_aws = _env("HADOOP_AWS_VERSION", default_hadoop_aws)
    return spark_version, scala, hadoop_aws


def _aws_credentials_provider(hadoop_aws_version: str) -> str:
    """AWS SDK default credential chain class for the hadoop-aws version in use."""
    if hadoop_aws_version.startswith("3.3"):
        # hadoop-aws 3.3.x uses AWS SDK v1.
        return "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    # hadoop-aws 3.4+ uses AWS SDK v2.
    return "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"


def build_spark(app_name: str, shuffle_partitions: int = 4, kafka: bool = False):
    """
    Create a SparkSession configured for local disk or S3.

    - kafka=True adds the Spark Kafka connector (needed by the streaming job).
    - When DATA_ROOT or CHECKPOINT_ROOT is on S3, the hadoop-aws connector is
      added. Credentials come from the AWS SDK default chain: environment
      variables, the ~/.aws profile written by `aws configure`, or the EC2
      instance role (preferred in AWS).
    """
    from pyspark.sql import SparkSession

    if RUNTIME == "glue":
        return _build_glue_spark(app_name, shuffle_partitions)

    spark_version, scala, hadoop_aws = _spark_versions()

    packages = []
    if kafka:
        packages.append(
            f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{spark_version}"
        )
    if is_s3(DATA_ROOT) or is_s3(CHECKPOINT_ROOT):
        packages.append(f"org.apache.hadoop:hadoop-aws:{hadoop_aws}")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        # Standard Parquet timestamps (instead of legacy INT96) so Snowflake
        # reads them reliably.
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    )

    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    if is_s3(DATA_ROOT) or is_s3(CHECKPOINT_ROOT):
        builder = (
            builder
            # Serve s3:// paths with the S3A connector (same scheme as Glue).
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A")
            .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            # Same credential lookup as the AWS CLI: env vars, ~/.aws profile
            # (from `aws configure`), then the EC2 instance role. Hadoop's own
            # default chain skips the ~/.aws profile.
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", _aws_credentials_provider(hadoop_aws))
            # Avoid _SUCCESS marker files being picked up by Snowpipe.
            .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _build_glue_spark(app_name: str, shuffle_partitions: int):
    """
    In AWS Glue, Spark, S3 access (s3://) and credentials (the Glue job role)
    are already set up; only apply the AutoPulse session settings.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def path_exists(spark, path: str) -> bool:
    """Check whether a local or S3 path exists, using Spark's Hadoop FS."""
    jvm = spark._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(hadoop_path)


# --------------------------------------------------
# Logging
# --------------------------------------------------

def setup_logging(log_file_name: str, logger_name: str) -> logging.Logger:
    """
    Log to stdout, and locally also to logs/<log_file_name> (overwritten each
    run). In Glue and Lambda, stdout goes to CloudWatch Logs and the code's
    folder is read-only, so no log file is written.
    """
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    if RUNTIME == "local":
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        handlers.insert(
            0, logging.FileHandler(LOG_DIR / log_file_name, mode="w", encoding="utf-8")
        )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
        force=True,
    )
    return logging.getLogger(logger_name)


def describe() -> str:
    """One-line summary of the active configuration, for logs."""
    return (
        f"RUNTIME={RUNTIME} | DATA_ROOT={DATA_ROOT} | CHECKPOINT_ROOT={CHECKPOINT_ROOT} | "
        f"KAFKA={KAFKA_BOOTSTRAP_SERVERS} | SPARK_MASTER={SPARK_MASTER}"
    )
