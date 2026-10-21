"""
AutoPulse - stream vehicle telemetry from Kafka into S3 Bronze.

Spark Structured Streaming reads every event the simulator sends to the Kafka
topic `vehicle.telemetry.raw` and, every 5 minutes, appends them to Bronze as
one Parquet file. (Every 5 minutes rather than every minute: 5x fewer small
files to list and read later, and Silver is only built every 15 minutes anyway.)
Bronze keeps the message exactly as received (raw JSON text) plus Kafka
metadata; parsing and validation happen later in Silver.

Bronze columns:
    raw_payload      the JSON event, unchanged
    kafka_topic      vehicle.telemetry.raw
    kafka_partition  Kafka partition the event was in
    kafka_offset     position within that partition (unique with the partition)
    kafka_timestamp  when Kafka received the event
    ingested_at      when this job wrote it

Progress is stored in a checkpoint, so after a restart the job continues from
the last processed event instead of starting over.

Run from the project root (Kafka and the simulator must be running):
    python ingestion/telemetry_stream/stream_telemetry_to_bronze.py
Stop with Ctrl+C.

AWS access uses your normal AWS login (`aws configure` / AWS_PROFILE), or the
instance role when running on EC2.

Settings (environment variables, all optional):
    KAFKA_BOOTSTRAP_SERVERS   default localhost:29092
    BRONZE_PATH               default s3://autopulse-julio-leon/bronze/vehicle_telemetry
    CHECKPOINT_PATH           default s3://autopulse-julio-leon/_checkpoints/vehicle_telemetry_bronze
    TRIGGER_INTERVAL          default "5 minutes"
    AWS_REGION                default us-east-1
"""

import os

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --------------------------------------------------
# Settings
# --------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = "vehicle.telemetry.raw"

BRONZE_PATH = os.getenv("BRONZE_PATH", "s3://autopulse-julio-leon/bronze/vehicle_telemetry")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "s3://autopulse-julio-leon/_checkpoints/vehicle_telemetry_bronze")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "5 minutes")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


# --------------------------------------------------
# Spark session
# --------------------------------------------------

def create_spark_session():
    """
    Spark with two add-on libraries, downloaded automatically on first run:
      - the Kafka connector (to read the topic)
      - hadoop-aws (to write to S3)
    Their versions must match your PySpark version.
    """
    spark_version = pyspark.__version__
    if spark_version.startswith("3."):
        scala_version, hadoop_aws_version = "2.12", "3.3.4"
    else:  # Spark 4.x
        scala_version, hadoop_aws_version = "2.13", "3.4.1"

    packages = [
        f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}",
        f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version}",
    ]

    # AWS SDK v1 for hadoop-aws 3.3.x, v2 for 3.4.x: same lookup order as the AWS CLI
    # (environment variables, ~/.aws profile, then the EC2 instance role).
    credentials_provider = (
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        if hadoop_aws_version.startswith("3.3")
        else "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
    )

    spark = (
        SparkSession.builder
        .appName("AutoPulse-Stream-Telemetry-To-Bronze")
        .master("local[*]")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        # Standard Parquet timestamps (readable by Glue and Snowflake).
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        # Serve "s3://" paths with the hadoop-aws (S3A) connector. We use s3:// everywhere
        # (here and in Glue): Spark's streaming file log stores full paths, and a reader
        # using a different scheme (s3a:// vs s3://) silently finds no files.
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", credentials_provider)
        .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# --------------------------------------------------
# Streaming job
# --------------------------------------------------

def main():
    spark = create_spark_session()

    print(f"Kafka      : {KAFKA_BOOTSTRAP_SERVERS} / {KAFKA_TOPIC}")
    print(f"Bronze     : {BRONZE_PATH}")
    print(f"Checkpoint : {CHECKPOINT_PATH}")
    print(f"Trigger    : every {TRIGGER_INTERVAL}")

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")   # first run: read everything Kafka still has
        .option("failOnDataLoss", "false")       # don't stop if Kafka already deleted old events
        .load()
    )

    # Keep the message as-is; Kafka's value column is bytes, so turn it into text.
    bronze_stream = kafka_stream.select(
        F.col("value").cast("string").alias("raw_payload"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
    )

    query = (
        bronze_stream
        .coalesce(1)                              # one file per trigger instead of one per partition
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print("Streaming started. Press Ctrl+C to stop.")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping...")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
