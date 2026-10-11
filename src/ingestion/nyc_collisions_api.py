
from pathlib import Path
import logging

import requests
from pyspark.sql import SparkSession

API_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "collisions"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    filename=str(LOG_DIR / "ingestion.log"),
    filemode="w"
)


def ingest_collisions(limit: int = 1000) -> None:
    spark = (
        SparkSession.builder
        .appName("AutoPulse-NYC-Collisions-Ingestion")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "12")
        .getOrCreate()
    )

    try:
        response = requests.get(
            API_URL,
            params={"$limit": limit, "$order": "collision_id"},
            timeout=30,
        )
        response.raise_for_status()

        records = response.json()

        if not records:
            logging.info("The API returned no records.")
            return

        df = spark.createDataFrame(records)

        df = df.withColumn(
            "ingested_at",
            __import__("pyspark.sql.functions", fromlist=["current_timestamp"])
            .current_timestamp(),
        )

        df.repartition(12).write.mode("overwrite").parquet(str(BRONZE_DIR))

        logging.info(f"Retrieved {df.count()} collision records.")
        logging.info(f"Bronze data saved to: {BRONZE_DIR}")

    finally:
        spark.stop()


if __name__ == "__main__":
    ingest_collisions()
