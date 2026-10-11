
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "collisions"
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "collisions"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "transform_collisions.log"

# Ensure the log directory exists.
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Overwrite this pipeline's log on every run.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
    force=True,
)

logger = logging.getLogger("autopulse.silver")

spark = None


def main():
    global spark

    logger.info("Starting Bronze-to-Silver pipeline.")
    logger.info("Bronze input: %s", BRONZE_DIR)
    logger.info("Silver output: %s", SILVER_DIR)
    logger.info("Log file: %s", LOG_FILE)

    try:
        if not BRONZE_DIR.exists():
            raise FileNotFoundError(
                f"Bronze directory does not exist: {BRONZE_DIR}"
            )

        spark = (
            SparkSession.builder
            .appName("AutoPulse-Bronze-to-Silver")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "12")
            .getOrCreate()
        )

        logger.info("Spark session created successfully.")

        # Read Bronze data.
        logger.info("Reading Bronze Parquet data.")
        df = spark.read.parquet(str(BRONZE_DIR))

        bronze_count = df.count()
        logger.info("Bronze record count: %d", bronze_count)
        logger.info("Bronze column count: %d", len(df.columns))
        logger.info("Bronze schema:\n%s", df._jdf.schema().treeString())

        # Standardize data types.
        logger.info("Casting collision ID, date, and coordinate columns.")

        df = (
            df
            .withColumn("collision_id", F.col("collision_id").cast("long"))
            .withColumn("crash_date", F.to_timestamp("crash_date"))
            .withColumn("crash_time", F.trim(F.col("crash_time")))
            .withColumn("latitude", F.col("latitude").cast("double"))
            .withColumn("longitude", F.col("longitude").cast("double"))
        )

        count_columns = [
            "number_of_persons_injured",
            "number_of_persons_killed",
            "number_of_pedestrians_injured",
            "number_of_pedestrians_killed",
            "number_of_cyclist_injured",
            "number_of_cyclist_killed",
            "number_of_motorist_injured",
            "number_of_motorist_killed",
        ]

        for column in count_columns:
            if column in df.columns:
                df = df.withColumn(
                    column,
                    F.col(column).cast("integer"),
                )

        logger.info("Data type conversions completed.")

        # Remove records without a usable ID.
        before_id_filter = df.count()
        df = df.filter(F.col("collision_id").isNotNull())
        after_id_filter = df.count()

        logger.info(
            "Records removed for missing/invalid collision ID: %d",
            before_id_filter - after_id_filter,
        )

        # Deduplicate by collision ID.
        before_dedup = df.count()
        df = df.dropDuplicates(["collision_id"])
        after_dedup = df.count()

        logger.info(
            "Duplicate records removed: %d",
            before_dedup - after_dedup,
        )

        duplicate_ids = (
            df.groupBy("collision_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        logger.info("Duplicate collision IDs remaining: %d", duplicate_ids)

        if duplicate_ids > 0:
            raise ValueError(f"Silver data contains {duplicate_ids} duplicate collision IDs.")

        # Validate coordinates but retain records without valid coordinates.
        logger.info("Validating geographic coordinates.")

        df = df.withColumn(
            "coordinates_valid",
            F.when(
                F.col("latitude").between(-90, 90)
                & F.col("longitude").between(-180, 180),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )

        invalid_coordinates = df.filter(
            ~F.col("coordinates_valid")
        ).count()

        logger.info(
            "Records with missing or invalid coordinates: %d",
            invalid_coordinates,
        )

        # Write Silver output.
        SILVER_DIR.parent.mkdir(parents=True, exist_ok=True)

        logger.info("Writing Silver Parquet data.")
        (
            df.repartition(20).write
            .mode("overwrite")
            .parquet(str(SILVER_DIR))
        )

        silver_count = df.count()

        logger.info("Silver record count: %d", silver_count)
        logger.info("Silver output written to: %s", SILVER_DIR)
        logger.info("Silver schema:\n%s", df._jdf.schema().treeString())
        logger.info("Bronze-to-Silver pipeline completed successfully.")

        print(f"Bronze records: {bronze_count}")
        print(f"Silver records: {silver_count}")
        print(f"Silver output: {SILVER_DIR}")
        print(f"Log file: {LOG_FILE}")

    except Exception:
        logger.exception("Bronze-to-Silver pipeline failed.")
        raise

    finally:
        if spark is not None:
            logger.info("Stopping Spark session.")
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
