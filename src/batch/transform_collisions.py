
import sys
from pathlib import Path

if __package__ in (None, ""):
    # Allow `python src/batch/transform_collisions.py` as well as `python -m ...`.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common import config

BRONZE_DIR = config.BRONZE_COLLISIONS
SILVER_DIR = config.SILVER_COLLISIONS
LOG_FILE = config.LOG_DIR / "transform_collisions.log"

logger = config.setup_logging(LOG_FILE.name, "autopulse.silver")

# Columns every Silver collision row must have. The NYC API omits fields that
# have no value, so a given ingestion window can be missing some of them
# entirely (e.g. number_of_persons_killed). Missing ones are added as null so
# downstream jobs and Snowflake always see the same schema.
EXPECTED_COLUMNS = [
    "collision_id",
    "crash_date",
    "crash_time",
    "borough",
    "zip_code",
    "latitude",
    "longitude",
    "on_street_name",
    "cross_street_name",
    "off_street_name",
    "number_of_persons_injured",
    "number_of_persons_killed",
    "number_of_pedestrians_injured",
    "number_of_pedestrians_killed",
    "number_of_cyclist_injured",
    "number_of_cyclist_killed",
    "number_of_motorist_injured",
    "number_of_motorist_killed",
    "contributing_factor_vehicle_1",
    "contributing_factor_vehicle_2",
    "vehicle_type_code1",
    "vehicle_type_code2",
]


def main():
    spark = None

    logger.info("Starting Bronze-to-Silver pipeline. %s", config.describe())
    logger.info("Bronze input: %s", BRONZE_DIR)
    logger.info("Silver output: %s", SILVER_DIR)
    logger.info("Log file: %s", LOG_FILE)

    try:
        spark = config.build_spark("AutoPulse-Bronze-to-Silver", shuffle_partitions=12)

        if not config.path_exists(spark, BRONZE_DIR):
            raise FileNotFoundError(
                f"Bronze directory does not exist: {BRONZE_DIR}"
            )

        logger.info("Spark session created successfully.")

        # Read Bronze data. mergeSchema tolerates columns that appear only in
        # some ingestion windows.
        logger.info("Reading Bronze Parquet data.")
        df = spark.read.option("mergeSchema", "true").parquet(BRONZE_DIR)

        bronze_count = df.count()
        logger.info("Bronze record count: %d", bronze_count)
        logger.info("Bronze column count: %d", len(df.columns))
        logger.info("Bronze schema:\n%s", df._jdf.schema().treeString())

        # Make sure every expected column exists (see EXPECTED_COLUMNS).
        missing_columns = [c for c in EXPECTED_COLUMNS if c not in df.columns]
        for column in missing_columns:
            df = df.withColumn(column, F.lit(None).cast("string"))

        if missing_columns:
            logger.warning(
                "Columns absent from Bronze, added as null: %s",
                ", ".join(missing_columns),
            )

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

        # Deduplicate by collision ID. The city revises records after the fact,
        # and the same window may be re-ingested, so keep the newest copy.
        before_dedup = df.count()
        latest_first = Window.partitionBy("collision_id").orderBy(
            F.col("ingested_at").desc_nulls_last()
        )
        df = (
            df.withColumn("_row_rank", F.row_number().over(latest_first))
            .filter(F.col("_row_rank") == 1)
            .drop("_row_rank")
        )
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

        # Standardize borough. Keep every collision: rows without a borough
        # (often highway/bridge crashes) become "UNKNOWN" so they still count
        # in totals and hotspots; borough-level reports exclude them.
        logger.info("Standardizing borough values.")

        df = (
            df.withColumn("borough", F.upper(F.trim(F.col("borough"))))
            .withColumn(
                "borough_known",
                F.col("borough").isNotNull() & (F.col("borough") != ""),
            )
            .withColumn(
                "borough",
                F.when(F.col("borough_known"), F.col("borough"))
                .otherwise(F.lit("UNKNOWN")),
            )
        )

        unknown_boroughs = df.filter(~F.col("borough_known")).count()

        logger.info(
            "Records without a borough (set to UNKNOWN): %d",
            unknown_boroughs,
        )

        # Write Silver output.
        logger.info("Writing Silver Parquet data.")
        (
            df.repartition(20).write
            .mode("overwrite")
            .parquet(SILVER_DIR)
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
