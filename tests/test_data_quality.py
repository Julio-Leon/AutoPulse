
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pathlib import Path
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "collisions"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("AutoPulse-Data-Quality-Tests")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    yield session

    session.stop()


@pytest.fixture(scope="session")
def silver_df(spark):
    if not SILVER_DIR.exists():
        pytest.fail(
            f"Silver data not found at {SILVER_DIR}. "
            "Run the Bronze-to-Silver pipeline first."
        )

    return spark.read.parquet(str(SILVER_DIR))


def test_silver_dataset_is_not_empty(silver_df):
    assert silver_df.count() > 0


def test_collision_ids_are_not_null(silver_df):
    invalid_ids = silver_df.filter(
        F.col("collision_id").isNull()
    ).count()

    assert invalid_ids == 0


def test_collision_ids_are_unique(silver_df):
    duplicate_ids = (
        silver_df.groupBy("collision_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    assert duplicate_ids == 0


@pytest.mark.parametrize(
    "column",
    [
        "number_of_persons_injured",
        "number_of_persons_killed",
        "number_of_pedestrians_injured",
        "number_of_pedestrians_killed",
        "number_of_cyclist_injured",
        "number_of_cyclist_killed",
        "number_of_motorist_injured",
        "number_of_motorist_killed",
    ],
)
def test_injury_and_fatality_counts_are_nonnegative(silver_df, column):
    if column not in silver_df.columns:
        pytest.fail(f"Required column is missing: {column}")

    invalid_count = silver_df.filter(
        F.col(column) < 0
    ).count()

    assert invalid_count == 0


def test_valid_coordinates_are_within_geographic_bounds(silver_df):
    invalid_coordinates = silver_df.filter(
        (F.col("coordinates_valid") == True)
        & (
            F.col("latitude").isNull()
            | F.col("longitude").isNull()
            | ~F.col("latitude").between(-90, 90)
            | ~F.col("longitude").between(-180, 180)
        )
    ).count()

    assert invalid_coordinates == 0


@pytest.mark.parametrize(
    "dataset_name, required_columns",
    [
        (
            "hourly_collisions",
            [
                "crash_hour",
                "collision_count",
                "persons_injured",
                "persons_killed",
            ],
        ),
        (
            "borough_summary",
            [
                "borough",
                "collision_count",
                "persons_injured",
                "persons_killed",
            ],
        ),
        (
            "geographic_hotspots",
            [
                "latitude_cell",
                "longitude_cell",
                "collision_count",
                "persons_injured",
                "persons_killed",
            ],
        ),
    ],
)
def test_gold_dataset_exists_and_has_required_columns(spark, dataset_name, required_columns):
    dataset_path = GOLD_DIR / dataset_name

    assert dataset_path.exists(), (
        f"Gold dataset does not exist: {dataset_path}"
    )

    df = spark.read.parquet(str(dataset_path))

    assert df.count() > 0, (
        f"Gold dataset is empty: {dataset_name}"
    )

    missing_columns = set(required_columns) - set(df.columns)

    assert not missing_columns, (
        f"{dataset_name} is missing columns: {missing_columns}"
    )


@pytest.mark.parametrize(
    "dataset_name",
    [
        "hourly_collisions",
        "borough_summary",
        "geographic_hotspots",
    ],
)
def test_gold_collision_counts_are_positive(spark, dataset_name):
    dataset_path = GOLD_DIR / dataset_name

    assert dataset_path.exists(), (
        f"Gold dataset does not exist: {dataset_path}"
    )

    df = spark.read.parquet(str(dataset_path))

    invalid_counts = df.filter(
        F.col("collision_count").isNull()
        | (F.col("collision_count") <= 0)
    ).count()

    assert invalid_counts == 0



# --------------------------------------------------
# Vehicle telemetry data quality tests
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

VEHICLE_SILVER_DIR = (
    PROJECT_ROOT / "data" / "silver" / "vehicle_telemetry"
)

VEHICLE_QUARANTINE_DIR = (
    PROJECT_ROOT / "data" / "quarantine" / "vehicle_telemetry"
)


@pytest.fixture(scope="session")
def vehicle_silver_df(spark):
    assert VEHICLE_SILVER_DIR.exists(), (
        f"Vehicle Silver directory not found: {VEHICLE_SILVER_DIR}"
    )
    return spark.read.parquet(str(VEHICLE_SILVER_DIR))


def test_vehicle_silver_dataset_is_not_empty(vehicle_silver_df):
    assert vehicle_silver_df.count() > 0


def test_vehicle_event_ids_are_not_null(vehicle_silver_df):
    assert vehicle_silver_df.filter(
        F.col("event_id").isNull()
        | (F.trim(F.col("event_id")) == "")
    ).count() == 0


def test_vehicle_event_ids_are_unique(vehicle_silver_df):
    duplicates = (
        vehicle_silver_df
        .groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
    )
    assert duplicates.count() == 0


def test_vehicle_ids_are_not_null(vehicle_silver_df):
    assert vehicle_silver_df.filter(
        F.col("vehicle_id").isNull()
        | (F.trim(F.col("vehicle_id")) == "")
    ).count() == 0


def test_vehicle_coordinates_are_valid(vehicle_silver_df):
    invalid_coordinates = vehicle_silver_df.filter(
        F.col("latitude").isNull()
        | F.col("longitude").isNull()
        | (F.col("latitude") < -90)
        | (F.col("latitude") > 90)
        | (F.col("longitude") < -180)
        | (F.col("longitude") > 180)
    )
    assert invalid_coordinates.count() == 0


def test_vehicle_speed_is_nonnegative(vehicle_silver_df):
    assert vehicle_silver_df.filter(
        F.col("speed_mph").isNull()
        | (F.col("speed_mph") < 0)
    ).count() == 0


def test_vehicle_odometer_is_nonnegative(vehicle_silver_df):
    assert vehicle_silver_df.filter(
        F.col("odometer_miles").isNull()
        | (F.col("odometer_miles") < 0)
    ).count() == 0


def test_vehicle_fuel_level_is_between_zero_and_100(vehicle_silver_df):
    assert vehicle_silver_df.filter(
        F.col("fuel_level_pct").isNull()
        | (F.col("fuel_level_pct") < 0)
        | (F.col("fuel_level_pct") > 100)
    ).count() == 0


def test_vehicle_engine_status_is_valid(vehicle_silver_df):
    allowed_statuses = ["running", "idling", "off"]

    assert vehicle_silver_df.filter(
        F.col("engine_status").isNull()
        | (~F.col("engine_status").isin(*allowed_statuses))
    ).count() == 0


def test_quarantine_records_have_validation_reasons(spark):
    if not VEHICLE_QUARANTINE_DIR.exists():
        pytest.skip("No vehicle quarantine dataset was created")

    quarantine_df = spark.read.parquet(str(VEHICLE_QUARANTINE_DIR))

    if quarantine_df.count() == 0:
        pytest.skip("Vehicle quarantine dataset is empty")

    assert "validation_error" in quarantine_df.columns

    assert quarantine_df.filter(
        F.col("validation_error").isNull()
        | (F.trim(F.col("validation_error")) == "")
    ).count() == 0
