
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
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
