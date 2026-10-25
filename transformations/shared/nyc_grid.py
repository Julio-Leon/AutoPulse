"""
AutoPulse - New York City location grid, shared by the Gold Glue jobs.

The city is divided into cells of about 500 m x 500 m. Every collision and
every telemetry event gets the cell_id of the cell it falls in, so vehicles and
accidents can be compared by place: "which cells have the most crashes?",
"how often does VEH-003 drive through those cells?".

    cell_id "R042-C067" = row 42 (counted north from the city's southern edge),
                          column 67 (counted east from its western edge)

The grid covers New York City's bounding box (the same box Silver uses for
coordinates_valid): 96 rows x 100 columns = 9,600 cells. Points outside the box
(or without coordinates) get no cell: cell_id is null.

Used by:
    transformations/gold_dimensions/build_gold_dimensions_glue.py   dim_location (every cell)
    transformations/collisions/collisions_silver_to_gold_glue.py    fact_collision.cell_id
    transformations/vehicle_telemetry/telemetry_silver_to_gold_glue.py  fact_vehicle_telemetry.cell_id

In Glue this file is attached to those jobs with the job parameter
    --extra-py-files  s3://autopulse-julio-leon/deployment/autopulse/transformations/shared/nyc_grid.py
and imported with `import nyc_grid`.
"""

import math

from pyspark.sql import functions as F

# --------------------------------------------------
# Grid definition
# --------------------------------------------------

# New York City bounding box.
SOUTH_LATITUDE, NORTH_LATITUDE = 40.49, 40.92
WEST_LONGITUDE, EAST_LONGITUDE = -74.27, -73.68

# ~500 m in degrees: 1 degree of latitude is ~111 km everywhere; 1 degree of
# longitude is ~84 km at New York's latitude (it shrinks towards the poles).
CELL_HEIGHT_DEGREES = 0.0045
CELL_WIDTH_DEGREES = 0.0059

# round(...) removes floating-point noise (0.59 / 0.0059 = 100.00000000000001).
NUMBER_OF_ROWS = math.ceil(round((NORTH_LATITUDE - SOUTH_LATITUDE) / CELL_HEIGHT_DEGREES, 6))       # 96
NUMBER_OF_COLUMNS = math.ceil(round((EAST_LONGITUDE - WEST_LONGITUDE) / CELL_WIDTH_DEGREES, 6))    # 100


# --------------------------------------------------
# Spark column expressions
# --------------------------------------------------

def format_cell_id(row, column):
    """Row 42, column 67 -> "R042-C067"."""
    return F.format_string("R%03d-C%03d", row.cast("int"), column.cast("int"))


def cell_id_for(latitude_column, longitude_column):
    """
    The cell_id of each row's point, or null when the point is missing or
    outside New York City. Arguments are column names, e.g.
        df.withColumn("cell_id", cell_id_for("latitude", "longitude"))
    """
    latitude, longitude = F.col(latitude_column), F.col(longitude_column)
    inside_city = latitude.between(SOUTH_LATITUDE, NORTH_LATITUDE) & longitude.between(WEST_LONGITUDE, EAST_LONGITUDE)

    # least(...) keeps a point exactly on the north/east edge in the last row/column.
    row = F.least(F.floor((latitude - SOUTH_LATITUDE) / CELL_HEIGHT_DEGREES), F.lit(NUMBER_OF_ROWS - 1))
    column = F.least(F.floor((longitude - WEST_LONGITUDE) / CELL_WIDTH_DEGREES), F.lit(NUMBER_OF_COLUMNS - 1))

    return F.when(inside_city, format_cell_id(row, column))


def all_cells(spark):
    """Every cell of the grid, one row each: id, position and corners (for dim_location)."""
    row = F.floor(F.col("id") / NUMBER_OF_COLUMNS)
    column = F.col("id") % NUMBER_OF_COLUMNS
    return spark.range(NUMBER_OF_ROWS * NUMBER_OF_COLUMNS).select(
        format_cell_id(row, column).alias("cell_id"),
        row.cast("int").alias("cell_row"),
        column.cast("int").alias("cell_column"),
        F.round(SOUTH_LATITUDE + row * CELL_HEIGHT_DEGREES, 6).alias("south_latitude"),
        F.round(SOUTH_LATITUDE + (row + 1) * CELL_HEIGHT_DEGREES, 6).alias("north_latitude"),
        F.round(WEST_LONGITUDE + column * CELL_WIDTH_DEGREES, 6).alias("west_longitude"),
        F.round(WEST_LONGITUDE + (column + 1) * CELL_WIDTH_DEGREES, 6).alias("east_longitude"),
        F.round(SOUTH_LATITUDE + (row + 0.5) * CELL_HEIGHT_DEGREES, 6).alias("center_latitude"),
        F.round(WEST_LONGITUDE + (column + 0.5) * CELL_WIDTH_DEGREES, 6).alias("center_longitude"),
    )
