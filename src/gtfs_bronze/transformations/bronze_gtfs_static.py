"""Bronze layer: Auto Loader ingestion of the TransLink GTFS Static feed.

The companion `gtfs-download` job task lands each weekly publication under
`<VOLUME_ROOT>/<entity>/snapshot_id=<UTC timestamp>/<entity>.txt`, so every new
publication looks like a new file to Auto Loader and is appended incrementally.

Everything is kept as STRING at bronze. GTFS identifiers are specified as
strings and several TransLink columns would be corrupted by type inference -
`route_short_name` values like "002" lose their leading zeros, and dates such as
"20260608" are not parseable as-is. Casting belongs in silver.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

VOLUME_ROOT = "/Volumes/gtfs/bronze/landing/gtfs_static"

# One streaming table per GTFS file published by TransLink. The first block is
# the GTFS reference spec; the second is TransLink's own extensions.
GTFS_ENTITIES = {
    "agency": "Transit agencies publishing this feed",
    "calendar": "Weekly service patterns with start/end dates",
    "calendar_dates": "Service exceptions (added/removed dates)",
    "routes": "Transit routes",
    "shapes": "Ordered lat/lon points tracing each route shape",
    "stop_times": "Arrival/departure times per trip per stop",
    "stops": "Stops, stations and platforms",
    "transfers": "Permitted transfers between stops or trips",
    "translations": "Field translations for other languages",
    "trips": "Individual vehicle trips on a route",
    "directions": "TransLink extension: direction labels per route",
    "direction_names_exceptions": "TransLink extension: direction name overrides",
    "route_names_exceptions": "TransLink extension: route name overrides",
    "signup_periods": "TransLink extension: schedule sign-up periods",
    "stop_order_exceptions": "TransLink extension: stop ordering overrides",
}


def _define_bronze_table(entity: str, description: str) -> None:
    """Declare one Auto Loader streaming table for a GTFS entity."""

    @dp.table(
        name=entity,
        comment=f"Bronze GTFS Static - {description}. Raw strings as published by TransLink.",
        table_properties={"quality": "bronze"},
    )
    @dp.expect("no_rescued_data", "_rescued_data IS NULL")
    def _bronze_table():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            # No inferColumnTypes: bronze stays all-STRING on purpose (see module docstring).
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.partitionColumns", "snapshot_id")
            .option("header", "true")
            .option("rescuedDataColumn", "_rescued_data")
            .load(f"{VOLUME_ROOT}/{entity}")
            .select(
                "*",
                F.col("_metadata.file_path").alias("_source_file"),
                F.current_timestamp().alias("_ingested_at"),
            )
        )


for _entity, _description in GTFS_ENTITIES.items():
    _define_bronze_table(_entity, _description)
