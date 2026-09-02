"""Pure transform functions. All business logic lives here.

No spark.read, no .write, no side effects. Every function takes a DataFrame and
returns a DataFrame, so it is testable in isolation and platform-agnostic.

Every function needs a docstring carrying: business context, Args with source
column types, Returns with grain, and full column lineage.

Owner: Etienne Wang (etienne.wang@slalom.com)
Updated: 2026-09-01
"""

# TODO (Lab 4): transform_bus_positions(df) -> DataFrame
# TODO (Lab 5): anomaly classification helpers driven by ANOMALY_RULES
# TODO (Lab 6): route performance and fleet health aggregations
