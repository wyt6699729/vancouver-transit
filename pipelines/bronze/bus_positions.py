# ═══════════════════════════════════════════════════════════════
# ASSET DECLARATION
# ═══════════════════════════════════════════════════════════════
# Name:         {catalog}.transit_bronze.bus_positions
# Layer:        Bronze
# Grain:        One row per vehicle position update
#
# Depends on:   TransLink GTFS-RT API (api)
# Feeds into:   transit_silver.bus_sensor_readings
#
# Transformations applied:
#   TODO (Lab 3) — document each column as {column} → {type} or DERIVED: {formula}
#
# Data Quality Rules:
#   TODO (Lab 3) — FAIL: @dp.expect_or_fail, DROP: @dp.expect_or_drop, WARN: @dp.expect
#
# PII columns:  vehicle_id
# SLA:          Within 5 minutes of upstream load
# Owner:        Etienne Wang (etienne.wang@slalom.com)
# Updated:      2026-09-01
# ═══════════════════════════════════════════════════════════════

"""Source: TransLink GTFS-RT vehicle positions API."""

# TODO (Lab 3): declare the @dp.table with @dp.expect checks per CLAUDE.md.
