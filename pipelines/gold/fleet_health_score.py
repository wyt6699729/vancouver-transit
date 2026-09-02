# ═══════════════════════════════════════════════════════════════
# ASSET DECLARATION
# ═══════════════════════════════════════════════════════════════
# Name:         {catalog}.transit_gold.fleet_health_score
# Layer:        Gold
# Grain:        One row per vehicle_id per date
#
# Depends on:   transit_silver.bus_sensor_readings, transit_silver.bus_sensor_anomalies
# Feeds into:   (none)
#
# Transformations applied:
#   TODO (Lab 6) — document each column as {column} → {type} or DERIVED: {formula}
#
# Data Quality Rules:
#   TODO (Lab 6) — FAIL: @dp.expect_or_fail, DROP: @dp.expect_or_drop, WARN: @dp.expect
#
# PII columns:  TBD
# SLA:          Within 60 minutes of upstream load
# Owner:        Etienne Wang (etienne.wang@slalom.com)
# Updated:      2026-09-01
# ═══════════════════════════════════════════════════════════════

"""Depends on: bus_sensor_readings, bus_sensor_anomalies."""

# TODO (Lab 6): vehicle-level health scoring. No lineage registry entry exists
# for this table in CLAUDE.md yet — add one when the grain is settled.
