# ═══════════════════════════════════════════════════════════════
# ASSET DECLARATION
# ═══════════════════════════════════════════════════════════════
# Name:         {catalog}.transit_silver.bus_sensor_readings
# Layer:        Silver
# Grain:        One row per vehicle position update (typed + enriched)
#
# Depends on:   transit_bronze.bus_positions
# Feeds into:   transit_silver.bus_sensor_anomalies, transit_gold.route_performance
#
# Transformations applied:
#   TODO (Lab 4) — document each column as {column} → {type} or DERIVED: {formula}
#
# Data Quality Rules:
#   TODO (Lab 4) — FAIL: @dp.expect_or_fail, DROP: @dp.expect_or_drop, WARN: @dp.expect
#
# PII columns:  vehicle_id
# SLA:          Within 15 minutes of upstream load
# Owner:        Etienne Wang (etienne.wang@slalom.com)
# Updated:      2026-09-01
# ═══════════════════════════════════════════════════════════════

"""Depends on: bus_positions."""

# TODO (Lab 4): declare the @dp.table; business logic belongs in
# shared/nb_transformations.transform_bus_positions, not here.
