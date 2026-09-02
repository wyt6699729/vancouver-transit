# ═══════════════════════════════════════════════════════════════
# ASSET DECLARATION
# ═══════════════════════════════════════════════════════════════
# Name:         {catalog}.transit_gold.route_performance
# Layer:        Gold
# Grain:        One row per route_id per date
#
# Depends on:   transit_silver.bus_sensor_readings, transit_silver.bus_sensor_anomalies
# Feeds into:   (none)
#
# Transformations applied:
#   TODO (Lab 6) — document each column as {column} → {type} or DERIVED: {formula}
#
# Data Quality Rules:
#   TODO (Lab 6) — CRITICAL rules stop the pipeline, WARN rules log and continue
#
# PII columns:  None
# SLA:          Within 60 minutes of upstream load
# Owner:        Etienne Wang (etienne.wang@slalom.com)
# Updated:      2026-09-01
# ═══════════════════════════════════════════════════════════════

"""Depends on: bus_sensor_readings, bus_sensor_anomalies."""

# TODO (Lab 6): on_time_rate, avg_delay_secs, anomaly_count, total_readings.
