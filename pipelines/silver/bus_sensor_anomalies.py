# ═══════════════════════════════════════════════════════════════
# ASSET DECLARATION
# ═══════════════════════════════════════════════════════════════
# Name:         {catalog}.transit_silver.bus_sensor_anomalies
# Layer:        Silver
# Grain:        One row per vehicle_id + anomaly_type
#
# Depends on:   transit_silver.bus_sensor_readings
# Feeds into:   transit_gold.route_performance
#
# Transformations applied:
#   TODO (Lab 5) — document each column as {column} → {type} or DERIVED: {formula}
#
# Data Quality Rules:
#   TODO (Lab 5) — CRITICAL rules stop the pipeline, WARN rules log and continue
#
# PII columns:  vehicle_id
# SLA:          Within 20 minutes of upstream load
# Owner:        Etienne Wang (etienne.wang@slalom.com)
# Updated:      2026-09-01
# ═══════════════════════════════════════════════════════════════

"""Depends on: bus_sensor_readings. Classification driven by ANOMALY_RULES."""

# TODO (Lab 5): apply ANOMALY_RULES (LOW_BATTERY, TELEMETRY_GAP, INGEST_DELAY).
