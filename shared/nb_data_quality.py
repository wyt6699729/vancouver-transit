"""DataQuality class — data quality rules as code, not documentation.

Every @dp.table carries @dp.expect checks. CRITICAL rules stop the pipeline;
WARN rules log and continue.

Owner: Etienne Wang (etienne.wang@slalom.com)
Updated: 2026-09-01
"""

# ANOMALY_RULES as defined in CLAUDE.md. Config lives in a Python file in git,
# never in a config table.
ANOMALY_RULES = {
    "LOW_BATTERY": {
        "condition":   "battery <= 10",
        "description": "Battery level critically low (<=10%)",
        "severity":    "HIGH"
    },
    "TELEMETRY_GAP": {
        "condition":   "gap_seconds > 300",
        "description": "5+ minute gap between readings",
        "severity":    "MEDIUM"
    },
    "INGEST_DELAY": {
        "condition":   "ingest_delay_seconds > 30",
        "description": "Data arriving 30+ seconds late",
        "severity":    "LOW"
    }
}

# TODO (Lab 5): DataQuality class wrapping the rules above.
