# CLAUDE.md — Vancouver Transit Intelligence Platform
# Etienne Wang · Slalom Consulting Vancouver · 2026
# AI Assistant: Claude Code

This file defines ALL engineering standards for the Vancouver Transit
Intelligence Platform. Claude Code reads this automatically on every session.
Every file generated must follow these standards exactly. No exceptions.

---

## PROJECT CONTEXT

**Project:** Vancouver Transit Intelligence Platform
**Purpose:** Real-time transit analytics and AI-powered delay prediction
**GitHub:** github.com/YOUR_USERNAME/vancouver-transit-intelligence
**Owner:** Etienne Wang (etienne.wang@slalom.com)
**Status:** Active development — Shadow project for SA Champion portfolio

**Data Source:** TransLink GTFS-RT API (developer.translink.ca)
**Platform:** Databricks Community Edition → Production Databricks Workspace
**Stack:** Lakeflow · Delta Lake · Unity Catalog · DABs · MLflow · Mosaic AI

---

## UNITY CATALOG DESIGN

```
Catalogs (ONLY these two — never more):
    gtfs_dev    ← development and testing
    gtfs_prod   ← production only

The project prefix is required: this is a SHARED Slalom metastore with 30+
catalogs, and bare `dev` / `prod` would be ambiguous about ownership and
would collide with other teams. Project-prefixed environment catalogs are
the established local convention (mlops_dbx_talk_dev, clinical_agents_dev,
hl_prod). Environment still lives in the CATALOG, never below it.

Schema naming: {domain}_{layer}
    transit_bronze   ← raw ingestion
    transit_silver   ← curated, typed, enriched
    transit_gold     ← aggregated, business-ready

Full path pattern: {catalog}.{domain}_{layer}.{table}

Tables:
    gtfs_dev.transit_bronze.bus_positions        ← raw GTFS-RT positions
    gtfs_dev.transit_silver.bus_sensor_readings  ← typed, enriched
    gtfs_dev.transit_silver.bus_sensor_anomalies ← detected anomalies
    gtfs_dev.transit_gold.route_performance      ← route-level metrics
    gtfs_dev.transit_gold.fleet_health_score     ← vehicle-level health

The GTFS Static reference feed shares this design; it lands as
gtfs_dev.transit_bronze.<entity> (agency, routes, trips, stops, ...) from
the gtfs_ingest job and gtfs_bronze_ingest pipeline.

NEVER: encode environment in schema or table names
WRONG: transit_bronze_dev, bus_positions_prod
RIGHT: gtfs_dev.transit_bronze.bus_positions
```

---

## ANOMALY DETECTION RULES

```python
ANOMALY_RULES = {
    "LOW_BATTERY": {
        "condition":   "battery <= 10",
        "description": "Battery level critically low (≤10%)",
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
```

---

## CORE ENGINEERING PHILOSOPHY

### 1. Define Assets, Not Actions
Every pipeline file = one data asset.
Declare WHAT data should exist. Let the platform handle HOW.

```python
# WRONG — imperative, action-focused
def load_silver():
    df = spark.read.load(source)
    df = transform(df)
    df.write.save(target)

# RIGHT — declarative, asset-focused
@dp.table(name="bus_sensor_readings")
def bus_sensor_readings():
    return transform_bus_positions(
        dp.read_stream("bus_positions")
    )
```

### 2. Pure Functions for All Business Logic
No spark.read, no .write, no side effects inside transform functions.
Testable in isolation. Platform-agnostic. Reusable across projects.

### 3. Mandatory Data Quality as Code
Every table must have @dp.expect checks. No exceptions.
DQ rules are code, not documentation.

### 4. Self-Documenting Systems
Code IS the documentation. A new engineer must understand the
entire system by reading the code — no wiki, no asking anyone.

### 5. Config in Python Files in Git
Never in database tables. Never in Config Tables.
Config Tables make the query optimizer blind:
- Predicate Pushdown FAILS
- Liquid Clustering cannot learn
- CBO statistics are wasted
- No version control, no testing

---

## ASSET DECLARATION BLOCK

**Required at the top of EVERY pipeline file. No exceptions.**

```python
# ═══════════════════════════════════════════════════════════════
# ASSET DECLARATION
# ═══════════════════════════════════════════════════════════════
# Name:         {catalog}.{schema}.{table}
# Layer:        {Bronze | Silver | Gold}
# Grain:        One row per {X}
#
# Depends on:   {upstream tables or file paths}
# Feeds into:   {downstream tables}
#
# Transformations applied:
#   1. {column} → {type} ({description})
#   2. {column} → DERIVED: {formula}
#
# Data Quality Rules:
#   CRITICAL: {rule}  ← pipeline stops on failure
#   WARN:     {rule}  ← logs and continues
#
# PII columns:  {list or None}
# SLA:          Within {N} minutes of upstream load
# Owner:        Etienne Wang (etienne.wang@slalom.com)
# Updated:      {YYYY-MM-DD}
# ═══════════════════════════════════════════════════════════════
```

---

## LAKEFLOW PIPELINE STANDARD

```python
import dlt as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ── BRONZE example ────────────────────────────────────────────
@dp.table(
    name="bus_positions",
    comment="Raw BC Transit vehicle positions from GTFS-RT. One row per update. Unmodified from source.",
    table_properties={
        "layer":   "bronze",
        "domain":  "transit",
        "owner":   "etienne.wang@slalom.com",
        "source":  "TransLink GTFS-RT API"
    },
    partition_by=["_ingest_date"]
)
@dp.expect("vehicle_id is not null",              "critical")
@dp.expect("timestamp is not null",               "critical")
@dp.expect("latitude >= 48.0 and latitude <= 56.0","warn")
@dp.expect("longitude >= -140.0 and longitude <= -114.0", "warn")
def bus_positions():
    """Source: TransLink GTFS-RT vehicle positions API"""
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", schema_path)
            .load(source_path)
    )

# ── SILVER example ────────────────────────────────────────────
@dp.table(
    name="bus_sensor_readings",
    comment="Curated BC Transit telemetry. Typed, enriched. SLA: 15 min after bronze.",
    table_properties={
        "layer":       "silver",
        "domain":      "transit",
        "sla_minutes": "15",
        "owner":       "etienne.wang@slalom.com"
    },
    partition_by=["_ingest_date"],
    liquid_cluster_by=["vehicle_id", "_ingest_date"]
)
@dp.expect("vehicle_id is not null",              "critical")
@dp.expect("timestamp is not null",               "critical")
@dp.expect("battery >= 0",                        "warn")
@dp.expect("ingest_delay_seconds is not null",    "warn")
def bus_sensor_readings():
    """Depends on: bus_positions"""
    return transform_bus_positions(
        dp.read_stream("bus_positions")
    )
```

---

## TRANSFORM FUNCTION STANDARD

```python
def transform_bus_positions(df: DataFrame) -> DataFrame:
    """
    Curates raw GTFS-RT bus positions into typed, enriched sensor readings.

    Business context:
        Converts raw vehicle position updates into typed records
        suitable for anomaly detection and route performance analysis.

    Args:
        df: Raw bronze DataFrame with columns:
            vehicle_id   (str):   Unique vehicle identifier  ⚠️ PII: device identifier
            route_id     (str):   Transit route identifier
            trip_id      (str):   Current trip identifier
            latitude     (str):   GPS latitude as string
            longitude    (str):   GPS longitude as string
            bearing      (str):   Vehicle heading in degrees
            speed        (str):   Speed in km/h as string
            timestamp    (str):   Position timestamp as string
            occupancy    (str):   Passenger occupancy level

    Returns:
        DataFrame. Grain: one row per vehicle position update.

    Column transformations:
        vehicle_id            ← bronze.vehicle_id (unchanged) ⚠️ PII
        route_id              ← bronze.route_id (unchanged)
        trip_id               ← bronze.trip_id (unchanged)
        timestamp             ← bronze.timestamp (cast TIMESTAMP)
        latitude              ← bronze.latitude (cast DOUBLE)
        longitude             ← bronze.longitude (cast DOUBLE)
        bearing               ← bronze.bearing (cast DOUBLE)
        speed_kmh             ← bronze.speed (cast DOUBLE)
        occupancy             ← bronze.occupancy (unchanged)
        ingest_delay_seconds  ← DERIVED: unix(eventTime) - unix(timestamp)
        _ingest_date          ← DERIVED: date(eventTime)
    """
    return (
        df
        .withColumn("timestamp",
            F.to_timestamp("timestamp"))
        .withColumn("latitude",
            F.col("latitude").cast("double"))
        .withColumn("longitude",
            F.col("longitude").cast("double"))
        .withColumn("bearing",
            F.col("bearing").cast("double"))
        .withColumn("speed_kmh",
            F.col("speed").cast("double"))
        .withColumn("ingest_delay_seconds",
            F.unix_timestamp(F.current_timestamp()) -
            F.unix_timestamp("timestamp"))
        .withColumn("_ingest_date",
            F.to_date(F.current_timestamp()))
    )
```

---

## LINEAGE REGISTRY FORMAT

Maintain in `shared/nb_lineage_registry.py`.
This is the ONLY acceptable lineage tracking method.
Never track lineage in a database table or wiki.

```python
LINEAGE = {
    "transit_bronze.bus_positions": {
        "display_name": "Bus Positions (Bronze)",
        "layer":        "bronze",
        "sources":      ["TransLink GTFS-RT API"],
        "source_type":  "api",
        "feeds":        ["transit_silver.bus_sensor_readings"],
        "grain":        "One row per vehicle position update",
        "owner":        "Etienne Wang",
        "notebook":     "pipelines/bronze/bus_positions.py",
        "sla_minutes":  5,
        "pii_columns":  ["vehicle_id"],
        "column_lineage": {
            "vehicle_id":           "GTFS-RT vehicle.id (unchanged) ⚠️ PII",
            "route_id":             "GTFS-RT trip_update.trip.route_id (unchanged)",
            "timestamp":            "GTFS-RT vehicle.timestamp (cast TIMESTAMP)",
            "latitude":             "GTFS-RT vehicle.position.latitude (cast DOUBLE)",
            "longitude":            "GTFS-RT vehicle.position.longitude (cast DOUBLE)"
        },
        "tags": {
            "domain":  "transit",
            "layer":   "bronze",
            "project": "VANCOUVER_TRANSIT_INTELLIGENCE"
        }
    },
    "transit_silver.bus_sensor_readings": {
        "display_name": "Bus Sensor Readings (Silver)",
        "layer":        "silver",
        "sources":      ["transit_bronze.bus_positions"],
        "source_type":  "delta_table",
        "feeds":        ["transit_silver.bus_sensor_anomalies",
                         "transit_gold.route_performance"],
        "grain":        "One row per vehicle position update (typed + enriched)",
        "owner":        "Etienne Wang",
        "notebook":     "pipelines/silver/bus_sensor_readings.py",
        "sla_minutes":  15,
        "pii_columns":  ["vehicle_id"],
        "column_lineage": {
            "vehicle_id":           "bronze.vehicle_id (unchanged) ⚠️ PII",
            "timestamp":            "bronze.timestamp (cast TIMESTAMP)",
            "latitude":             "bronze.latitude (cast DOUBLE)",
            "speed_kmh":            "bronze.speed (cast DOUBLE, renamed)",
            "ingest_delay_seconds": "DERIVED: unix(eventTime) - unix(timestamp)"
        },
        "tags": {
            "domain":  "transit",
            "layer":   "silver",
            "project": "VANCOUVER_TRANSIT_INTELLIGENCE"
        }
    },
    "transit_silver.bus_sensor_anomalies": {
        "display_name": "Bus Sensor Anomalies (Silver)",
        "layer":        "silver",
        "sources":      ["transit_silver.bus_sensor_readings"],
        "source_type":  "delta_table",
        "feeds":        ["transit_gold.route_performance"],
        "grain":        "One row per vehicle_id + anomaly_type",
        "owner":        "Etienne Wang",
        "notebook":     "pipelines/silver/bus_sensor_anomalies.py",
        "sla_minutes":  20,
        "pii_columns":  ["vehicle_id"],
        "column_lineage": {
            "vehicle_id":    "silver.bus_sensor_readings.vehicle_id ⚠️ PII",
            "anomaly_type":  "DERIVED: rule-based classification",
            "anomaly_detail":"DERIVED: concatenated detail per rule",
            "severity":      "DERIVED: from ANOMALY_RULES config"
        },
        "tags": {
            "domain":  "transit",
            "layer":   "silver",
            "project": "VANCOUVER_TRANSIT_INTELLIGENCE"
        }
    },
    "transit_gold.route_performance": {
        "display_name": "Route Performance (Gold)",
        "layer":        "gold",
        "sources":      ["transit_silver.bus_sensor_readings",
                         "transit_silver.bus_sensor_anomalies"],
        "source_type":  "delta_table",
        "feeds":        [],
        "grain":        "One row per route_id per date",
        "owner":        "Etienne Wang",
        "notebook":     "pipelines/gold/route_performance.py",
        "sla_minutes":  60,
        "pii_columns":  [],
        "column_lineage": {
            "route_id":        "silver.bus_sensor_readings.route_id",
            "date":            "silver.bus_sensor_readings._ingest_date",
            "on_time_rate":    "DERIVED: % readings with ingest_delay < 30s",
            "avg_delay_secs":  "DERIVED: avg(ingest_delay_seconds)",
            "anomaly_count":   "DERIVED: count from bus_sensor_anomalies",
            "total_readings":  "DERIVED: count(*)"
        },
        "tags": {
            "domain":  "transit",
            "layer":   "gold",
            "project": "VANCOUVER_TRANSIT_INTELLIGENCE"
        }
    }
}
```

---

## DATABRICKS ASSET BUNDLES (DABs)

All deployments via DABs. No click-ops ever.

```yaml
# databricks.yml
bundle:
  name: vancouver-transit-intelligence

variables:
  catalog:
    default: gtfs_dev
  schema_prefix:
    default: transit

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://dbc-817e0bdc-6c87.cloud.databricks.com
    variables:
      catalog: gtfs_dev
  prod:
    mode: production
    workspace:
      host: https://dbc-817e0bdc-6c87.cloud.databricks.com
    variables:
      catalog: gtfs_prod

resources:
  pipelines:
    transit_bronze:
      name: "Vancouver Transit - Bronze"
      target: ${var.catalog}.${var.schema_prefix}_bronze
      libraries:
        - notebook:
            path: ./pipelines/bronze/bus_positions.py
    transit_silver:
      name: "Vancouver Transit - Silver"
      target: ${var.catalog}.${var.schema_prefix}_silver
      libraries:
        - notebook:
            path: ./pipelines/silver/bus_sensor_readings.py
        - notebook:
            path: ./pipelines/silver/bus_sensor_anomalies.py
    transit_gold:
      name: "Vancouver Transit - Gold"
      target: ${var.catalog}.${var.schema_prefix}_gold
      libraries:
        - notebook:
            path: ./pipelines/gold/route_performance.py
```

```bash
# Deploy commands — use these only
databricks bundle validate              # always run first
databricks bundle deploy                # deploy to dev
databricks bundle deploy --target prod  # deploy to prod
```

---

## PROJECT DIRECTORY STRUCTURE

```
vancouver-transit-intelligence/
│
├── CLAUDE.md                          ← This file
├── databricks.yml                     ← DABs configuration
├── README.md                          ← Architecture + lab progress
├── .gitignore
│
├── pipelines/
│   ├── bronze/
│   │   └── bus_positions.py           ← Lab 3
│   ├── silver/
│   │   ├── bus_sensor_readings.py     ← Lab 4
│   │   └── bus_sensor_anomalies.py    ← Lab 5
│   └── gold/
│       ├── route_performance.py       ← Lab 6
│       └── fleet_health_score.py      ← Lab 6
│
├── shared/
│   ├── nb_lineage_registry.py         ← All lineage declarations
│   ├── nb_transformations.py          ← All pure transform functions
│   └── nb_data_quality.py             ← DataQuality class
│
├── ml/
│   └── delay_prediction.py            ← Lab 10: MLflow model
│
├── ai/
│   └── transit_rag_agent.py           ← Lab 11: Mosaic AI RAG
│
├── mcp/
│   └── transit_server.py              ← Lab 8: MCP Server
│
├── unity_catalog/
│   └── catalog_setup.sql              ← Lab 7: UC design
│
├── tests/
│   ├── test_transformations.py        ← Unit tests for pure functions
│   └── test_anomaly_rules.py          ← Unit tests for anomaly detection
│
└── .github/
    └── workflows/
        └── databricks_ci.yml          ← CI/CD pipeline
```

---

## ANTI-PATTERNS — NEVER GENERATE THESE

### ❌ Config Tables
```python
# WRONG — optimizer is blind to SQL in strings
sql = row["sql_string"]
spark.sql(sql)  # Predicate pushdown FAILS, Liquid Clustering BLIND

# RIGHT — optimizer sees everything
df = spark.table("transit_bronze.bus_positions") \
    .filter(F.col("date") > "2026-01-01")
```

### ❌ External Tables as Primary Storage
```python
# WRONG
spark.sql("CREATE EXTERNAL TABLE ...")

# RIGHT — managed Delta always
df.write.format("delta").mode("overwrite").saveAsTable("transit_silver.bus_sensor_readings")
```

### ❌ Environment in Names
```python
# WRONG
catalog = "transit_bronze_dev"

# RIGHT
catalog = "dev"
schema  = "transit_bronze"
```

### ❌ Business Logic Mixed with I/O
```python
# WRONG — untestable
def load_and_transform():
    df = spark.read.load(source)    # I/O
    df = df.withColumn(...)          # logic mixed
    df.write.save(target)            # I/O

# RIGHT — separated concerns
def transform_bus_positions(df):     # pure function
    return df.withColumn(...)

source_df = spark.read.load(source)  # I/O separate
result_df = transform_bus_positions(source_df)
result_df.write.save(target)
```

### ❌ Missing Required Elements
Never generate a file without:
- Asset Declaration Block
- Docstring with column lineage on every transform function
- @dp.expect checks on every @dp.table
- Lineage registry entry for every new table

---

## SELF-DOCUMENTATION TEST

Before completing any file, verify a new engineer could answer:

1. ✅ What table does this pipeline produce?
2. ✅ Where does the data come from?
3. ✅ What transformations are applied and why?
4. ✅ What are the data quality rules?
5. ✅ Does this table contain PII?
6. ✅ Who owns this table?
7. ✅ What breaks if I change this table?

If any answer requires querying a database or asking a person → fix the code.

---

## LAB PROGRESS TRACKER

```
Phase 1: Foundation
    Lab 1: Project Setup + CLAUDE.md     ← claude init
    Lab 2: TransLink API + Auto Loader   ← /compact
    Lab 3: Bronze Pipeline               ← /skill

Phase 2: Transformation
    Lab 4: Silver Pipeline               ← /rewind
    Lab 5: Anomaly Detection             ← Tool Use
    Lab 6: Gold Aggregations             ← /bug

Phase 3: Platform
    Lab 7: Unity Catalog                 ← /doctor
    Lab 8: DABs + CI/CD + MCP Server     ← MCP basics
    Lab 9: Lineage + MCP Desktop         ← Claude Desktop

Phase 4: AI Layer
    Lab 10: MLflow Delay Prediction      ← Multi-Agent
    Lab 11: Mosaic AI RAG Agent          ← Agent SDK
    Lab 12: Portfolio + SA Champion Prep ← Full review
```

Update checklist as labs complete:
- [ ] Lab 1   - [ ] Lab 2   - [ ] Lab 3
- [ ] Lab 4   - [ ] Lab 5   - [ ] Lab 6
- [ ] Lab 7   - [ ] Lab 8   - [ ] Lab 9
- [ ] Lab 10  - [ ] Lab 11  - [ ] Lab 12

---

## HOW TO USE CLAUDE CODE ON THIS PROJECT

```bash
# Start every session
cd vancouver-transit-intelligence
claude

# First task in every session
> "Read CLAUDE.md and confirm you understand
   the Vancouver Transit project standards."

# Generate a new pipeline
> "Create the [table_name] pipeline following
   all CLAUDE.md standards. Source is [source].
   Target is [target]. Layer is [layer]."

# Review existing code
> "Review pipelines/silver/bus_sensor_readings.py
   against CLAUDE.md standards.
   Check: Asset Block, @dp.expect, column lineage,
   UC naming, anti-patterns."

# After every session
git add .
git commit -m "Lab X: [description]

Generated with Claude Code
Reviewed and approved by Etienne Wang"
git push origin main
```

---

## COMMIT MESSAGE STANDARD

```
Lab X: [Short description]

- [What was created/changed]
- [Standards followed]
- [DQ rules added]

Generated with Claude Code
Reviewed and approved by Etienne Wang
```

---

*Living document — update as the project evolves.*
*Last updated: 2026-08-20 · Etienne Wang*
