# GTFS Static — bronze ingestion

Ingests the [TransLink GTFS Static feed](https://www.translink.ca/about-us/doing-business-with-translink/app-developer-resources/gtfs/gtfs-data)
into `gtfs_dev.transit_bronze` via Auto Loader.

## Shape

```
gtfs_ingest (job, daily 06:00 America/Vancouver)
├── download_gtfs_static   vancouver_transit.gtfs_download:main
│     GET https://gtfs-static.translink.ca/gtfs/google_transit.zip
│     └── /Volumes/gtfs_dev/transit_bronze/landing/gtfs_static/<entity>/snapshot_id=<ts>/<entity>.txt
└── refresh_bronze         pipeline gtfs_bronze_ingest
      Auto Loader (csv) → gtfs_dev.transit_bronze.<entity>   (15 streaming tables)
```

## Egress: works in slalom-sandbox, was blocked in the old workspace

Verified end to end on 2026-09-01 against
`https://dbc-817e0bdc-6c87.cloud.databricks.com` (profile `slalom-sandbox`):
the job succeeded in 92s, landed 15 files, and built all 15 streaming tables
(5,428,459 rows in `stop_times`). Serverless egress to
`gtfs-static.translink.ca` is permitted there — no network-policy change needed.

This is workspace-specific, so it is worth knowing what failure looks like. In the
previous GCP workspace `download_gtfs_static` failed with `URLError: [Errno -3]
Temporary failure in name resolution`, because that workspace was serverless-only
(`new_cluster` rejected with "Only serverless compute is supported") and its
serverless compute had no outbound internet. The fix there is an account-admin
action: add `gtfs-static.translink.ca` to the serverless network policy's allowed
domains. No code change is needed either way.

If you hit that error in a new workspace, you can stage the files from outside
Databricks in the meantime. This produces exactly the layout Auto Loader expects:

```bash
python -c "from vancouver_transit.gtfs_download import download_gtfs_static; \
           download_gtfs_static(volume_root='/tmp/stage/gtfs_static')"
databricks fs cp -r --overwrite /tmp/stage/gtfs_static \
    dbfs:/Volumes/gtfs_dev/transit_bronze/landing/gtfs_static --profile <your-profile>
databricks bundle run gtfs_bronze_ingest -t dev --profile <your-profile>
```

## Notes

- **No API key.** The static ZIP is public. The TransLink key is only needed for the
  GTFS *Realtime* endpoints; it is stored in the `translink` secret scope under `api_key`.
- **Idempotent download.** The archive's SHA-256 is recorded under
  `<volume_root>/_manifests/`. If TransLink has not republished, extraction is skipped and
  the pipeline sees no new files.
- **Bronze is all STRING.** Type inference is deliberately off — it would strip leading
  zeros from values like `route_short_name = "002"` and mis-handle GTFS's `YYYYMMDD`
  dates. Cast in silver.
- **Snapshot history.** `snapshot_id` is a Hive partition on the landing path and arrives
  as a column, so each weekly publication is retained and queryable as an append.
- **BOM stripping.** `direction_names_exceptions.txt` and `route_names_exceptions.txt`
  ship with a UTF-8 BOM; the downloader strips it so the first column name parses cleanly.
- **New GTFS files** land in the volume automatically but need adding to `GTFS_ENTITIES`
  in `transformations/bronze_gtfs_static.py` before they become tables.
- **The landing path is not hardcoded.** `VOLUME_ROOT` comes from the pipeline's
  `configuration.gtfs.volume_root`, set in `resources/gtfs_bronze_ingest.pipeline.yml`
  from `${var.catalog}` / `${var.gtfs_bronze_schema}`. The `landing` volume itself is a
  bundle resource (`resources/gtfs_landing.volume.yml`); the catalog and bronze schema
  must already exist in the workspace.

## Run

```bash
databricks bundle deploy -t dev --profile <your-profile>
databricks bundle run gtfs_ingest -t dev --profile <your-profile>
```
