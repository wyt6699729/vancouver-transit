# GTFS Static — bronze ingestion

Ingests the [TransLink GTFS Static feed](https://www.translink.ca/about-us/doing-business-with-translink/app-developer-resources/gtfs/gtfs-data)
into `gtfs.bronze` via Auto Loader.

## Shape

```
gtfs_ingest (job, weekly)
├── download_gtfs_static   vancouver_transit.gtfs_download:main
│     GET https://gtfs-static.translink.ca/gtfs/google_transit.zip
│     └── /Volumes/gtfs/bronze/landing/gtfs_static/<entity>/snapshot_id=<ts>/<entity>.txt
└── refresh_bronze         pipeline gtfs_bronze_ingest
      Auto Loader (csv) → gtfs.bronze.<entity>   (15 streaming tables)
```

## ⚠️ The download task is blocked in this workspace

`download_gtfs_static` fails with `URLError: [Errno -3] Temporary failure in name
resolution`. Two workspace facts combine to cause this:

- the workspace is **serverless-only** — adding a `new_cluster` to the task is rejected
  with `Only serverless compute is supported in the workspace`;
- **serverless compute has no outbound internet**, so it cannot reach
  `gtfs-static.translink.ca`.

The fix is an account-admin action: add `gtfs-static.translink.ca` to the serverless
network policy's allowed domains. Once that is done the job runs end to end with no code
change.

**Until then**, stage the files from outside Databricks. This produces exactly the layout
Auto Loader expects:

```bash
python -c "from vancouver_transit.gtfs_download import download_gtfs_static; \
           download_gtfs_static(volume_root='/tmp/stage/gtfs_static')"
databricks fs cp -r --overwrite /tmp/stage/gtfs_static \
    dbfs:/Volumes/gtfs/bronze/landing/gtfs_static --profile slalom-dev
databricks bundle run gtfs_bronze_ingest -t dev --profile slalom-dev
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

## Run

```bash
databricks bundle deploy -t dev --profile slalom-dev
databricks bundle run gtfs_ingest -t dev --profile slalom-dev
```
