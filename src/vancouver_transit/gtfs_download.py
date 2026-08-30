"""Download the TransLink GTFS Static feed and land it in a Unity Catalog volume.

TransLink publishes a new ZIP roughly weekly (usually by Friday). The archive is
public - the TransLink API key is only needed for the GTFS *Realtime* endpoints.

Files are landed one directory per GTFS entity, with a Hive-style snapshot
partition so Auto Loader picks up each new publication as new files:

    <volume_root>/<entity>/snapshot_id=<UTC timestamp>/<entity>.txt

A manifest keyed by the archive's SHA-256 is written alongside; if the same
archive is downloaded again the extraction is skipped, so an unchanged weekly
feed does not re-trigger 200+ MB of ingestion.
"""

import argparse
import hashlib
import json
import os
import shutil
import tempfile
import urllib.request
import zipfile
from datetime import datetime, timezone

GTFS_STATIC_URL = "https://gtfs-static.translink.ca/gtfs/google_transit.zip"
DEFAULT_VOLUME_ROOT = "/Volumes/gtfs/bronze/landing/gtfs_static"

# UTF-8 BOM. A few TransLink files carry one, which would otherwise end up glued
# to the first column name in the parsed header.
BOM = b"\xef\xbb\xbf"


def _download(url: str, dest: str) -> str:
    """Fetch `url` to `dest` and return the SHA-256 of the bytes written."""
    digest = hashlib.sha256()
    request = urllib.request.Request(url, headers={"User-Agent": "vancouver-transit-platform"})
    with urllib.request.urlopen(request) as response, open(dest, "wb") as out:
        for chunk in iter(lambda: response.read(1024 * 1024), b""):
            digest.update(chunk)
            out.write(chunk)
    return digest.hexdigest()


def _extract(archive: str, volume_root: str, snapshot_id: str) -> list[str]:
    """Extract every .txt member into its own entity/snapshot directory."""
    landed = []
    with zipfile.ZipFile(archive) as zf:
        for member in zf.namelist():
            if not member.endswith(".txt"):
                continue
            entity = os.path.basename(member)[: -len(".txt")]
            target_dir = os.path.join(volume_root, entity, f"snapshot_id={snapshot_id}")
            os.makedirs(target_dir, exist_ok=True)
            target = os.path.join(target_dir, f"{entity}.txt")
            with zf.open(member) as src, open(target, "wb") as out:
                head = src.read(len(BOM))
                out.write(head[len(BOM):] if head.startswith(BOM) else head)
                shutil.copyfileobj(src, out, length=8 * 1024 * 1024)
            landed.append(entity)
    return sorted(landed)


def download_gtfs_static(volume_root: str = DEFAULT_VOLUME_ROOT, url: str = GTFS_STATIC_URL) -> dict:
    manifest_dir = os.path.join(volume_root, "_manifests")
    os.makedirs(manifest_dir, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmp:
        archive = os.path.join(tmp, "google_transit.zip")
        sha256 = _download(url, archive)

        manifest_path = os.path.join(manifest_dir, f"{sha256}.json")
        if os.path.exists(manifest_path):
            with open(manifest_path) as f:
                previous = json.load(f)
            print(f"Archive {sha256[:12]} already landed as snapshot {previous['snapshot_id']}; skipping.")
            return {**previous, "skipped": True}

        snapshot_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        entities = _extract(archive, volume_root, snapshot_id)
        manifest = {
            "snapshot_id": snapshot_id,
            "sha256": sha256,
            "source_url": url,
            "bytes": os.path.getsize(archive),
            "entities": entities,
        }

    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"Landed snapshot {snapshot_id}: {len(entities)} files -> {volume_root}")
    return {**manifest, "skipped": False}


def main() -> None:
    parser = argparse.ArgumentParser(description="Land the TransLink GTFS Static feed in a UC volume")
    parser.add_argument("--volume-root", default=DEFAULT_VOLUME_ROOT)
    parser.add_argument("--url", default=GTFS_STATIC_URL)
    args = parser.parse_args()
    download_gtfs_static(volume_root=args.volume_root, url=args.url)


if __name__ == "__main__":
    main()
