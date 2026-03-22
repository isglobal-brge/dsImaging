#!/usr/bin/env python3
"""Build or update a content hash index for a local image directory.

Computes full-file SHA-256 for each image file and writes a Parquet
index. Supports --incremental to only hash files not in the existing index.

For S3-hosted datasets, the R-level build_hash_index() function handles
download+hash via the backend abstraction. This script is for local
filesystem datasets or admin bootstrapping.

Usage:
    python build_hash_index.py --image-root /data/images --output index.parquet
    python build_hash_index.py --image-root /data/images --output index.parquet --incremental
"""

import argparse
import hashlib
import json
import os
import sys
import time

HASH_CHUNK = 65536
IMAGE_EXTENSIONS = {".nii.gz", ".nii", ".nrrd", ".mha", ".mhd", ".dcm"}


def sha256_file(path):
    """Streaming SHA-256 of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(HASH_CHUNK)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def sample_id_from_filename(filename):
    """Extract sample_id by stripping known extensions."""
    name = filename
    for ext in sorted(IMAGE_EXTENSIONS, key=len, reverse=True):
        if name.lower().endswith(ext):
            return name[: -len(ext)]
    return os.path.splitext(name)[0]


def is_image_file(filename):
    """Check if a filename has a known image extension."""
    lower = filename.lower()
    return any(lower.endswith(ext) for ext in IMAGE_EXTENSIONS)


def load_existing_index(path):
    """Load existing Parquet index. Returns dict of uri -> row."""
    if not os.path.exists(path):
        return {}
    try:
        import pyarrow.parquet as pq
        table = pq.read_table(path)
        df = table.to_pydict()
        return {uri: i for i, uri in enumerate(df.get("uri", []))}
    except Exception:
        return {}


def main():
    parser = argparse.ArgumentParser(
        description="Build content hash index for image files")
    parser.add_argument("--image-root", required=True,
                        help="Directory containing image files")
    parser.add_argument("--output", required=True,
                        help="Output Parquet file path")
    parser.add_argument("--incremental", action="store_true",
                        help="Only hash files not already in the index")
    args = parser.parse_args()

    if not os.path.isdir(args.image_root):
        print(f"ERROR: Not a directory: {args.image_root}", file=sys.stderr)
        sys.exit(1)

    # Collect image files
    files = []
    for filename in sorted(os.listdir(args.image_root)):
        filepath = os.path.join(args.image_root, filename)
        if os.path.isfile(filepath) and is_image_file(filename):
            files.append((filepath, filename))

    # Load existing index if incremental
    existing = {}
    if args.incremental:
        existing = load_existing_index(args.output)
        print(f"Existing index: {len(existing)} entries")

    rows = {"sample_id": [], "uri": [], "content_hash": [], "size": [],
            "last_modified": [], "version_id": [], "etag": []}

    # Keep existing entries
    if args.incremental and existing:
        try:
            import pyarrow.parquet as pq
            table = pq.read_table(args.output)
            for col in rows:
                rows[col] = table.column(col).to_pylist()
        except Exception:
            pass

    new_count = 0
    for filepath, filename in files:
        uri = filepath
        if uri in existing and args.incremental:
            continue

        try:
            stat = os.stat(filepath)
            content_hash = sha256_file(filepath)
            rows["sample_id"].append(sample_id_from_filename(filename))
            rows["uri"].append(uri)
            rows["content_hash"].append(content_hash)
            rows["size"].append(stat.st_size)
            rows["last_modified"].append(
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(stat.st_mtime)))
            rows["version_id"].append(None)
            rows["etag"].append(None)
            new_count += 1
            print(f"  Hashed: {filename} ({content_hash[:16]}...)")
        except Exception as e:
            print(f"  Warning: skipping {filename}: {e}", file=sys.stderr)

    # Write Parquet
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        table = pa.table(rows)
        pq.write_table(table, args.output)
        print(f"Index written: {len(rows['sample_id'])} entries "
              f"({new_count} new)")
    except ImportError:
        # Fallback: write as JSON (for environments without pyarrow)
        json_path = args.output.replace(".parquet", ".json")
        entries = []
        for i in range(len(rows["sample_id"])):
            entries.append({k: rows[k][i] for k in rows})
        with open(json_path, "w") as f:
            json.dump(entries, f, indent=2)
        print(f"Index written as JSON (pyarrow not available): {json_path}")


if __name__ == "__main__":
    main()
