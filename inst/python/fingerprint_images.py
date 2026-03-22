#!/usr/bin/env python3
"""Content fingerprinting for medical images.

Two levels of hashing:

1. Fast fingerprint (always computed):
   SHA-256(file_size || mtime || first_4096_bytes || last_4096_bytes)
   Purpose: fast change detection between scans. O(1) memory.

2. Content hash (optional, --compute-content-hash):
   SHA-256(full file contents), streamed in 64KB chunks.
   Purpose: true content identity for deduplication, provenance, publication.

The fingerprint is for scan speed. The content_hash is for correctness.
"""

import argparse
import hashlib
import json
import os
import struct
import sys

CHUNK_SIZE = 4096
HASH_CHUNK = 65536


def fingerprint_file(path):
    """Fast fingerprint: size + mtime + head + tail."""
    stat = os.stat(path)
    file_size = stat.st_size
    file_mtime = stat.st_mtime

    h = hashlib.sha256()
    h.update(struct.pack(">Q", file_size))
    h.update(struct.pack(">d", file_mtime))

    with open(path, "rb") as f:
        head = f.read(CHUNK_SIZE)
        h.update(head)
        if file_size > CHUNK_SIZE:
            f.seek(max(0, file_size - CHUNK_SIZE))
            h.update(f.read(CHUNK_SIZE))

    return h.hexdigest(), file_size, file_mtime


def content_hash_file(path):
    """Strong content hash: full-file SHA-256, streamed."""
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
    for ext in (".nii.gz", ".nii", ".nrrd", ".mha", ".mhd", ".dcm"):
        if name.lower().endswith(ext):
            return name[: -len(ext)]
    return os.path.splitext(name)[0]


def main():
    parser = argparse.ArgumentParser(description="Fingerprint image files")
    parser.add_argument("--image-root", required=True,
                        help="Directory containing image files")
    parser.add_argument("--output", required=True,
                        help="Output JSON file path")
    parser.add_argument("--compute-content-hash", action="store_true",
                        help="Also compute full-file SHA-256 (slower but strong)")
    args = parser.parse_args()

    if not os.path.isdir(args.image_root):
        print(f"ERROR: Not a directory: {args.image_root}", file=sys.stderr)
        sys.exit(1)

    results = []
    for filename in sorted(os.listdir(args.image_root)):
        filepath = os.path.join(args.image_root, filename)
        if not os.path.isfile(filepath) or filename.startswith("."):
            continue
        try:
            fp, fsize, fmtime = fingerprint_file(filepath)
            entry = {
                "sample_id": sample_id_from_filename(filename),
                "file_path": filepath,
                "fingerprint": fp,
                "file_size": fsize,
                "file_mtime": fmtime,
            }
            if args.compute_content_hash:
                entry["content_hash"] = content_hash_file(filepath)
            results.append(entry)
        except Exception as e:
            print(f"  Warning: skipping {filename}: {e}", file=sys.stderr)

    with open(args.output, "w") as f:
        json.dump(results, f)

    mode = "fingerprint + content_hash" if args.compute_content_hash else "fingerprint only"
    print(f"Fingerprinted {len(results)} files ({mode})")


if __name__ == "__main__":
    main()
