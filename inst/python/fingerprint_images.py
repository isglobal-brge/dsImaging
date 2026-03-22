#!/usr/bin/env python3
"""Content fingerprinting for medical images.

Computes a fast, content-addressable fingerprint for each image file
in a directory. The fingerprint is:

    SHA-256(file_size_bytes || mtime_ns || first_4096_bytes || last_4096_bytes)

This avoids reading the full file (which can be >1GB for NIfTI/DICOM)
while still detecting changes reliably. Streaming, O(1) memory per file.

Output: JSON array of {sample_id, file_path, fingerprint, file_size, file_mtime}.
"""

import argparse
import hashlib
import json
import os
import struct
import sys

CHUNK_SIZE = 4096


def fingerprint_file(path):
    """Compute fingerprint for a single file."""
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
            tail = f.read(CHUNK_SIZE)
            h.update(tail)

    return h.hexdigest(), file_size, file_mtime


def sample_id_from_filename(filename):
    """Extract sample_id by stripping known extensions."""
    name = filename
    for ext in (".nii.gz", ".nii", ".nrrd", ".mha", ".mhd", ".dcm"):
        if name.lower().endswith(ext):
            name = name[: -len(ext)]
            break
    else:
        name = os.path.splitext(name)[0]
    return name


def main():
    parser = argparse.ArgumentParser(description="Fingerprint image files")
    parser.add_argument("--image-root", required=True,
                        help="Directory containing image files")
    parser.add_argument("--output", required=True,
                        help="Output JSON file path")
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
            results.append({
                "sample_id": sample_id_from_filename(filename),
                "file_path": filepath,
                "fingerprint": fp,
                "file_size": fsize,
                "file_mtime": fmtime,
            })
        except Exception as e:
            print(f"  Warning: skipping {filename}: {e}", file=sys.stderr)

    with open(args.output, "w") as f:
        json.dump(results, f)

    print(f"Fingerprinted {len(results)} files")


if __name__ == "__main__":
    main()
