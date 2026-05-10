#!/usr/bin/env python3
"""Image embedding extraction with a deterministic local baseline."""

import argparse
import csv
import os
import sys

from dsimaging_utils import cfg, cfg_int, image_files, package_versions, resolve_asset_path, strip_extensions, write_json


def torch_device():
    try:
        import torch
        if torch.cuda.is_available():
            return "cuda"
        if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
            return "mps"
        return "cpu"
    except Exception:
        return "unavailable"


def image_vector(path, bins):
    import numpy as np

    try:
      import SimpleITK as sitk
      arr = sitk.GetArrayFromImage(sitk.ReadImage(path)).astype("float32")
    except Exception:
      from PIL import Image
      arr = np.asarray(Image.open(path).convert("L")).astype("float32")

    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        arr = np.array([0.0], dtype="float32")
    lo, hi = np.percentile(arr, [1, 99])
    if hi <= lo:
        lo, hi = float(arr.min()), float(arr.max())
    if hi <= lo:
        hi = lo + 1.0
    clipped = np.clip(arr, lo, hi)
    hist, _ = np.histogram(clipped, bins=bins, range=(lo, hi), density=True)
    stats = np.array([
        clipped.mean(),
        clipped.std(),
        clipped.min(),
        clipped.max(),
        np.percentile(clipped, 10),
        np.percentile(clipped, 50),
        np.percentile(clipped, 90),
        float(arr.size),
    ], dtype="float64")
    vec = np.concatenate([stats, hist.astype("float64")])
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec = vec / norm
    return vec


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    os.makedirs(args.output, exist_ok=True)

    image_root = resolve_asset_path(cfg("image_asset", "images"), "images", cfg("image_root"))
    bins = cfg_int("bins", 32)
    model = cfg("model", "intensity_histogram")
    images = image_files(image_root)
    if not images:
        print("ERROR: No images found for embeddings", file=sys.stderr)
        sys.exit(1)

    rows = []
    for path in images:
        sid = strip_extensions(os.path.basename(path))
        vec = image_vector(path, bins)
        row = {"sample_id": sid, "model": model}
        for i, value in enumerate(vec):
            row[f"emb_{i:03d}"] = float(value)
        rows.append(row)

    fields = ["sample_id", "model"] + [f"emb_{i:03d}" for i in range(len(rows[0]) - 2)]
    csv_path = os.path.join(args.output, "image_embeddings.csv")
    with open(csv_path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    write_json(os.path.join(args.output, "image_embeddings_summary.json"), {
        "n_samples": len(rows),
        "model": model,
        "embedding_dim": len(fields) - 2,
        "accelerator": torch_device(),
        "table": csv_path,
        "versions": package_versions(["SimpleITK", "numpy", "PIL", "torch"]),
    })


if __name__ == "__main__":
    main()
