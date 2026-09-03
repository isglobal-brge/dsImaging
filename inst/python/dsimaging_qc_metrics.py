#!/usr/bin/env python3
"""Disclosure-local image and mask QC metrics for dsImaging jobs."""

import argparse
import csv
import os
import sys

from dsimaging_utils import (
    IMAGE_EXTS,
    MASK_EXTS,
    cfg,
    mapped_sample_files,
    package_versions,
    write_json,
)


def image_metrics(path):
    import SimpleITK as sitk
    import numpy as np

    img = sitk.ReadImage(path)
    arr = sitk.GetArrayFromImage(img).astype("float64")
    return {
        "size": "x".join(str(v) for v in img.GetSize()),
        "spacing": "x".join(f"{v:.6g}" for v in img.GetSpacing()),
        "image_min": float(np.min(arr)),
        "image_max": float(np.max(arr)),
        "image_mean": float(np.mean(arr)),
        "image_std": float(np.std(arr)),
    }, img, arr


def mask_metrics(mask_path, image, image_arr):
    import SimpleITK as sitk
    import numpy as np

    mask = sitk.ReadImage(mask_path)
    if mask.GetSize() != image.GetSize() or mask.GetSpacing() != image.GetSpacing():
        mask = sitk.Resample(mask, image, sitk.Transform(), sitk.sitkNearestNeighbor, 0, sitk.sitkUInt8)
    marr = sitk.GetArrayFromImage(mask) > 0
    voxels = int(np.sum(marr))
    spacing = image.GetSpacing()
    voxel_volume = float(np.prod(spacing))
    out = {
        "mask_voxels": voxels,
        "mask_volume": voxels * voxel_volume,
    }
    if voxels > 0:
        vals = image_arr[marr]
        out.update({
            "masked_mean": float(np.mean(vals)),
            "masked_std": float(np.std(vals)),
            "masked_min": float(np.min(vals)),
            "masked_max": float(np.max(vals)),
        })
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    image_asset = cfg("image_asset", "images")
    mask_asset = cfg("mask_asset", "")
    try:
        images = dict((sid, path) for path, sid in mapped_sample_files(
            image_asset, "images", artifact_types=("image_root",),
            extensions=IMAGE_EXTS,
        ))
        masks = dict((sid, path) for path, sid in mapped_sample_files(
            mask_asset, "masks", artifact_types=("mask_root",),
            extensions=MASK_EXTS,
        )) if mask_asset else {}
    except RuntimeError:
        print("ERROR: Admitted imaging inputs are unavailable", file=sys.stderr)
        sys.exit(1)
    if not images:
        print("ERROR: No images found", file=sys.stderr)
        sys.exit(1)
    os.makedirs(args.output, exist_ok=True)

    rows = []
    for sid, image_path in sorted(images.items()):
        row, img, arr = image_metrics(image_path)
        row["sample_id"] = sid
        if sid in masks:
            row.update(mask_metrics(masks[sid], img, arr))
        rows.append(row)

    all_fields = []
    for row in rows:
        for key in row.keys():
            if key not in all_fields:
                all_fields.append(key)
    csv_path = os.path.join(args.output, "imaging_qc_metrics.csv")
    with open(csv_path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=all_fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    write_json(os.path.join(args.output, "imaging_qc_summary.json"), {
        "n_samples": len(rows),
        "has_masks": bool(masks),
        "table": csv_path,
        "versions": package_versions(["SimpleITK", "numpy"]),
    })


if __name__ == "__main__":
    main()
