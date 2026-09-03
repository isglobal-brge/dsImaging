#!/usr/bin/env python3
"""Create low-resolution QC thumbnails and overlays for local review."""

import argparse
import csv
import hashlib
import os
import sys

from dsimaging_utils import (
    IMAGE_EXTS,
    MASK_EXTS,
    cfg,
    cfg_int,
    mapped_sample_files,
    package_versions,
    write_collection_output_manifest,
    write_json,
)


def middle_slice(path):
    import numpy as np
    import SimpleITK as sitk

    img = sitk.ReadImage(path)
    arr = sitk.GetArrayFromImage(img).astype("float32")
    if arr.ndim == 3:
        arr = arr[arr.shape[0] // 2, :, :]
    elif arr.ndim > 3:
        arr = arr.reshape((-1,) + arr.shape[-2:])[arr.reshape((-1,) + arr.shape[-2:]).shape[0] // 2]
    lo, hi = np.percentile(arr, [1, 99])
    if hi <= lo:
        lo, hi = float(np.min(arr)), float(np.max(arr))
    if hi <= lo:
        hi = lo + 1.0
    return np.clip((arr - lo) / (hi - lo), 0, 1)


def mask_slice(path, shape):
    import numpy as np
    import SimpleITK as sitk

    mask = sitk.GetArrayFromImage(sitk.ReadImage(path))
    if mask.ndim == 3:
        mask = mask[mask.shape[0] // 2, :, :]
    mask = mask > 0
    if mask.shape != shape:
        return np.zeros(shape, dtype=bool)
    return mask


def save_overlay(image_arr, mask_arr, out_path, max_size):
    import numpy as np
    from PIL import Image

    gray = (image_arr * 255).astype("uint8")
    rgb = np.stack([gray, gray, gray], axis=-1)
    if mask_arr is not None:
        rgb[mask_arr, 0] = 255
        rgb[mask_arr, 1] = (rgb[mask_arr, 1] * 0.35).astype("uint8")
        rgb[mask_arr, 2] = (rgb[mask_arr, 2] * 0.35).astype("uint8")
    img = Image.fromarray(rgb)
    img.thumbnail((max_size, max_size))
    img.save(out_path, format="PNG", optimize=True)


def public_name(sample_id, idx, anonymize=True):
    if not anonymize:
        return sample_id
    digest = hashlib.sha256(sample_id.encode("utf-8")).hexdigest()[:12]
    return f"case_{idx:04d}_{digest}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    os.makedirs(args.output, exist_ok=True)

    image_asset = cfg("image_asset", "images")
    mask_asset = cfg("mask_asset", "")
    max_size = cfg_int("max_size", 192)
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
        print("ERROR: No images found for QC thumbnails", file=sys.stderr)
        sys.exit(1)

    rows = []
    output_samples = {}
    for idx, (sample_id, image_path) in enumerate(images.items(), start=1):
        arr = middle_slice(image_path)
        marr = mask_slice(masks[sample_id], arr.shape) if sample_id in masks else None
        name = public_name(sample_id, idx, anonymize=True) + ".png"
        out_path = os.path.join(args.output, name)
        save_overlay(arr, marr, out_path, max_size)
        rows.append({
            "sample_id": sample_id,
            "qc_id": os.path.splitext(name)[0],
            "file": name,
            "has_mask": bool(marr is not None),
            "width_px": max_size,
        })
        output_samples[sample_id] = {"primary": out_path, "files": [out_path]}

    manifest_path = os.path.join(args.output, "qc_visual_manifest.csv")
    with open(manifest_path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "sample_id", "qc_id", "file", "has_mask", "width_px"
        ])
        writer.writeheader()
        writer.writerows(rows)
    write_json(os.path.join(args.output, "qc_visual_summary.json"), {
        "n_images": len(rows),
        "max_size": max_size,
        "anonymized_names": True,
        "versions": package_versions(["SimpleITK", "numpy", "PIL"]),
    })
    write_collection_output_manifest(
        args.output, "qc_visual_asset", output_samples
    )


if __name__ == "__main__":
    main()
