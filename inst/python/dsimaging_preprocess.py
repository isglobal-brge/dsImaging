#!/usr/bin/env python3
"""Image preprocessing runner for dsImaging."""

import argparse
import os
import sys

from dsimaging_utils import cfg, cfg_float, cfg_list, image_files, resolve_asset_path, safe_id, strip_extensions, write_json


def parse_spacing(value):
    if value is None:
        return None
    vals = [float(v.strip()) for v in str(value).split(",") if v.strip()]
    return vals or None


def resample(image, spacing):
    import SimpleITK as sitk

    dim = image.GetDimension()
    spacing = spacing[:dim]
    old_spacing = image.GetSpacing()
    old_size = image.GetSize()
    new_size = [
        max(1, int(round(old_size[i] * (old_spacing[i] / spacing[i]))))
        for i in range(dim)
    ]
    return sitk.Resample(
        image,
        new_size,
        sitk.Transform(),
        sitk.sitkLinear,
        image.GetOrigin(),
        spacing,
        image.GetDirection(),
        0,
        image.GetPixelID(),
    )


def normalize_zscore(image):
    import SimpleITK as sitk
    import numpy as np

    arr = sitk.GetArrayFromImage(image).astype("float32")
    mean = float(np.mean(arr))
    std = float(np.std(arr))
    if std > 0:
        arr = (arr - mean) / std
    out = sitk.GetImageFromArray(arr)
    out.CopyInformation(image)
    return out


def clamp(image, lower, upper):
    import SimpleITK as sitk
    return sitk.Clamp(image, lowerBound=float(lower), upperBound=float(upper))


def cast_float32(image):
    import SimpleITK as sitk
    return sitk.Cast(image, sitk.sitkFloat32)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--operations", default=None)
    args = parser.parse_args()

    import SimpleITK as sitk

    operations = cfg_list("operations", []) or [v.strip() for v in str(args.operations or "float32").split(",") if v.strip()]
    root = resolve_asset_path(cfg("image_asset", "images"), "images", cfg("image_root"))
    images = image_files(root)
    if not images:
        print("ERROR: No images found", file=sys.stderr)
        sys.exit(1)
    spacing = parse_spacing(cfg("spacing", cfg("resampled_spacing", "")))
    os.makedirs(args.output, exist_ok=True)

    manifest = {"runner": "dsimaging_preprocess", "operations": operations, "samples": {}}
    results = []
    for path in images:
        sid = strip_extensions(os.path.basename(path))
        try:
            img = sitk.ReadImage(path)
            for op in operations:
                op_l = op.lower()
                if op_l == "resample":
                    if not spacing:
                        raise ValueError("resample requires spacing")
                    img = resample(img, spacing)
                elif op_l in ("normalize", "zscore"):
                    img = normalize_zscore(img)
                elif op_l in ("clamp", "window"):
                    img = clamp(img, cfg_float("lower", -1000), cfg_float("upper", 1000))
                elif op_l in ("float32", "cast_float32"):
                    img = cast_float32(img)
                else:
                    raise ValueError(f"Unsupported preprocessing operation: {op}")
            out_path = os.path.join(args.output, f"{safe_id(sid)}_preprocessed.nii.gz")
            sitk.WriteImage(img, out_path)
            manifest["samples"][sid] = {"primary_image": out_path, "status": "done"}
            results.append({"sample_id": sid, "status": "done"})
        except Exception as exc:
            manifest["samples"][sid] = {"status": "failed", "error": str(exc)}
            results.append({"sample_id": sid, "status": "failed", "error": str(exc)})

    summary = {
        "n_total": len(results),
        "n_done": sum(1 for r in results if r["status"] == "done"),
        "n_failed": sum(1 for r in results if r["status"] == "failed"),
        "operations": operations,
    }
    write_json(os.path.join(args.output, "preprocess_manifest.json"), manifest)
    write_json(os.path.join(args.output, "preprocess_summary.json"), summary)
    if summary["n_failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
