#!/usr/bin/env python3
"""RTDOSE/RTPLAN summaries and optional dose-in-mask metrics."""

import argparse
import csv
import os
import sys

from dsimaging_utils import cfg, image_files, package_versions, resolve_asset_path, strip_extensions, write_json


def require_pydicom():
    try:
        import pydicom
        return pydicom
    except Exception as exc:
        raise RuntimeError("pydicom is required for RTDOSE/RTPLAN workflows") from exc


def modality(path):
    pydicom = require_pydicom()
    try:
        return str(getattr(pydicom.dcmread(path, stop_before_pixels=True, force=True), "Modality", "")).upper()
    except Exception:
        return ""


def find_modality(root, wanted):
    if not root or not os.path.exists(root):
        return []
    if os.path.isfile(root):
        return [root] if modality(root) == wanted else []
    out = []
    for path in image_files(root, extensions=(".dcm", "")):
        if os.path.isfile(path) and modality(path) == wanted:
            out.append(path)
    return sorted(out)


def dose_array(path):
    pydicom = require_pydicom()
    import numpy as np

    ds = pydicom.dcmread(path, force=True)
    arr = np.asarray(ds.pixel_array).astype("float64")
    scale = float(getattr(ds, "DoseGridScaling", 1.0))
    return arr * scale, ds


def plan_summary(path):
    pydicom = require_pydicom()
    ds = pydicom.dcmread(path, stop_before_pixels=True, force=True)
    beams = getattr(ds, "BeamSequence", []) or []
    fractions = getattr(ds, "FractionGroupSequence", []) or []
    return {
        "plan_file": os.path.basename(path),
        "plan_label": str(getattr(ds, "RTPlanLabel", "")),
        "plan_name": str(getattr(ds, "RTPlanName", "")),
        "n_beams": len(beams),
        "n_fraction_groups": len(fractions),
        "n_fractions": sum(int(getattr(f, "NumberOfFractionsPlanned", 0) or 0) for f in fractions),
    }


def mask_index(mask_root):
    return {strip_extensions(os.path.basename(p)): p for p in image_files(mask_root)} if mask_root else {}


def dose_metrics(dose_path, masks):
    import numpy as np
    import SimpleITK as sitk

    arr, ds = dose_array(dose_path)
    rows = [{
        "dose_file": os.path.basename(dose_path),
        "roi": "whole_grid",
        "dose_min": float(np.min(arr)),
        "dose_max": float(np.max(arr)),
        "dose_mean": float(np.mean(arr)),
        "dose_std": float(np.std(arr)),
        "dose_voxels": int(arr.size),
        "dose_units": str(getattr(ds, "DoseUnits", "")),
        "dose_type": str(getattr(ds, "DoseType", "")),
    }]

    for roi, mask_path in sorted(masks.items()):
        try:
            mask = sitk.GetArrayFromImage(sitk.ReadImage(mask_path)) > 0
        except Exception:
            continue
        if mask.shape != arr.shape:
            continue
        values = arr[mask]
        if values.size == 0:
            continue
        rows.append({
            "dose_file": os.path.basename(dose_path),
            "roi": roi,
            "dose_min": float(np.min(values)),
            "dose_max": float(np.max(values)),
            "dose_mean": float(np.mean(values)),
            "dose_std": float(np.std(values)),
            "dose_voxels": int(values.size),
            "dose_units": str(getattr(ds, "DoseUnits", "")),
            "dose_type": str(getattr(ds, "DoseType", "")),
        })
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    os.makedirs(args.output, exist_ok=True)

    dose_root = resolve_asset_path(cfg("dose_asset", "rt_dose"), "rt_dose", cfg("dose_file", cfg("dose_root")))
    plan_root = resolve_asset_path(cfg("plan_asset", "rt_plan"), "rt_plan", cfg("plan_file", cfg("plan_root")))
    mask_root = resolve_asset_path(cfg("mask_asset", "masks"), "masks", cfg("mask_root"))

    dose_files = find_modality(dose_root, "RTDOSE")
    plan_files = find_modality(plan_root, "RTPLAN")
    if not dose_files:
        print("ERROR: No RTDOSE files found", file=sys.stderr)
        sys.exit(1)

    masks = mask_index(mask_root) if mask_root else {}
    rows = []
    for dose_path in dose_files:
        rows.extend(dose_metrics(dose_path, masks))

    csv_path = os.path.join(args.output, "rt_dose_metrics.csv")
    fields = ["dose_file", "roi", "dose_min", "dose_max", "dose_mean", "dose_std",
              "dose_voxels", "dose_units", "dose_type"]
    with open(csv_path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    write_json(os.path.join(args.output, "rt_plan_summary.json"), {
        "plans": [plan_summary(p) for p in plan_files],
        "n_dose_files": len(dose_files),
        "n_mask_rois": len(masks),
        "table": csv_path,
        "versions": package_versions(["pydicom", "SimpleITK", "numpy"]),
    })


if __name__ == "__main__":
    main()
