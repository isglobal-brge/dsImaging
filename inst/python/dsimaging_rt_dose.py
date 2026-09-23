#!/usr/bin/env python3
"""Private per-ROI dose metrics from exactly paired RTDOSE/RTPLAN samples."""

import argparse
import csv
import os
import re
import sys

from dsimaging_utils import (
    MASK_EXTS, cfg, mapped_sample_files, package_versions, worker_context, write_json,
)
from dsimaging_dicom import read_object, validate_dose_plan, dose_image, require_same_geometry


METRICS = ["dose_min", "dose_max", "dose_mean", "dose_std", "dose_voxels",
           "n_beams", "n_fraction_groups", "n_fractions"]


def public_rois():
    """Pair a bounded public vocabulary with exact mask assets and voxel labels."""
    labels, numbers = cfg("roi_labels"), cfg("mask_labels")
    asset, assets = cfg("mask_asset"), cfg("mask_assets")
    if labels is None and numbers is None:
        if assets is not None:
            raise RuntimeError("Multiple dose masks require a public ROI schema")
        return []
    if labels is None or numbers is None or (asset is None) == (assets is None):
        raise RuntimeError("The public dose ROI schema is incomplete")
    labels = [item.strip() for item in labels.split(",")]
    numbers = [item.strip() for item in numbers.split(",")]
    sources = [asset] * len(labels) if asset else [item.strip() for item in assets.split(",")]
    if (not 1 <= len(labels) <= 128 or len(labels) != len(set(labels)) or
            len(numbers) != len(labels) or len(sources) != len(labels) or
            any(not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{0,63}", item) for item in labels) or
            any(not re.fullmatch(r"[1-9][0-9]{0,9}", item) or
                int(item) > 2147483647 for item in numbers) or
            any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", item) or
                ".." in item for item in sources)):
        raise RuntimeError("The public dose ROI schema is invalid")
    if len(set(zip(sources, numbers))) != len(labels):
        raise RuntimeError("The public dose ROI mapping is ambiguous")
    return list(zip(labels, sources, map(int, numbers)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    os.makedirs(args.output, exist_ok=True)
    try:
        import numpy as np
        import SimpleITK as sitk
        rois = public_rois()
        doses = mapped_sample_files(cfg("dose_asset", "rt_dose"), "rt_dose", extensions=(".dcm",))
        plans = dict((sid, path) for path, sid in mapped_sample_files(
            cfg("plan_asset", "rt_plan"), "rt_plan", extensions=(".dcm",)))
        mask_asset = cfg("mask_asset")
        mask_assets = list(dict.fromkeys(item[1] for item in rois)) if rois else (
            [mask_asset] if mask_asset else [])
        masks = {asset: dict((sid, path) for path, sid in mapped_sample_files(
            asset, "masks", artifact_types=("mask_root",), extensions=MASK_EXTS))
            for asset in mask_assets}
        id_col = worker_context()["manifest"].get("metadata", {}).get("id_col", "sample_id")
        roi_col = "roi_label" if rois else "roi"
        rows = []
        for path, sid in doses:
            selected_sources = [(os.path.realpath(masks[asset][sid]), number)
                                for _, asset, number in rois]
            if len(set(selected_sources)) != len(selected_sources):
                raise RuntimeError("The public dose ROI mapping aliases the same source region")
            dose = read_object(path, sid, "RTDOSE")
            plan = read_object(plans[sid], sid, "RTPLAN")
            validate_dose_plan(dose, plan)
            image = dose_image(dose)
            values = sitk.GetArrayFromImage(image)
            if np.any(values < 0):
                raise RuntimeError("RTDOSE contains negative physical dose")
            mask_values_by_asset = {}
            for asset in mask_assets:
                mask_path = masks[asset][sid]
                if rois and mask_path.lower().endswith((".nii", ".nii.gz")):
                    # ITK maps nonfinite NIfTI voxels to zero during decoding.
                    # Validate source values before that lossy conversion.
                    import nibabel as nib
                    source = nib.load(mask_path)
                    qform, qcode = source.get_qform(coded=True)
                    sform, scode = source.get_sform(coded=True)
                    if qcode and scode and not np.allclose(qform, sform, rtol=0, atol=1e-4):
                        raise RuntimeError("An admitted dose mask has ambiguous geometry")
                    source_values = np.asanyarray(source.dataobj)
                    if (not np.all(np.isfinite(source_values)) or np.any(source_values < 0) or
                            np.any(source_values != np.floor(source_values))):
                        raise RuntimeError("An admitted dose mask has invalid labels")
                mask = sitk.ReadImage(mask_path)
                require_same_geometry(image, mask)
                mask_values = sitk.GetArrayFromImage(mask)
                if (mask_values.shape != values.shape or not np.all(np.isfinite(mask_values)) or
                        np.any(mask_values < 0) or
                        (rois and np.any(mask_values != np.floor(mask_values)))):
                    raise RuntimeError("An admitted dose mask has invalid labels")
                mask_values_by_asset[asset] = mask_values
            if rois:
                regions = {label: values[mask_values_by_asset[asset] == number]
                           for label, asset, number in rois}
            else:
                regions = {"whole_grid": values.ravel()}
            if mask_asset and not rois:
                mask_values = mask_values_by_asset[mask_asset]
                selected = values[mask_values > 0]
                if not selected.size:
                    raise RuntimeError("An admitted dose mask is empty")
                regions["mask"] = selected
            fractions = list(getattr(plan, "FractionGroupSequence", []))
            for roi, selected in regions.items():
                rows.append({id_col: sid, roi_col: roi,
                    "dose_min": float(np.min(selected)) if selected.size else None,
                    "dose_max": float(np.max(selected)) if selected.size else None,
                    "dose_mean": float(np.mean(selected)) if selected.size else None,
                    "dose_std": float(np.std(selected)) if selected.size else None,
                    "dose_voxels": int(selected.size), "n_beams": len(getattr(plan, "BeamSequence", [])),
                    "n_fraction_groups": len(fractions),
                    "n_fractions": sum(int(item.NumberOfFractionsPlanned) for item in fractions)})
        with open(os.path.join(args.output, "rt_dose_metrics.csv"), "w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=[id_col, roi_col] + METRICS)
            writer.writeheader()
            writer.writerows(rows)
        write_json(os.path.join(args.output, "rt_plan_summary.json"), {
            "n_samples": len(doses), "n_rows": len(rows),
            "versions": package_versions(["pydicom", "SimpleITK", "numpy", "nibabel"]),
        })
    except Exception:
        print("ERROR: Exact RT dose association or measurement failed", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
