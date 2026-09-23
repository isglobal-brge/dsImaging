#!/usr/bin/env python3
"""Private per-ROI dose metrics from exactly paired RTDOSE/RTPLAN samples."""

import argparse
import csv
import os
import sys

from dsimaging_utils import (
    MASK_EXTS, cfg, mapped_sample_files, package_versions, worker_context, write_json,
)
from dsimaging_dicom import read_object, validate_dose_plan, dose_image, require_same_geometry


METRICS = ["dose_min", "dose_max", "dose_mean", "dose_std", "dose_voxels",
           "n_beams", "n_fraction_groups", "n_fractions"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    os.makedirs(args.output, exist_ok=True)
    try:
        import numpy as np
        import SimpleITK as sitk
        doses = mapped_sample_files(cfg("dose_asset", "rt_dose"), "rt_dose", extensions=(".dcm",))
        plans = dict((sid, path) for path, sid in mapped_sample_files(
            cfg("plan_asset", "rt_plan"), "rt_plan", extensions=(".dcm",)))
        mask_asset = cfg("mask_asset")
        masks = {} if not mask_asset else dict((sid, path) for path, sid in mapped_sample_files(
            mask_asset, "masks", artifact_types=("mask_root",), extensions=MASK_EXTS))
        id_col = worker_context()["manifest"].get("metadata", {}).get("id_col", "sample_id")
        rows = []
        for path, sid in doses:
            dose = read_object(path, sid, "RTDOSE")
            plan = read_object(plans[sid], sid, "RTPLAN")
            validate_dose_plan(dose, plan)
            image = dose_image(dose)
            values = sitk.GetArrayFromImage(image)
            if np.any(values < 0):
                raise RuntimeError("RTDOSE contains negative physical dose")
            regions = {"whole_grid": values.ravel()}
            if mask_asset:
                mask = sitk.ReadImage(masks[sid])
                require_same_geometry(image, mask)
                mask_values = sitk.GetArrayFromImage(mask)
                if not np.all(np.isfinite(mask_values)) or np.any(mask_values < 0):
                    raise RuntimeError("An admitted dose mask has invalid labels")
                selected = values[mask_values > 0]
                if not selected.size:
                    raise RuntimeError("An admitted dose mask is empty")
                regions["mask"] = selected
            fractions = list(getattr(plan, "FractionGroupSequence", []))
            for roi, selected in regions.items():
                rows.append({id_col: sid, "roi": roi,
                    "dose_min": float(np.min(selected)), "dose_max": float(np.max(selected)),
                    "dose_mean": float(np.mean(selected)), "dose_std": float(np.std(selected)),
                    "dose_voxels": int(selected.size), "n_beams": len(getattr(plan, "BeamSequence", [])),
                    "n_fraction_groups": len(fractions),
                    "n_fractions": sum(int(item.NumberOfFractionsPlanned) for item in fractions)})
        with open(os.path.join(args.output, "rt_dose_metrics.csv"), "w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=[id_col, "roi"] + METRICS)
            writer.writeheader()
            writer.writerows(rows)
        write_json(os.path.join(args.output, "rt_plan_summary.json"), {
            "n_samples": len(doses), "n_rows": len(rows),
            "versions": package_versions(["pydicom", "SimpleITK", "numpy"]),
        })
    except Exception:
        print("ERROR: Exact RT dose association or measurement failed", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
