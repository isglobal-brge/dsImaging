#!/usr/bin/env python3
"""Convert exactly associated RTSTRUCT ROIs into one binary mask per sample."""

import argparse
import os
import shutil
import sys
import tempfile

from dsimaging_utils import (
    cfg, cfg_list, mapped_sample_files, mapped_sample_groups, package_versions,
    sample_token, write_collection_output_manifest, write_json,
)
from dsimaging_dicom import read_object, read_series, series_image, validate_rtstruct


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    os.makedirs(args.output, exist_ok=True)
    try:
        import numpy as np
        import SimpleITK as sitk
        from rt_utils import RTStructBuilder

        structures = dict((sid, path) for path, sid in mapped_sample_files(
            cfg("rt_asset", cfg("rt_struct_asset", "rt_struct")), "rt_struct",
            extensions=(".dcm",)))
        series = mapped_sample_groups(cfg("dicom_asset", "dicom"), "dicom", (".dcm",))
        selected = cfg_list("rois", [])
        if len(set(selected)) != len(selected):
            raise RuntimeError("ROI selection contains duplicates")
        outputs = {}
        for paths, sid in series:
            paths, objects = read_series(paths, sid)
            rt = read_object(structures[sid], sid, "RTSTRUCT")
            names = validate_rtstruct(rt, objects)
            rois = selected or names
            if not set(rois).issubset(names):
                raise RuntimeError("A selected RTSTRUCT ROI is unavailable")
            reference = series_image(paths)
            # rt-utils takes a directory: isolate only the already verified series.
            with tempfile.TemporaryDirectory(prefix=".rt-series-", dir=args.output) as stage:
                for index, path in enumerate(paths):
                    shutil.copyfile(path, os.path.join(stage, f"{index:06d}.dcm"))
                structure = RTStructBuilder.create_from(stage, structures[sid])
                union = None
                for roi in rois:
                    mask = np.asarray(structure.get_roi_mask_by_name(roi), dtype=bool)
                    if mask.shape != (int(objects[0].Columns), int(objects[0].Rows), len(paths)):
                        raise RuntimeError("RTSTRUCT rasterization geometry is invalid")
                    union = mask if union is None else union | mask
            if union is None or not np.any(union):
                raise RuntimeError("RTSTRUCT rasterization produced an empty mask")
            image = sitk.GetImageFromArray(np.transpose(union, (2, 1, 0)).astype("uint8"))
            if image.GetSize() != reference.GetSize():
                raise RuntimeError("RTSTRUCT rasterization geometry is invalid")
            image.CopyInformation(reference)
            output = os.path.join(args.output, sample_token(sid) + "_rt.nii.gz")
            sitk.WriteImage(image, output)
            outputs[sid] = {"primary": output, "files": [output]}
        write_collection_output_manifest(args.output, "mask_root", outputs)
        write_json(os.path.join(args.output, "rt_conversion_summary.json"), {
            "n_samples": len(outputs), "versions": package_versions(["pydicom", "SimpleITK", "rt_utils"]),
        })
    except Exception:
        print("ERROR: Exact RTSTRUCT association or conversion failed", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
