#!/usr/bin/env python3
"""Exact single-file or series DICOM to NIfTI conversion runner for dsImaging."""

import argparse
import os
import sys

from dsimaging_utils import (
    cfg,
    mapped_sample_groups,
    package_versions,
    sample_token,
    write_collection_output_manifest,
    write_json,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    asset_name = cfg("dicom_asset", cfg("image_asset", "images"))
    os.makedirs(args.output, exist_ok=True)
    if str(cfg("converter", "simpleitk")).lower() != "simpleitk":
        print("ERROR: Collection DICOM conversion requires simpleitk",
              file=sys.stderr)
        sys.exit(1)
    try:
        samples = mapped_sample_groups(
            asset_name, "images", extensions=(".dcm",),
        )
    except RuntimeError:
        print("ERROR: Admitted imaging inputs are unavailable", file=sys.stderr)
        sys.exit(1)
    if not samples:
        print("ERROR: No admitted DICOM samples found",
              file=sys.stderr)
        sys.exit(1)

    manifest = {"runner": "dsimaging_dicom_convert", "samples": {}}
    failures = 0
    output_samples = {}
    import SimpleITK as sitk
    from dsimaging_dicom import read_object, read_series, series_image
    for sources, sid in samples:
        try:
            if len(sources) == 1:
                read_object(sources[0], sid)
                image = sitk.ReadImage(sources[0])
            else:
                sources, _ = read_series(sources, sid)
                image = series_image(sources)
            out_path = os.path.join(args.output, sample_token(sid) + ".nii.gz")
            sitk.WriteImage(image, out_path)
            manifest["samples"][sid] = {
                "primary_image": out_path,
                "status": "done",
            }
            output_samples[sid] = {"primary": out_path, "files": [out_path]}
        except Exception as exc:
            failures += 1
            manifest["samples"][sid] = {
                "status": "failed",
                "error": str(exc),
            }

    write_json(os.path.join(args.output, "dicom_conversion_manifest.json"), manifest)
    write_json(os.path.join(args.output, "dicom_conversion_summary.json"), {
        "n_total": len(samples),
        "n_done": len(samples) - failures,
        "n_failed": failures,
        "versions": package_versions(["SimpleITK"]),
    })
    if failures:
        sys.exit(1)
    write_collection_output_manifest(args.output, "image_root", output_samples)


if __name__ == "__main__":
    main()
