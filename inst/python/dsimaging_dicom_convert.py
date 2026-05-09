#!/usr/bin/env python3
"""DICOM series to NIfTI conversion runner for dsImaging."""

import argparse
import os
import shutil
import subprocess
import sys

from dsimaging_utils import cfg, resolve_asset_path, safe_id, write_json


def dicom_dirs(root):
    if not root or not os.path.exists(root):
        return []
    out = []
    for base, _, files in os.walk(root):
        if any(name.lower().endswith(".dcm") or "." not in name for name in files):
            out.append(base)
    return sorted(set(out))


def convert_with_dcm2niix(src, output, sid):
    exe = shutil.which("dcm2niix")
    if not exe:
        return None
    cmd = [exe, "-z", "y", "-f", safe_id(sid), "-o", output, src]
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    produced = [
        os.path.join(output, f) for f in os.listdir(output)
        if f.startswith(safe_id(sid)) and f.lower().endswith((".nii", ".nii.gz"))
    ]
    return sorted(produced)[0] if produced else None


def convert_with_simpleitk(src, output, sid):
    import SimpleITK as sitk

    reader = sitk.ImageSeriesReader()
    series_ids = reader.GetGDCMSeriesIDs(src)
    if not series_ids:
        raise RuntimeError(f"No DICOM series found in {src}")
    series_id = series_ids[0]
    files = reader.GetGDCMSeriesFileNames(src, series_id)
    reader.SetFileNames(files)
    image = reader.Execute()
    out_path = os.path.join(output, f"{safe_id(sid)}.nii.gz")
    sitk.WriteImage(image, out_path)
    return out_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    root = resolve_asset_path(cfg("dicom_asset", cfg("image_asset", "images")), "images", cfg("dicom_root"))
    os.makedirs(args.output, exist_ok=True)
    prefer_dcm2niix = str(cfg("converter", "auto")).lower() in ("auto", "dcm2niix")

    manifest = {"runner": "dsimaging_dicom_convert", "samples": {}}
    failures = 0
    dirs = dicom_dirs(root)
    if not dirs:
        print("ERROR: No DICOM series directories found", file=sys.stderr)
        sys.exit(1)
    for src in dirs:
        sid = os.path.basename(src.rstrip(os.sep)) or "series"
        try:
            out_path = None
            if prefer_dcm2niix:
                out_path = convert_with_dcm2niix(src, args.output, sid)
            if out_path is None:
                out_path = convert_with_simpleitk(src, args.output, sid)
            manifest["samples"][sid] = {
                "primary_image": out_path,
                "source": src,
                "status": "done",
            }
        except Exception as exc:
            failures += 1
            manifest["samples"][sid] = {
                "source": src,
                "status": "failed",
                "error": str(exc),
            }

    write_json(os.path.join(args.output, "dicom_conversion_manifest.json"), manifest)
    write_json(os.path.join(args.output, "dicom_conversion_summary.json"), {
        "n_total": len(dirs),
        "n_done": len(dirs) - failures,
        "n_failed": failures,
    })
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
