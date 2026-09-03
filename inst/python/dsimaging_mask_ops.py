#!/usr/bin/env python3
"""Mask and ROI operations for dsImaging."""

import argparse
import os
import sys

from dsimaging_utils import (
    IMAGE_EXTS,
    MASK_EXTS,
    cfg,
    cfg_float,
    cfg_int,
    cfg_list,
    mapped_sample_files,
    package_versions,
    sample_token,
    write_collection_output_manifest,
    write_json,
)


def read_image(path):
    import SimpleITK as sitk
    return sitk.ReadImage(path)


def write_image(image, path):
    import SimpleITK as sitk
    os.makedirs(os.path.dirname(path), exist_ok=True)
    sitk.WriteImage(sitk.Cast(image, sitk.sitkUInt8), path)


def resample_mask(mask, reference):
    import SimpleITK as sitk
    return sitk.Resample(mask, reference, sitk.Transform(), sitk.sitkNearestNeighbor, 0, sitk.sitkUInt8)


def apply_single(mask, operation):
    import SimpleITK as sitk

    op = operation.lower()
    if op == "binarize":
        threshold = cfg_float("threshold", 0)
        return mask > threshold
    if op == "label_select":
        labels = [int(float(x)) for x in cfg_list("labels", [cfg("label", "1")])]
        out = mask == labels[0]
        for label in labels[1:]:
            out = out | (mask == label)
        return out
    if op == "connected_components":
        min_voxels = cfg_int("min_voxels", 1)
        max_components = cfg_int("max_components", 1)
        comps = sitk.ConnectedComponent(mask > 0)
        comps = sitk.RelabelComponent(comps, sortByObjectSize=True, minimumObjectSize=min_voxels)
        out = comps == 1
        for label in range(2, max_components + 1):
            out = out | (comps == label)
        return out
    if op == "morphology":
        mode = str(cfg("mode", "closing")).lower()
        radius = [cfg_int("radius", 1)] * mask.GetDimension()
        binary = mask > cfg_float("threshold", 0)
        if mode in ("closing", "close"):
            return sitk.BinaryMorphologicalClosing(binary, radius)
        if mode in ("opening", "open"):
            return sitk.BinaryMorphologicalOpening(binary, radius)
        if mode in ("dilate", "dilation"):
            return sitk.BinaryDilate(binary, radius)
        if mode in ("erode", "erosion"):
            return sitk.BinaryErode(binary, radius)
        raise ValueError(f"Unsupported morphology mode: {mode}")
    raise ValueError(f"Unsupported single-mask operation: {operation}")


def apply_pair(mask_a, mask_b, operation):
    a = mask_a > cfg_float("threshold", 0)
    b = mask_b > cfg_float("threshold_b", cfg_float("threshold", 0))
    op = operation.lower()
    if op == "union":
        return a | b
    if op == "intersection":
        return a & b
    if op == "difference":
        return a & (b == 0)
    raise ValueError(f"Unsupported pair operation: {operation}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--operation", default=None)
    args = parser.parse_args()

    operation = args.operation or cfg("operation", "binarize")
    operation_l = operation.lower()
    mask_asset = cfg("mask_asset", "masks")
    mask_b_asset = cfg("mask_b_asset", "")
    reference_asset = cfg("reference_asset", "")
    os.makedirs(args.output, exist_ok=True)

    try:
        masks = dict((sid, path) for path, sid in mapped_sample_files(
            mask_asset, "masks", artifact_types=("mask_root",),
            extensions=MASK_EXTS,
        ))
        masks_b = dict((sid, path) for path, sid in mapped_sample_files(
            mask_b_asset, "masks", artifact_types=("mask_root",),
            extensions=MASK_EXTS,
        )) if mask_b_asset else {}
        refs = dict((sid, path) for path, sid in mapped_sample_files(
            reference_asset, "images", artifact_types=("image_root",),
            extensions=IMAGE_EXTS,
        )) if reference_asset else {}
    except RuntimeError:
        print("ERROR: Admitted imaging inputs are unavailable", file=sys.stderr)
        sys.exit(1)

    manifest = {"runner": "dsimaging_mask_ops", "operation": operation, "samples": {}}
    results = []
    output_samples = {}

    if operation_l == "resample_to_image":
        if not refs:
            print("ERROR: resample_to_image requires an exact reference mapping", file=sys.stderr)
            sys.exit(1)
        pairs = [(sid, masks[sid], refs[sid]) for sid in masks]
        for sid, mask_path, ref_path in pairs:
            try:
                out_path = os.path.join(
                    args.output, f"{sample_token(sid)}_mask.nii.gz"
                )
                write_image(resample_mask(read_image(mask_path), read_image(ref_path)), out_path)
                manifest["samples"][sid] = {"primary_mask": out_path, "status": "done"}
                results.append({"sample_id": sid, "status": "done"})
                output_samples[sid] = {"primary": out_path, "files": [out_path]}
            except Exception as exc:
                manifest["samples"][sid] = {"status": "failed", "error": str(exc)}
                results.append({"sample_id": sid, "status": "failed", "error": str(exc)})
    elif operation_l in ("union", "intersection", "difference"):
        if not masks_b:
            print(f"ERROR: {operation_l} requires mask_b_asset or mask_b", file=sys.stderr)
            sys.exit(1)
        for sid, path_a in masks.items():
            try:
                out_path = os.path.join(
                    args.output, f"{sample_token(sid)}_{operation_l}.nii.gz"
                )
                out = apply_pair(
                    read_image(path_a), read_image(masks_b[sid]), operation_l
                )
                write_image(out, out_path)
                manifest["samples"][sid] = {"primary_mask": out_path, "status": "done"}
                results.append({"sample_id": sid, "status": "done"})
                output_samples[sid] = {"primary": out_path, "files": [out_path]}
            except Exception as exc:
                manifest["samples"][sid] = {"status": "failed", "error": str(exc)}
                results.append({"sample_id": sid, "status": "failed", "error": str(exc)})
    else:
        if not masks:
            print("ERROR: No masks found", file=sys.stderr)
            sys.exit(1)
        for sid, path in masks.items():
            try:
                out_path = os.path.join(
                    args.output, f"{sample_token(sid)}_{operation}.nii.gz"
                )
                write_image(apply_single(read_image(path), operation), out_path)
                manifest["samples"][sid] = {"primary_mask": out_path, "status": "done"}
                results.append({"sample_id": sid, "status": "done"})
                output_samples[sid] = {"primary": out_path, "files": [out_path]}
            except Exception as exc:
                manifest["samples"][sid] = {"status": "failed", "error": str(exc)}
                results.append({"sample_id": sid, "status": "failed", "error": str(exc)})

    summary = {
        "n_total": len(results),
        "n_done": sum(1 for r in results if r["status"] == "done"),
        "n_failed": sum(1 for r in results if r["status"] == "failed"),
        "operation": operation,
        "versions": package_versions(["SimpleITK", "numpy"]),
    }
    write_json(os.path.join(args.output, "seg_manifest.json"), manifest)
    write_json(os.path.join(args.output, "mask_ops_summary.json"), summary)
    if summary["n_failed"]:
        sys.exit(1)
    write_collection_output_manifest(args.output, "mask_root", output_samples)


if __name__ == "__main__":
    main()
