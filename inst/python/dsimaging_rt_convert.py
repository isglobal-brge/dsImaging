#!/usr/bin/env python3
"""Convert RTSTRUCT or DICOM SEG objects into mask assets."""

import argparse
import csv
import os
import sys

from dsimaging_utils import cfg, cfg_list, image_files, package_versions, resolve_asset_path, safe_id, write_json


def require_pydicom():
    try:
        import pydicom
        return pydicom
    except Exception as exc:
        raise RuntimeError("pydicom is required for RTSTRUCT/DICOM SEG conversion") from exc


def dicom_modality(path):
    pydicom = require_pydicom()
    try:
        ds = pydicom.dcmread(path, stop_before_pixels=True, force=True)
        return str(getattr(ds, "Modality", "")).upper()
    except Exception:
        return ""


def find_dicom(root, modalities):
    if not root or not os.path.exists(root):
        return []
    paths = image_files(root, extensions=(".dcm", ""))
    out = []
    for path in paths:
        if os.path.isdir(path):
            continue
        if dicom_modality(path) in modalities:
            out.append(path)
    return sorted(out)


def sitk_reference_from_series(series_dir):
    import SimpleITK as sitk

    if not series_dir or not os.path.isdir(series_dir):
        return None
    reader = sitk.ImageSeriesReader()
    series_ids = reader.GetGDCMSeriesIDs(series_dir) or []
    if not series_ids:
        return None
    files = reader.GetGDCMSeriesFileNames(series_dir, series_ids[0])
    reader.SetFileNames(files)
    return reader.Execute()


def mask_to_sitk(mask, reference):
    import numpy as np
    import SimpleITK as sitk

    arr = np.asarray(mask).astype("uint8")
    if reference is not None:
        ref_shape = tuple(reversed(reference.GetSize()))
        if arr.shape != ref_shape and arr.ndim == 3 and arr.shape == reference.GetSize():
            arr = np.transpose(arr, (2, 1, 0))
    img = sitk.GetImageFromArray(arr)
    if reference is not None and img.GetSize() == reference.GetSize():
        img.CopyInformation(reference)
    return img


def convert_rtstruct(rt_path, series_dir, output_dir, selected_rois):
    try:
        from rt_utils import RTStructBuilder
    except Exception as exc:
        raise RuntimeError("rt-utils is required to rasterize RTSTRUCT contours") from exc

    import SimpleITK as sitk

    if not series_dir or not os.path.isdir(series_dir):
        raise RuntimeError("RTSTRUCT conversion requires a reference DICOM series")
    rtstruct = RTStructBuilder.create_from(dicom_series_path=series_dir, rt_struct_path=rt_path)
    roi_names = rtstruct.get_roi_names()
    if selected_rois:
        roi_names = [r for r in roi_names if r in selected_rois]
    if not roi_names:
        raise RuntimeError("No matching RTSTRUCT ROIs found")

    reference = sitk_reference_from_series(series_dir)
    rows = []
    for roi in roi_names:
        mask = rtstruct.get_roi_mask_by_name(roi)
        img = mask_to_sitk(mask, reference)
        out_name = safe_id(roi) + ".nii.gz"
        out_path = os.path.join(output_dir, out_name)
        sitk.WriteImage(img, out_path)
        rows.append({
            "source": os.path.basename(rt_path),
            "source_type": "RTSTRUCT",
            "roi": roi,
            "mask": out_path,
        })
    return rows


def convert_dicom_seg(seg_path, reference_path, output_dir, selected_rois):
    pydicom = require_pydicom()
    import numpy as np
    import SimpleITK as sitk

    ds = pydicom.dcmread(seg_path, force=True)
    arr = ds.pixel_array
    arr = np.asarray(arr)
    if arr.ndim == 2:
        arr = arr[None, :, :]
    segments = getattr(ds, "SegmentSequence", [])
    frame_segments = []
    for fg in getattr(ds, "PerFrameFunctionalGroupsSequence", []) or []:
        seq = getattr(fg, "SegmentIdentificationSequence", []) or []
        frame_segments.append(int(getattr(seq[0], "ReferencedSegmentNumber", 1)) if seq else 1)
    if len(frame_segments) != (arr.shape[0] if arr.ndim >= 3 else 1):
        frame_segments = []
    rows = []
    for idx, seg in enumerate(segments or [None]):
        label = getattr(seg, "SegmentLabel", None) if seg is not None else None
        label = str(label or f"segment_{idx + 1}")
        if selected_rois and label not in selected_rois:
            continue
        seg_number = int(getattr(seg, "SegmentNumber", idx + 1)) if seg is not None else idx + 1
        if arr.ndim == 4:
            mask_arr = arr[:, idx, :, :] > 0
        elif frame_segments:
            keep = np.asarray(frame_segments) == seg_number
            mask_arr = arr[keep, :, :] > 0
        else:
            mask_arr = arr > 0
        img = sitk.GetImageFromArray(mask_arr.astype("uint8"))
        if reference_path and os.path.exists(reference_path):
            try:
                ref = sitk.ReadImage(reference_path)
                if ref.GetSize() == img.GetSize():
                    img.CopyInformation(ref)
            except Exception:
                pass
        out_path = os.path.join(output_dir, safe_id(label) + ".nii.gz")
        sitk.WriteImage(img, out_path)
        rows.append({
            "source": os.path.basename(seg_path),
            "source_type": "DICOM_SEG",
            "roi": label,
            "mask": out_path,
        })
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    os.makedirs(args.output, exist_ok=True)

    rt_root = resolve_asset_path(
        cfg("rt_asset", cfg("rt_struct_asset", "rt_struct")),
        "rt_struct",
        cfg("rt_root", cfg("rt_file"))
    )
    series_dir = resolve_asset_path(
        cfg("dicom_asset", "dicom"),
        "dicom",
        cfg("dicom_root")
    )
    reference_image = resolve_asset_path(
        cfg("reference_asset", cfg("image_asset", "images")),
        "images",
        cfg("reference_image")
    )
    selected_rois = cfg_list("rois", [])

    rtstructs = find_dicom(rt_root, {"RTSTRUCT"})
    segs = find_dicom(rt_root, {"SEG"})
    if os.path.isfile(rt_root):
        modality = dicom_modality(rt_root)
        if modality == "RTSTRUCT":
            rtstructs = [rt_root]
        elif modality == "SEG":
            segs = [rt_root]

    rows = []
    for path in rtstructs:
        rows.extend(convert_rtstruct(path, series_dir, args.output, selected_rois))
    for path in segs:
        rows.extend(convert_dicom_seg(path, reference_image, args.output, selected_rois))

    if not rows:
        print("ERROR: No RTSTRUCT or DICOM SEG masks were converted", file=sys.stderr)
        sys.exit(1)

    manifest_csv = os.path.join(args.output, "rt_mask_manifest.csv")
    with open(manifest_csv, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["source", "source_type", "roi", "mask"])
        writer.writeheader()
        writer.writerows(rows)
    write_json(os.path.join(args.output, "rt_conversion_summary.json"), {
        "n_masks": len(rows),
        "n_rtstruct": len(rtstructs),
        "n_dicom_seg": len(segs),
        "versions": package_versions(["pydicom", "SimpleITK", "rt_utils", "highdicom"]),
    })


if __name__ == "__main__":
    main()
