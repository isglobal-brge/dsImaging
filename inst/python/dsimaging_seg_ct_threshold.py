#!/usr/bin/env python3
"""Fast CT lung threshold segmentation runner for dsImaging.

This runner is intentionally dependency-light: it uses SimpleITK and NumPy,
which are already present in the PyRadiomics environment. It is suitable for
demo/QC workflows where a deterministic whole-lung mask is enough to exercise
the segmentation -> radiomics pipeline without requiring PyTorch.
"""

import argparse
import json
import os
import sys


def _strip_extensions(filename):
    for ext in (".nii.gz", ".nii", ".nrrd", ".mha", ".mhd", ".dcm"):
        if filename.lower().endswith(ext):
            return filename[: -len(ext)]
    return os.path.splitext(filename)[0]


def find_images(input_dir):
    return [(os.path.join(input_dir, f), _strip_extensions(f))
            for f in sorted(os.listdir(input_dir))
            if not f.startswith(".")
            and os.path.isfile(os.path.join(input_dir, f))]


def _component_touches_border(bbox, size):
    x, y, z, sx, sy, sz = bbox
    return (
        x <= 0 or y <= 0 or z <= 0 or
        x + sx >= size[0] or
        y + sy >= size[1] or
        z + sz >= size[2]
    )


def segment_image(image, threshold, max_components, min_voxels):
    import SimpleITK as sitk

    candidate = image < float(threshold)
    components = sitk.ConnectedComponent(candidate)
    components = sitk.RelabelComponent(
        components,
        sortByObjectSize=True,
        minimumObjectSize=int(min_voxels),
    )

    stats = sitk.LabelShapeStatisticsImageFilter()
    stats.Execute(components)
    labels = list(stats.GetLabels())
    if not labels:
        raise RuntimeError("No threshold components found")

    size = image.GetSize()
    selected = [
        label for label in labels
        if not _component_touches_border(stats.GetBoundingBox(label), size)
    ]
    if not selected:
        # Fallback for tightly cropped CTs where lung air reaches the volume
        # edge. Keep the largest non-background components.
        selected = labels

    selected = selected[:int(max_components)]
    mask = components == int(selected[0])
    for label in selected[1:]:
        mask = mask | (components == int(label))

    mask = sitk.BinaryMorphologicalClosing(mask, [2, 2, 1])
    mask = sitk.Cast(mask, sitk.sitkUInt8)
    mask.CopyInformation(image)
    return mask


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--threshold", type=float, default=-320)
    parser.add_argument("--max-components", type=int, default=2)
    parser.add_argument("--min-voxels", type=int, default=1000)
    parser.add_argument("--image", default=None)
    parser.add_argument("--sample-id", default=None)
    args = parser.parse_args()

    image = args.image or os.environ.get("DSJOBS_CFG_IMAGE")
    sample_id = args.sample_id or os.environ.get("DSJOBS_CFG_SAMPLE_ID")
    threshold = float(os.environ.get("DSJOBS_CFG_THRESHOLD", args.threshold))
    max_components = int(os.environ.get(
        "DSJOBS_CFG_MAX_COMPONENTS", args.max_components))
    min_voxels = int(os.environ.get("DSJOBS_CFG_MIN_VOXELS", args.min_voxels))

    if image:
        sid = sample_id or _strip_extensions(os.path.basename(image))
        images = [(image, sid)]
        print(f"CT threshold segmentation")
        print(f"  Single-image mode: {sid}")
    else:
        images = find_images(args.input)
        print("CT threshold segmentation")

    print(f"  Threshold: {threshold}")
    print(f"  Found {len(images)} images")
    os.makedirs(args.output, exist_ok=True)

    import SimpleITK as sitk

    results = []
    manifest = {
        "provider": "ct_lung_threshold",
        "threshold": threshold,
        "samples": {},
    }

    for img_path, sid in images:
        try:
            print(f"  Segmenting: {sid}")
            img = sitk.ReadImage(img_path)
            mask = segment_image(img, threshold, max_components, min_voxels)
            out_path = os.path.join(args.output, f"{sid}_ct_lung_mask.nii.gz")
            sitk.WriteImage(mask, out_path)
            manifest["samples"][sid] = {
                "sample_id": sid,
                "primary_mask": out_path,
                "mask_files": [out_path],
                "status": "done",
            }
            results.append({"sample_id": sid, "status": "done"})
        except Exception as exc:
            print(f"  FAILED {sid}: {exc}", file=sys.stderr)
            results.append({
                "sample_id": sid,
                "status": "failed",
                "error": str(exc),
            })

    summary = {
        "n_total": len(images),
        "n_done": sum(1 for r in results if r["status"] == "done"),
        "n_failed": sum(1 for r in results if r["status"] == "failed"),
        "provider": "ct_lung_threshold",
        "threshold": threshold,
    }
    with open(os.path.join(args.output, "segmentation_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    with open(os.path.join(args.output, "seg_manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)

    if summary["n_failed"]:
        sys.exit(1)
    print(f"  Done: {summary['n_done']}/{summary['n_total']}")


if __name__ == "__main__":
    main()
