#!/usr/bin/env python3
"""TotalSegmentator inference runner for dsImaging.

Reads images from the server-authorized dsImaging worker context and produces
mask NIfTI files.
"""
import argparse, json, os, sys

from dsimaging_utils import (
    IMAGE_EXTS,
    cfg,
    mapped_sample_files,
    package_versions,
    sample_token,
    write_collection_output_manifest,
)


def find_images():
    """Find only images present in the admitted collection mapping."""
    return mapped_sample_files(
        cfg("image_asset", "images"), "images",
        artifact_types=("image_root",), extensions=IMAGE_EXTS,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--task", default="total")
    parser.add_argument("--image", default=None,
                        help="Single image path (single-image mode)")
    parser.add_argument("--sample-id", default=None,
                        help="Sample identifier (single-image mode)")
    args = parser.parse_args()

    print(f"TotalSegmentator inference")
    print(f"  Task: {args.task}")

    models_dir = os.environ.get("DSIMAGING_MODELS", "/var/lib/dsimaging/models")
    os.environ["TOTALSEG_WEIGHTS_PATH"] = os.path.join(models_dir, "totalsegmentator", args.task)

    # Merge CLI args with env vars (dsHPC sets DSHPC_CFG_* from config)
    image = args.image or os.environ.get("DSHPC_CFG_IMAGE")
    sample_id = getattr(args, "sample_id", None) or os.environ.get("DSHPC_CFG_SAMPLE_ID")
    fast = os.environ.get("DSHPC_CFG_FAST", "").lower() in ("true", "1", "yes")

    # Single-image mode
    collection_mode = not bool(image)
    if image:
        if not sample_id:
            print("ERROR: Single-image mode requires sample_id", file=sys.stderr)
            sys.exit(1)
        sid = sample_id
        images = [(image, sid)]
        print("  Single-image mode")
    else:
        try:
            images = find_images()
        except RuntimeError:
            print("ERROR: Admitted imaging inputs are unavailable", file=sys.stderr)
            sys.exit(1)

    print(f"  Found {len(images)} images")
    if fast:
        print(f"  Using fast mode (3mm resolution)")
    os.makedirs(args.output, exist_ok=True)

    from totalsegmentator.python_api import totalsegmentator

    results = []
    output_samples = {}
    for img_path, sample_id in images:
        try:
            print("  Segmenting admitted image")
            out_dir = os.path.join(args.output, sample_token(sample_id))
            os.makedirs(out_dir, exist_ok=True)
            totalsegmentator(img_path, out_dir, task=args.task, fast=fast)
            masks = sorted(
                os.path.join(out_dir, f) for f in os.listdir(out_dir)
                if f.endswith((".nii.gz", ".nii"))
            )
            if not masks:
                raise RuntimeError("Segmentation produced no masks")
            print(f"  Done: {len(masks)} masks")
            results.append({
                "sample_id": sample_id, "status": "done",
                "n_masks": len(masks), "mask_files": masks,
                "primary_mask": masks[0],
            })
            output_samples[sample_id] = {
                "primary": masks[0], "files": masks
            }
        except Exception as e:
            print("  FAILED: admitted image segmentation failed", file=sys.stderr)
            results.append({"sample_id": sample_id, "status": "failed", "error": str(e)})

    summary = {"n_total": len(images), "n_done": sum(1 for r in results if r["status"] == "done"),
               "n_failed": sum(1 for r in results if r["status"] == "failed"), "task": args.task,
               "versions": package_versions(["totalsegmentator", "SimpleITK", "numpy", "torch"])}
    with open(os.path.join(args.output, "segmentation_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    # Write seg_manifest.json (explicit contract with extraction step)
    seg_manifest = {"provider": "totalsegmentator", "task": args.task, "samples": {}}
    for r in results:
        sid = r["sample_id"]
        if r["status"] == "done":
            masks = r["mask_files"]
            seg_manifest["samples"][sid] = {
                "sample_id": sid,
                "mask_dir": os.path.dirname(r["primary_mask"]),
                "mask_files": masks,
                "primary_mask": r["primary_mask"],
                "status": "done"
            }
    with open(os.path.join(args.output, "seg_manifest.json"), "w") as f:
        json.dump(seg_manifest, f, indent=2)

    print(f"  Done: {summary['n_done']}/{summary['n_total']} ({summary['n_failed']} failed)")

    if summary["n_failed"]:
        sys.exit(1)
    if collection_mode:
        write_collection_output_manifest(args.output, "mask_root", output_samples)


if __name__ == "__main__":
    main()
