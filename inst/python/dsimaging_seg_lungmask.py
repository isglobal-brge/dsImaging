#!/usr/bin/env python3
"""LungMask inference runner for dsImaging.

Produces lung/lobe segmentation masks from CT images.
Models: R231, LTRCLobes, LTRCLobes_R231, R231CovidWeb
"""
import argparse, json, os, sys

from dsimaging_utils import (
    IMAGE_EXTS,
    cfg,
    mapped_sample_files,
    package_versions,
    sample_token,
    validate_input_file,
    write_collection_output_manifest,
)


def find_images():
    return mapped_sample_files(
        cfg("image_asset", "images"), "images",
        artifact_types=("image_root",), extensions=IMAGE_EXTS,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--model", default="R231")
    parser.add_argument("--image", default=None,
                        help="Single image path (single-image mode)")
    parser.add_argument("--sample-id", default=None,
                        help="Sample identifier (single-image mode)")
    args = parser.parse_args()

    print(f"LungMask inference")
    print(f"  Model: {args.model}")

    # Merge CLI args with env vars (dsHPC sets DSHPC_CFG_* from config)
    image = args.image or os.environ.get("DSHPC_CFG_IMAGE")
    sample_id = getattr(args, "sample_id", None) or os.environ.get("DSHPC_CFG_SAMPLE_ID")

    collection_mode = not bool(image)
    if image:
        if not sample_id:
            print("ERROR: Single-image mode requires sample_id", file=sys.stderr)
            sys.exit(1)
        try:
            validate_input_file(image, IMAGE_EXTS)
        except RuntimeError:
            print("ERROR: Admitted imaging inputs are unavailable", file=sys.stderr)
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
    os.makedirs(args.output, exist_ok=True)

    import SimpleITK as sitk
    from lungmask import LMInferer

    inferer = LMInferer(modelname=args.model)
    results = []
    output_samples = {}
    for img_path, sample_id in images:
        try:
            print("  Segmenting admitted image")
            image = sitk.ReadImage(img_path)
            mask = inferer.apply(image)
            # Save mask as NIfTI
            mask_sitk = sitk.GetImageFromArray(mask)
            mask_sitk.CopyInformation(image)
            out_path = os.path.join(
                args.output, f"{sample_token(sample_id)}_lungmask.nii.gz"
            )
            sitk.WriteImage(mask_sitk, out_path)
            results.append({
                "sample_id": sample_id, "status": "done",
                "primary_mask": out_path,
            })
            output_samples[sample_id] = {
                "primary": out_path, "files": [out_path]
            }
        except Exception as e:
            print("  FAILED: admitted image segmentation failed", file=sys.stderr)
            results.append({"sample_id": sample_id, "status": "failed", "error": str(e)})

    summary = {"n_total": len(images), "n_done": sum(1 for r in results if r["status"] == "done"),
               "n_failed": sum(1 for r in results if r["status"] == "failed"), "model": args.model,
               "versions": package_versions(["lungmask", "SimpleITK", "numpy", "torch"])}
    with open(os.path.join(args.output, "segmentation_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    # Write seg_manifest.json
    seg_manifest = {"provider": "lungmask", "model": args.model, "samples": {}}
    for r in results:
        sid = r["sample_id"]
        if r["status"] == "done":
            mask_path = r["primary_mask"]
            seg_manifest["samples"][sid] = {
                "sample_id": sid,
                "primary_mask": mask_path,
                "mask_files": [mask_path],
                "status": "done"
            }
    with open(os.path.join(args.output, "seg_manifest.json"), "w") as f:
        json.dump(seg_manifest, f, indent=2)

    print(f"  Done: {summary['n_done']}/{summary['n_total']}")
    if summary["n_failed"]:
        sys.exit(1)
    if collection_mode:
        write_collection_output_manifest(args.output, "mask_root", output_samples)


if __name__ == "__main__":
    main()
