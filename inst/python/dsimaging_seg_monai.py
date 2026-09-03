#!/usr/bin/env python3
"""MONAI bundle inference runner for dsImaging.

Uses MONAI Model Zoo bundles for segmentation.
"""
import argparse, json, os, sys

from dsimaging_utils import (
    image_files,
    package_versions,
    resolve_asset_path,
    sample_token,
    strip_extensions,
)


def find_images(input_dir):
    root = resolve_asset_path("images", "images")
    images = image_files(root)
    if images:
        return [(path, strip_extensions(os.path.basename(path))) for path in images]
    return [(os.path.join(input_dir, f), os.path.splitext(f)[0])
            for f in sorted(os.listdir(input_dir))
            if not f.startswith(".") and os.path.isfile(os.path.join(input_dir, f))]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--image", default=None,
                        help="Single image path (single-image mode)")
    parser.add_argument("--sample-id", default=None,
                        help="Sample identifier (single-image mode)")
    args = parser.parse_args()

    models_dir = os.environ.get("DSIMAGING_MODELS", "/var/lib/dsimaging/models")
    bundle_dir = os.path.join(models_dir, "monai", args.bundle)

    print(f"MONAI bundle inference")
    print(f"  Bundle: {args.bundle}")
    print(f"  Bundle path: {bundle_dir}")

    if not os.path.isdir(bundle_dir):
        print(f"ERROR: Bundle not found at {bundle_dir}", file=sys.stderr)
        print("Install with: dsImaging::install_model('monai', '<bundle_name>')", file=sys.stderr)
        sys.exit(1)

    # Merge CLI args with env vars (dsHPC sets DSHPC_CFG_* from config)
    image = args.image or os.environ.get("DSHPC_CFG_IMAGE")
    sample_id = getattr(args, "sample_id", None) or os.environ.get("DSHPC_CFG_SAMPLE_ID")

    if image:
        sid = sample_id or os.path.splitext(os.path.basename(image))[0]
        images = [(image, sid)]
        print("  Single-image mode")
    else:
        images = find_images(args.input)

    print(f"  Found {len(images)} images")
    os.makedirs(args.output, exist_ok=True)

    from monai.bundle import run

    results = []
    for img_path, sample_id in images:
        try:
            print("  Inferring admitted image")
            out_path = os.path.join(
                args.output, f"{sample_token(sample_id)}_seg.nii.gz"
            )
            run(
                runner_id="inference",
                meta_file=os.path.join(bundle_dir, "configs", "metadata.json"),
                config_file=os.path.join(bundle_dir, "configs", "inference.json"),
                logging_file=os.path.join(bundle_dir, "configs", "logging.conf"),
                bundle_root=bundle_dir,
                image=img_path,
                output_dir=os.path.dirname(out_path),
            )
            results.append({"sample_id": sample_id, "status": "done"})
        except Exception as e:
            print("  FAILED: admitted image inference failed", file=sys.stderr)
            results.append({"sample_id": sample_id, "status": "failed", "error": str(e)})

    summary = {"n_total": len(images), "n_done": sum(1 for r in results if r["status"] == "done"),
               "n_failed": sum(1 for r in results if r["status"] == "failed"), "bundle": args.bundle,
               "versions": package_versions(["monai", "SimpleITK", "numpy", "torch"])}
    with open(os.path.join(args.output, "segmentation_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    # Write seg_manifest.json
    seg_manifest = {"provider": "monai", "bundle": args.bundle, "samples": {}}
    for r in results:
        sid = r["sample_id"]
        if r["status"] == "done":
            mask_path = os.path.join(
                args.output, f"{sample_token(sid)}_seg.nii.gz"
            )
            seg_manifest["samples"][sid] = {
                "sample_id": sid, "primary_mask": mask_path,
                "mask_files": [mask_path], "status": "done"
            }
    with open(os.path.join(args.output, "seg_manifest.json"), "w") as f:
        json.dump(seg_manifest, f, indent=2)

    print(f"  Done: {summary['n_done']}/{summary['n_total']}")


if __name__ == "__main__":
    main()
