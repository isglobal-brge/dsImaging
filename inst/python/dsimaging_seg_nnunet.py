#!/usr/bin/env python3
"""nnU-Net v2 inference runner for dsImaging.

Uses site-registered nnU-Net model packs for segmentation.
"""
import argparse, json, os, sys

from dsimaging_model_bundles import (
    ModelBundleError, bundle_path, prepare_inference, disable_provider_downloads,
)

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
    parser.add_argument("--model", required=True)
    parser.add_argument("--image", default=None,
                        help="Single image path (single-image mode)")
    parser.add_argument("--sample-id", default=None,
                        help="Sample identifier (single-image mode)")
    args = parser.parse_args()

    print("nnU-Net v2 inference")
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

    try:
        bundle = prepare_inference("nnunetv2", args.model)
        runtime = bundle["manifest"]["runtime"]
        requested_fold = cfg("fold", "all")
        folds = runtime["folds"]
        if requested_fold != "all":
            try:
                selected = int(requested_fold)
            except (TypeError, ValueError):
                raise ModelBundleError("Requested nnU-Net fold is not registered")
            if str(selected) != str(requested_fold) or selected not in folds:
                raise ModelBundleError("Requested nnU-Net fold is not registered")
            folds = [selected]
    except ModelBundleError as exc:
        print("ERROR: Model bundle unavailable: " + str(exc), file=sys.stderr)
        sys.exit(1)
    from nnunetv2.inference.predict_from_raw_data import nnUNetPredictor
    disable_provider_downloads(bundle)
    predictor = nnUNetPredictor()
    predictor.initialize_from_trained_model_folder(
        bundle_path(bundle, runtime["model_folder"]), use_folds=folds,
        checkpoint_name=runtime["checkpoint"])

    # nnU-Net expects a specific input format -- create temp folder
    import shutil, tempfile
    tmpdir = tempfile.mkdtemp()
    tokens = {sid: sample_token(sid) for _, sid in images}
    try:
        for img_path, sample_id in images:
            shutil.copy(
                img_path,
                os.path.join(tmpdir, f"{tokens[sample_id]}_0000.nii.gz"),
            )
        predictor.predict_from_files(tmpdir, args.output)
    finally:
        shutil.rmtree(tmpdir)

    summary = {"n_total": len(images), "model": args.model,
               "model_bundle_manifest_sha256": bundle["manifest_sha256"],
               "versions": package_versions(["nnunetv2", "numpy", "torch"])}
    with open(os.path.join(args.output, "segmentation_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    # Write seg_manifest.json
    seg_manifest = {"provider": "nnunetv2", "model": args.model,
                    "model_bundle_manifest_sha256": bundle["manifest_sha256"], "samples": {}}
    output_samples = {}
    for img_path, sid in images:
        mask_path = os.path.join(args.output, f"{tokens[sid]}.nii.gz")
        if not os.path.isfile(mask_path):
            print("ERROR: Segmentation output is incomplete", file=sys.stderr)
            sys.exit(1)
        seg_manifest["samples"][sid] = {
            "sample_id": sid, "primary_mask": mask_path,
            "mask_files": [mask_path], "status": "done"
        }
        output_samples[sid] = {"primary": mask_path, "files": [mask_path]}
    with open(os.path.join(args.output, "seg_manifest.json"), "w") as f:
        json.dump(seg_manifest, f, indent=2)

    if collection_mode:
        write_collection_output_manifest(args.output, "mask_root", output_samples)

    print(f"  Done: {len(images)} images processed")


if __name__ == "__main__":
    main()
