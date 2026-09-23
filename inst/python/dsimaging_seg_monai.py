#!/usr/bin/env python3
"""MONAI inference with an exact image and mask contract for every sample."""

import argparse
import json
import os
import sys
import tempfile

from dsimaging_model_bundles import (
    ModelBundleError, bundle_path, prepare_inference, disable_provider_downloads,
    monai_weight_bindings,
)

from dsimaging_utils import (
    IMAGE_EXTS, cfg, mapped_sample_files, package_versions,
    sample_token, validate_input_file, write_collection_output_manifest, write_json,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--image", default=None)
    parser.add_argument("--sample-id", default=None)
    args = parser.parse_args()
    os.makedirs(args.output, exist_ok=True)
    try:
        single_image = args.image or cfg("image")
        sid = args.sample_id or cfg("sample_id")
        collection_mode = not bool(single_image)
        if single_image:
            if not sid:
                raise RuntimeError("Single-image mode requires sample_id")
            validate_input_file(single_image, IMAGE_EXTS)
            images = [(single_image, sid)]
        else:
            images = mapped_sample_files(cfg("image_asset", "images"), "images",
                artifact_types=("image_root",), extensions=IMAGE_EXTS)
        # Materialize admitted inputs before disabling provider network access.
        verified = prepare_inference("monai", args.bundle)
        bundle = verified["path"]
        runtime = verified["manifest"]["runtime"]
        inference_path = bundle_path(verified, runtime["inference_config"])
        weight_bindings = {key: bundle_path(verified, name)
                           for key, name in monai_weight_bindings(runtime).items()}
        import numpy as np
        import SimpleITK as sitk
        from monai.bundle import run
        disable_provider_downloads(verified)
        outputs = {}
        seg_samples = {}
        for image_path, sid in images:
            token = sample_token(sid)
            if image_path.lower().endswith(".dcm"):
                from dsimaging_dicom import read_object
                read_object(image_path, sid)
            source = sitk.ReadImage(image_path)
            with tempfile.TemporaryDirectory(prefix=".monai-", dir=args.output) as stage:
                # A pseudonymous local copy keeps source filenames out of bundle outputs.
                input_path = os.path.join(stage, token + ".nii.gz")
                sitk.WriteImage(source, input_path)
                output_root = os.path.join(stage, "output")
                os.mkdir(output_root)
                run(run_id="run", meta_file=bundle_path(verified, runtime["metadata"]),
                    config_file=inference_path, bundle_root=bundle,
                    image=input_path, output_dir=output_root, **weight_bindings)
                masks = []
                for base, directories, filenames in os.walk(stage):
                    if any(os.path.islink(os.path.join(base, name))
                           for name in directories + filenames):
                        raise RuntimeError("MONAI output contains a symlink")
                    masks.extend(os.path.join(base, name) for name in filenames
                                 if name.lower().endswith((".nii", ".nii.gz")) and
                                 os.path.join(base, name) != input_path)
                if len(masks) != 1 or os.path.islink(masks[0]):
                    raise RuntimeError("MONAI did not produce exactly one mask")
                real = os.path.realpath(masks[0])
                if os.path.commonpath([os.path.realpath(output_root), real]) != os.path.realpath(output_root):
                    raise RuntimeError("MONAI mask leaves the sample output directory")
                mask = sitk.ReadImage(real)
                if (source.GetSize() != mask.GetSize() or
                        not np.allclose(source.GetSpacing(), mask.GetSpacing(), atol=1e-5) or
                        not np.allclose(source.GetOrigin(), mask.GetOrigin(), atol=1e-4) or
                        not np.allclose(source.GetDirection(), mask.GetDirection(), atol=1e-5)):
                    raise RuntimeError("MONAI mask geometry does not match its sample")
                array = sitk.GetArrayFromImage(mask)
                if not np.all(np.isfinite(array)) or np.any(array < 0) or not np.all(array == np.floor(array)):
                    raise RuntimeError("MONAI output is not a discrete segmentation mask")
                final = os.path.join(args.output, token + "_seg.nii.gz")
                sitk.WriteImage(mask, final)
            outputs[sid] = {"primary": final, "files": [final]}
            seg_samples[sid] = {"sample_id": sid, "primary_mask": final,
                                "mask_files": [final], "status": "done"}
        if collection_mode:
            write_collection_output_manifest(args.output, "mask_root", outputs)
        write_json(os.path.join(args.output, "seg_manifest.json"), {
            "provider": "monai", "bundle": args.bundle,
            "model_bundle_manifest_sha256": verified["manifest_sha256"], "samples": seg_samples})
        write_json(os.path.join(args.output, "segmentation_summary.json"), {
            "n_total": len(images), "n_done": len(images), "n_failed": 0,
            "bundle": args.bundle, "model_bundle_manifest_sha256": verified["manifest_sha256"],
            "versions": package_versions(["monai", "SimpleITK", "numpy", "torch"])})
    except ModelBundleError as exc:
        print("ERROR: Model bundle unavailable: " + str(exc), file=sys.stderr)
        sys.exit(1)
    except Exception:
        print("ERROR: Exact MONAI input/output association or inference failed", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
