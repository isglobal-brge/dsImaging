#!/usr/bin/env python3
"""dsImaging feature extraction runner.

Resolves collection assets only from the server-created worker context.
"""

import argparse
import json
import os
import sys

from dsimaging_utils import (
    IMAGE_EXTS,
    MASK_EXTS,
    mapped_sample_files,
    package_versions,
)


def _find_mask_from_manifest(input_dir, sample_id):
    """Read seg_manifest.json and return primary_mask for the sample.

    This is the canonical path for production. The manifest is written
    by the segmentation step and provides an explicit, unambiguous
    mapping from sample_id to mask file(s).
    """
    manifest_path = os.path.join(input_dir, "seg_manifest.json")
    if not os.path.exists(manifest_path):
        return None
    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
        sample_entry = manifest.get("samples", {}).get(sample_id)
        if sample_entry and sample_entry.get("primary_mask"):
            root = os.path.realpath(input_dir)
            mask = os.path.realpath(sample_entry["primary_mask"])
            if os.path.commonpath([root, mask]) == root and os.path.isfile(mask):
                return mask
    except Exception:
        print("  Warning: segmentation manifest could not be read", file=sys.stderr)
    return None


def _selected_features_from_env():
    raw = os.environ.get("DSHPC_CFG_SELECTED_FEATURES", "")
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _filter_selected_features(features, selected_features, sample_id):
    if not selected_features:
        return features
    missing = [name for name in selected_features if name not in features]
    if missing:
        raise ValueError(f"Selected feature(s) missing: {', '.join(missing)}")
    return {name: features[name] for name in selected_features}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--settings", default=None)
    parser.add_argument("--image", default=None,
                        help="Single image path (single-image mode)")
    parser.add_argument("--mask", default=None,
                        help="Single mask path (single-image mode)")
    parser.add_argument("--sample-id", default=None,
                        help="Sample identifier (single-image mode)")
    args = parser.parse_args()

    # Merge CLI args with env vars (dsHPC sets DSHPC_CFG_* from config)
    image = args.image or os.environ.get("DSHPC_CFG_IMAGE")
    mask = args.mask or os.environ.get("DSHPC_CFG_MASK")
    sample_id = getattr(args, "sample_id", None) or os.environ.get("DSHPC_CFG_SAMPLE_ID")

    print("dsImaging extraction")

    # Single-image mode
    if image:
        if not sample_id:
            print("ERROR: Single-image mode requires sample_id", file=sys.stderr)
            sys.exit(1)
        sid = sample_id
        if not mask:
            mask = _find_mask_from_manifest(args.input, sid)
        if not mask:
            print("ERROR: No mask found for the admitted image", file=sys.stderr)
            sys.exit(1)
        pairs = [(image, mask, sid)]
        print("  Single-image mode")
    else:
        image_asset = os.environ.get("DSHPC_CFG_IMAGE_ASSET", "images")
        mask_asset = os.environ.get("DSHPC_CFG_MASK_ASSET", "masks")
        try:
            images = mapped_sample_files(
                image_asset, "images", artifact_types=("image_root",),
                extensions=IMAGE_EXTS,
            )
            masks = dict((sid, path) for path, sid in mapped_sample_files(
                mask_asset, "masks", artifact_types=("mask_root",),
                extensions=MASK_EXTS,
            ))
        except RuntimeError:
            print("ERROR: Admitted imaging inputs are unavailable", file=sys.stderr)
            sys.exit(1)
        pairs = [(path, masks[sid], sid) for path, sid in images]

    print(f"  Found {len(pairs)} image-mask pairs")
    if not pairs:
        print("ERROR: No image-mask pairs found", file=sys.stderr)
        sys.exit(1)

    from radiomics import featureextractor
    import pandas as pd

    if args.settings and args.settings != "default" and os.path.exists(args.settings):
        extractor = featureextractor.RadiomicsFeatureExtractor(args.settings)
    else:
        extractor = featureextractor.RadiomicsFeatureExtractor()

    selected_features = _selected_features_from_env()
    if selected_features:
        print(f"  Selected features: {', '.join(selected_features)}")

    results = []
    failures = 0
    for img, mask, sid in pairs:
        try:
            print("  Extracting admitted image")
            result = extractor.execute(img, mask)
            features = {}
            for k, v in result.items():
                if k.startswith("diagnostics"):
                    continue
                try:
                    features[k] = float(v)
                except (TypeError, ValueError):
                    features[k] = str(v)
            features = _filter_selected_features(features, selected_features, sid)
            features["sample_id"] = sid
            results.append(features)
        except Exception:
            print("  FAILED: admitted image extraction failed", file=sys.stderr)
            failures += 1

    if failures or len(results) != len(pairs):
        print("ERROR: Feature extraction did not complete the admitted roster",
              file=sys.stderr)
        sys.exit(1)

    df = pd.DataFrame(results)
    os.makedirs(args.output, exist_ok=True)
    df.to_parquet(os.path.join(args.output, "radiomics.parquet"), index=False)

    summary = {"n_samples": len(results), "n_features": len(df.columns)-1,
               "format": "parquet", "columns": list(df.columns),
               "versions": package_versions([
                   "radiomics", "SimpleITK", "numpy", "pandas", "pyarrow"
               ])}
    with open(os.path.join(args.output, "extraction_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    print(f"  Saved: {summary['n_samples']} samples x {summary['n_features']} features (parquet)")


if __name__ == "__main__":
    main()
