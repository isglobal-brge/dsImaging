#!/usr/bin/env python3
"""Spatial image operations: registration, cropping, resampling, bias correction."""

import argparse
import os
import sys

from dsimaging_utils import (
    IMAGE_EXTS,
    MASK_EXTS,
    cfg,
    cfg_list,
    mapped_sample_files,
    package_versions,
    sample_token,
    write_collection_output_manifest,
    write_json,
)


def parse_ints(value, default):
    if not value:
        return default
    out = []
    for item in str(value).split(","):
        item = item.strip()
        if item:
            out.append(int(float(item)))
    return out or default


def resample_to_reference(image, reference, interpolator=None):
    import SimpleITK as sitk

    interpolator = interpolator or sitk.sitkLinear
    return sitk.Resample(image, reference, sitk.Transform(), interpolator, 0, image.GetPixelID())


def crop_to_mask(image, mask):
    import SimpleITK as sitk

    if mask.GetSize() != image.GetSize():
        mask = resample_to_reference(mask, image, sitk.sitkNearestNeighbor)
    stats = sitk.LabelShapeStatisticsImageFilter()
    stats.Execute(mask > 0)
    labels = list(stats.GetLabels())
    if not labels:
        return image
    bbox = stats.GetBoundingBox(labels[0])
    index = bbox[: image.GetDimension()]
    size = bbox[image.GetDimension():]
    return sitk.RegionOfInterest(image, size=size, index=index)


def center_crop(image, crop_size):
    import SimpleITK as sitk

    dim = image.GetDimension()
    size = list(image.GetSize())
    crop_size = (crop_size + size)[:dim]
    crop_size = [min(size[i], int(crop_size[i])) for i in range(dim)]
    index = [max(0, int((size[i] - crop_size[i]) / 2)) for i in range(dim)]
    return sitk.RegionOfInterest(image, size=crop_size, index=index)


def n4_bias_correct(image):
    import SimpleITK as sitk

    img = sitk.Cast(image, sitk.sitkFloat32)
    mask = sitk.OtsuThreshold(img, 0, 1, 200)
    corrector = sitk.N4BiasFieldCorrectionImageFilter()
    return corrector.Execute(img, mask)


def register_rigid(image, reference):
    import SimpleITK as sitk

    fixed = sitk.Cast(reference, sitk.sitkFloat32)
    moving = sitk.Cast(image, sitk.sitkFloat32)
    transform = sitk.CenteredTransformInitializer(
        fixed, moving, sitk.Euler3DTransform() if fixed.GetDimension() == 3 else sitk.Euler2DTransform(),
        sitk.CenteredTransformInitializerFilter.GEOMETRY)
    method = sitk.ImageRegistrationMethod()
    method.SetMetricAsMattesMutualInformation(numberOfHistogramBins=32)
    method.SetMetricSamplingStrategy(method.RANDOM)
    method.SetMetricSamplingPercentage(0.1)
    method.SetInterpolator(sitk.sitkLinear)
    method.SetOptimizerAsGradientDescent(learningRate=1.0, numberOfIterations=100,
                                         convergenceMinimumValue=1e-6, convergenceWindowSize=10)
    method.SetOptimizerScalesFromPhysicalShift()
    method.SetInitialTransform(transform, inPlace=False)
    final = method.Execute(fixed, moving)
    return sitk.Resample(image, reference, final, sitk.sitkLinear, 0, image.GetPixelID())


def resample_spacing(image, spacing):
    import SimpleITK as sitk

    if not spacing:
        return image
    dim = image.GetDimension()
    spacing = (spacing + list(image.GetSpacing()))[:dim]
    old_size = image.GetSize()
    old_spacing = image.GetSpacing()
    new_size = [max(1, int(round(old_size[i] * old_spacing[i] / spacing[i]))) for i in range(dim)]
    return sitk.Resample(image, new_size, sitk.Transform(), sitk.sitkLinear,
                         image.GetOrigin(), spacing, image.GetDirection(), 0,
                         image.GetPixelID())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--operations", required=True)
    args = parser.parse_args()
    os.makedirs(args.output, exist_ok=True)

    import SimpleITK as sitk

    image_asset = cfg("image_asset", "images")
    mask_asset = cfg("mask_asset", "")
    reference_asset = cfg("reference_asset", "")
    try:
        images = dict((sid, path) for path, sid in mapped_sample_files(
            image_asset, "images", artifact_types=("image_root",),
            extensions=IMAGE_EXTS,
        ))
        masks = dict((sid, path) for path, sid in mapped_sample_files(
            mask_asset, "masks", artifact_types=("mask_root",),
            extensions=MASK_EXTS,
        )) if mask_asset else {}
        refs = dict((sid, path) for path, sid in mapped_sample_files(
            reference_asset, "images", artifact_types=("image_root",),
            extensions=IMAGE_EXTS,
        )) if reference_asset else {}
    except RuntimeError:
        print("ERROR: Admitted imaging inputs are unavailable", file=sys.stderr)
        sys.exit(1)
    if not images:
        print("ERROR: No images found for spatial processing", file=sys.stderr)
        sys.exit(1)

    operations = [op.strip() for op in args.operations.split(",") if op.strip()]
    if "crop_to_mask" in operations and not masks:
        print("ERROR: crop_to_mask requires an exact mask mapping", file=sys.stderr)
        sys.exit(1)
    if "register_rigid" in operations and not refs:
        print("ERROR: register_rigid requires an exact reference mapping", file=sys.stderr)
        sys.exit(1)
    spacing = [float(x) for x in cfg_list("spacing", [])]
    crop_size = parse_ints(cfg("crop_size", ""), [])

    manifest = []
    output_samples = {}
    for sample_id, path in images.items():
        img = sitk.ReadImage(path)
        for op in operations:
            if op == "crop_to_mask":
                img = crop_to_mask(img, sitk.ReadImage(masks[sample_id]))
            elif op == "center_crop":
                img = center_crop(img, crop_size or [128, 128, 128])
            elif op == "n4_bias":
                img = n4_bias_correct(img)
            elif op == "register_rigid":
                img = register_rigid(img, sitk.ReadImage(refs[sample_id]))
            elif op == "resample":
                img = resample_spacing(img, spacing)
        out_path = os.path.join(args.output, sample_token(sample_id) + ".nii.gz")
        sitk.WriteImage(img, out_path)
        manifest.append({"sample_id": sample_id, "image": out_path, "operations": operations})
        output_samples[sample_id] = {"primary": out_path, "files": [out_path]}

    write_json(os.path.join(args.output, "derived_images_manifest.json"), {
        "images": manifest,
        "n_images": len(manifest),
        "operations": operations,
        "versions": package_versions(["SimpleITK", "numpy"]),
    })
    write_collection_output_manifest(args.output, "image_root", output_samples)


if __name__ == "__main__":
    main()
