"""Synthetic DICOM, RT, slide and fake-provider integration fixtures."""

import argparse
import copy
import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import numpy as np
import pydicom
from pydicom.dataset import Dataset, FileDataset, FileMetaDataset
from pydicom.sequence import Sequence
from pydicom.uid import (ExplicitVRLittleEndian, CTImageStorage,
                          RTStructureSetStorage, RTDoseStorage, RTPlanStorage,
                          generate_uid)
from PIL import Image
import SimpleITK as sitk
import yaml

from dsimaging_utils import collection_sample_groups, sample_token
from dsimaging_dicom import series_image


IDS = ["PHI_CASE_A", "PHI_CASE_B", "PHI_CASE_C"]
PATIENTS = ["patient-A", "patient-B", "patient-C"]


def record(path, root, sid, kind="single_file"):
    path = Path(path)
    return {"sample_id": sid, "source_kind": kind, "uri": str(path),
            "relative_path": path.relative_to(root).as_posix(),
            "content_hash": hashlib.sha256(path.read_bytes()).hexdigest(),
            "size": path.stat().st_size, "n_files": 1}


def new_dicom(path, sop_class, modality, patient, study, series, frame):
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = sop_class
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    obj = FileDataset(str(path), {}, file_meta=meta, preamble=b"\0" * 128)
    obj.SOPClassUID = sop_class
    obj.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    obj.Modality = modality
    obj.PatientID = patient
    obj.PatientName = "Synthetic^Private"
    obj.StudyInstanceUID = study
    obj.SeriesInstanceUID = series
    obj.FrameOfReferenceUID = frame
    obj.StudyDate = "20260101"
    obj.StudyTime = "120000"
    obj.StudyID = "1"
    obj.SeriesNumber = 1
    return obj


def ref(sop, sop_class):
    item = Dataset()
    item.ReferencedSOPInstanceUID = sop
    item.ReferencedSOPClassUID = sop_class
    return item


def pixel_fields(obj, array):
    obj.Rows, obj.Columns = array.shape[-2:]
    obj.SamplesPerPixel = 1
    obj.PhotometricInterpretation = "MONOCHROME2"
    obj.BitsAllocated = 16
    obj.BitsStored = 16
    obj.HighBit = 15
    obj.PixelRepresentation = 0
    obj.PixelData = array.astype("<u2").tobytes()


def write_table(path, rows):
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def build_fixture(root):
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    names = ["images", "dicom", "rt_struct", "rt_dose", "rt_plan", "wsi", "masks"]
    records = {name: [] for name in names}
    for name in names:
        (root / name).mkdir()
    for index, (sid, patient) in enumerate(zip(IDS, PATIENTS)):
        study, series, frame = generate_uid(), generate_uid(), generate_uid()
        directory = root / "dicom" / f"source_{index}"
        directory.mkdir()
        slices, paths = [], []
        for z in range(3):
            path = directory / f"slice_{z}.dcm"
            obj = new_dicom(path, CTImageStorage, "CT", patient, study, series, frame)
            obj.InstanceNumber = z + 1
            obj.NumberOfSeriesRelatedInstances = 3
            obj.ImagePositionPatient = [0, 0, z]
            obj.ImageOrientationPatient = [1, 0, 0, 0, 1, 0]
            obj.PixelSpacing = [1, 1]
            obj.SliceThickness = 1
            obj.SpacingBetweenSlices = 1
            obj.RescaleIntercept = -1000
            obj.RescaleSlope = 1
            pixel_fields(obj, np.arange(80, dtype="uint16").reshape(8, 10) + z + 1000)
            obj.save_as(path, enforce_file_format=True)
            slices.append(obj)
            paths.append(str(path))
        entries = [{"path": Path(path).relative_to(root / "dicom").as_posix(),
                    "role": "slice", "size": Path(path).stat().st_size,
                    "content_hash": hashlib.sha256(Path(path).read_bytes()).hexdigest()}
                   for path in paths]
        payload = "".join(f"{item['path']}\t{item['size']}\t{item['content_hash']}\n"
                          for item in sorted(entries, key=lambda value: value["path"]))
        records["dicom"].append({"sample_id": sid, "source_kind": "dicom_series",
            "uri": str(directory), "relative_path": directory.name,
            "content_hash": hashlib.sha256(payload.encode()).hexdigest(),
            "size": sum(item["size"] for item in entries), "n_files": 3, "files": entries})
        image = series_image(paths)
        image_path = root / "images" / f"image_{index}.nii.gz"
        sitk.WriteImage(image, str(image_path))
        records["images"].append(record(image_path, root / "images", sid))
        mask_array = np.zeros((3, 8, 10), dtype="uint8")
        mask_array[1, 2:6, 2:6] = 1
        mask = sitk.GetImageFromArray(mask_array)
        mask.CopyInformation(image)
        mask_path = root / "masks" / f"mask_{index}.nii.gz"
        sitk.WriteImage(mask, str(mask_path))
        records["masks"].append(record(mask_path, root / "masks", sid))

        rt_path = root / "rt_struct" / f"struct_{index}.dcm"
        rt = new_dicom(rt_path, RTStructureSetStorage, "RTSTRUCT", patient,
                       study, generate_uid(), frame)
        frame_item = Dataset()
        frame_item.FrameOfReferenceUID = frame
        study_item = ref(study, "1.2.840.10008.3.1.2.3.1")
        series_item = Dataset()
        series_item.SeriesInstanceUID = series
        series_item.ContourImageSequence = Sequence([ref(obj.SOPInstanceUID, CTImageStorage) for obj in slices])
        study_item.RTReferencedSeriesSequence = Sequence([series_item])
        frame_item.RTReferencedStudySequence = Sequence([study_item])
        rt.ReferencedFrameOfReferenceSequence = Sequence([frame_item])
        roi = Dataset()
        roi.ROINumber = 1
        roi.ROIName = "target"
        roi.ReferencedFrameOfReferenceUID = frame
        roi.ROIGenerationAlgorithm = "MANUAL"
        rt.StructureSetROISequence = Sequence([roi])
        contour = Dataset()
        contour.ContourGeometricType = "CLOSED_PLANAR"
        contour.NumberOfContourPoints = 4
        contour.ContourData = [2, 2, 1, 5, 2, 1, 5, 5, 1, 2, 5, 1]
        contour.ContourImageSequence = Sequence([ref(slices[1].SOPInstanceUID, CTImageStorage)])
        roi_contour = Dataset()
        roi_contour.ReferencedROINumber = 1
        roi_contour.ROIDisplayColor = [255, 0, 0]
        roi_contour.ContourSequence = Sequence([contour])
        rt.ROIContourSequence = Sequence([roi_contour])
        observation = Dataset()
        observation.ObservationNumber = 1
        observation.ReferencedROINumber = 1
        observation.RTROIInterpretedType = "ORGAN"
        observation.ROIInterpreter = ""
        rt.RTROIObservationsSequence = Sequence([observation])
        rt.save_as(rt_path, enforce_file_format=True)
        records["rt_struct"].append(record(rt_path, root / "rt_struct", sid))

        plan_path = root / "rt_plan" / f"plan_{index}.dcm"
        plan = new_dicom(plan_path, RTPlanStorage, "RTPLAN", patient,
                         study, generate_uid(), frame)
        plan.RTPlanLabel = "private_plan"
        plan.RTPlanGeometry = "PATIENT"
        plan.ReferencedStructureSetSequence = Sequence([ref(rt.SOPInstanceUID, RTStructureSetStorage)])
        beam = Dataset()
        beam.BeamNumber = 1
        plan.BeamSequence = Sequence([beam])
        fraction = Dataset()
        fraction.FractionGroupNumber = 1
        fraction.NumberOfFractionsPlanned = 5
        plan.FractionGroupSequence = Sequence([fraction])
        plan.save_as(plan_path, enforce_file_format=True)
        records["rt_plan"].append(record(plan_path, root / "rt_plan", sid))

        dose_path = root / "rt_dose" / f"dose_{index}.dcm"
        dose = new_dicom(dose_path, RTDoseStorage, "RTDOSE", patient,
                         study, generate_uid(), frame)
        dose.NumberOfFrames = 3
        dose.ImagePositionPatient = [0, 0, 0]
        dose.ImageOrientationPatient = [1, 0, 0, 0, 1, 0]
        dose.PixelSpacing = [1, 1]
        dose.GridFrameOffsetVector = [0, 1, 2]
        dose.DoseGridScaling = 0.1
        dose.DoseUnits = "GY"
        dose.DoseType = "PHYSICAL"
        dose.DoseSummationType = "PLAN"
        dose.ReferencedRTPlanSequence = Sequence([ref(plan.SOPInstanceUID, RTPlanStorage)])
        pixel_fields(dose, np.arange(240).reshape(3, 8, 10))
        dose.save_as(dose_path, enforce_file_format=True)
        records["rt_dose"].append(record(dose_path, root / "rt_dose", sid))

        slide_path = root / "wsi" / f"slide_{index}.png"
        Image.new("RGB", (32, 32), (200, 20, 40) if index < 2 else (255, 255, 255)).save(slide_path)
        records["wsi"].append(record(slide_path, root / "wsi", sid))
    kinds = {"images": "image_root", "dicom": "dicom_series_root", "rt_struct": "rt_struct_root",
             "rt_dose": "rt_dose_file", "rt_plan": "rt_plan_file", "wsi": "wsi_root", "masks": "mask_root"}
    assets = {}
    for name in names:
        index_rows, manifest_rows = [], []
        for item in records[name]:
            file_items = item.get("files") or [{"path": item["relative_path"], "role": "primary",
                "size": item["size"], "content_hash": item["content_hash"]}]
            index_rows.append({key: item[key] for key in ("sample_id", "source_kind", "uri", "content_hash", "size")})
            manifest_rows.append({"sample_id": item["sample_id"], "source_kind": item["source_kind"],
                "primary_uri": "" if item["source_kind"] == "dicom_series" else item["relative_path"],
                "files_json": json.dumps(file_items), "content_hash": item["content_hash"], "n_files": item["n_files"]})
        index_path, manifest_path = root / f"{name}_index.csv", root / f"{name}_manifest.csv"
        write_table(index_path, index_rows)
        write_table(manifest_path, manifest_rows)
        assets[name] = {"kind": kinds[name], "uri": str(root / name),
                        "content_hash_index": {"uri": str(index_path), "format": "csv"},
                        "sample_manifests": {"uri": str(manifest_path), "format": "csv"}}
    metadata_path = root / "metadata.csv"
    write_table(metadata_path, [{"sample_id": sid, "patient_id": patient}
                               for sid, patient in zip(IDS, PATIENTS)])
    manifest = {"schema_version": 1, "dataset_id": "synthetic.admission", "modality": "image",
        "metadata": {"uri": str(metadata_path), "format": "csv", "id_col": "sample_id",
                     "privacy_unit": "patient", "privacy_unit_col": "patient_id",
                     "privacy_unit_canonicalization": "trim-utf8-v2"},
        "assets": assets, "sample_manifests": assets["images"]["sample_manifests"],
        "content_hash_index": assets["images"]["content_hash_index"]}
    with open(root / "manifest.yaml", "w") as handle:
        yaml.safe_dump(manifest, handle)
    manifest[".dsimaging_privacy_roster"] = {"sample_ids": IDS, "privacy_ids": PATIENTS}
    context_id = "dsctx_" + "a" * 64
    context = {"schema_version": 1, "context_id": context_id, "manifest": manifest,
               "backend": {"type": "file", "config": {}},
               "collection_map": {"version": 1, "seal": "b" * 64, "asset_names": names,
                                  "records": records["images"], "records_by_asset": records}}
    path = root / (context_id + ".context.yaml")
    with open(path, "w") as handle:
        yaml.safe_dump(context, handle)
    env = {**os.environ, "DSIMAGING_WORKER_CONTEXT_DIR": str(root), "DSHPC_CFG_DATASET_ID": context_id,
           "DSHPC_CFG_WORKER_CONTEXT": str(path), "DSHPC_INPUT_DIR": ""}
    return context, path, env


def run_script(name, root, env, output, *extra):
    script = Path(__import__("dsimaging_utils").__file__).with_name(name)
    return subprocess.run([sys.executable, str(script), "--input", str(root), "--output", str(output), *extra],
                          env=env, capture_output=True, text=True)


def expect_success(result):
    assert result.returncode == 0, result.stderr


def expect_failure(result):
    assert result.returncode != 0, "Runner unexpectedly accepted invalid input"
    assert all(value not in result.stdout + result.stderr for value in IDS + PATIENTS)


def output_map(path, kind):
    result = json.loads((Path(path) / "dsimaging_output_manifest.json").read_text())
    assert result["artifact_type"] == kind
    assert [item["sample_id"] for item in result["samples"]] == IDS
    for item in result["samples"]:
        for info in item["file_integrity"]:
            artifact = Path(path) / info["path"]
            assert info["size"] == artifact.stat().st_size
            assert info["sha256"] == hashlib.sha256(artifact.read_bytes()).hexdigest()
    return result


def save_context(context, path):
    with open(path, "w") as handle:
        yaml.safe_dump(context, handle)


def refresh_record(context, asset, index):
    item = context["collection_map"]["records_by_asset"][asset][index]
    path = Path(item["uri"])
    item["size"] = path.stat().st_size
    item["content_hash"] = hashlib.sha256(path.read_bytes()).hexdigest()


def case_mapping(root, context, path, env):
    os.environ.update(env)
    result = collection_sample_groups("dicom", extensions=(".dcm",))
    assert [sid for _, sid in result] == IDS
    assert all(len(paths) == 3 for paths, _ in result)
    first = Path(result[0][0][0])
    original = first.read_bytes()
    first.write_bytes(original + b"changed")
    try:
        collection_sample_groups("dicom")
        raise AssertionError("Tampered slice accepted")
    except RuntimeError:
        pass
    first.write_bytes(original)
    extra = first.with_name("unexpected.dcm")
    extra.write_bytes(original)
    try:
        collection_sample_groups("dicom")
        raise AssertionError("Extra slice accepted")
    except RuntimeError:
        pass
    extra.unlink()
    first.unlink()
    try:
        collection_sample_groups("dicom")
        raise AssertionError("Missing slice accepted")
    except RuntimeError:
        pass
    first.write_bytes(original)
    alias = first.with_name("symlink.dcm")
    alias.symlink_to(first)
    try:
        collection_sample_groups("dicom")
        raise AssertionError("Symlink accepted")
    except RuntimeError:
        pass
    alias.unlink()
    return {"samples": 3, "verified_files": 9, "negative_cases": 4}


def case_s3_mapping(root, context, path, env):
    import dsimaging_utils as utils

    bucket, prefix = "synthetic-imaging", "sealed/dicom/"
    objects = {}
    expected_downloads = []
    for sample_index, sample in enumerate(context["collection_map"]["records_by_asset"]["dicom"]):
        sample["uri"] = f"s3://{bucket}/{prefix}{sample['relative_path']}"
        for file_index, item in enumerate(sample["files"]):
            key = prefix + item["path"]
            version = f"version-{sample_index}-{file_index}"
            item["version_id"] = version
            objects[(key, version)] = (root / "dicom" / item["path"]).read_bytes()
            expected_downloads.append((bucket, key, version))
    context["backend"] = {"type": "s3", "config": {}}
    context["manifest"]["assets"]["dicom"]["uri"] = f"s3://{bucket}/{prefix}"
    save_context(context, path)
    env["DSIMAGING_CACHE_DIR"] = str(root / "s3-cache")
    os.environ.update(env)

    class FakeS3:
        def __init__(self):
            self.downloads = []
            self.listings = []
            self.listing_mode = "exact"

        def download_file(self, requested_bucket, key, dest, ExtraArgs=None):
            version = (ExtraArgs or {}).get("VersionId")
            self.downloads.append((requested_bucket, key, version))
            assert requested_bucket == bucket
            Path(dest).write_bytes(objects[(key, version)])

        def get_paginator(self, operation):
            assert operation == "list_objects_v2"
            return self

        def paginate(self, Bucket, Prefix):
            assert Bucket == bucket
            self.listings.append(Prefix)
            entries = [{"Key": key, "Size": len(body)}
                       for (key, _), body in objects.items() if key.startswith(Prefix)]
            if self.listing_mode == "extra":
                entries.append({"Key": Prefix + "unexpected.dcm", "Size": 1})
            elif self.listing_mode == "missing":
                entries = entries[:-1]
            yield {"Contents": entries[:1]}
            yield {"Contents": entries[1:]}

    fake = FakeS3()
    utils.s3_client_for_entry = lambda entry: fake
    groups = collection_sample_groups("dicom", extensions=(".dcm",))
    assert [sid for _, sid in groups] == IDS
    assert all(len(paths) == 3 for paths, _ in groups)
    assert fake.downloads == expected_downloads
    assert fake.listings == [prefix + f"source_{index}/" for index in range(3)]
    for (paths, _), sample in zip(groups, context["collection_map"]["records_by_asset"]["dicom"]):
        for local, item in zip(paths, sample["files"]):
            assert Path(local).read_bytes() == objects[(prefix + item["path"], item["version_id"])]
    for mode in ("extra", "missing"):
        fake.listing_mode = mode
        try:
            collection_sample_groups("dicom", extensions=(".dcm",))
            raise AssertionError("Inexact S3 series listing accepted")
        except RuntimeError as error:
            assert "file set" in str(error)
    fake.listing_mode = "exact"
    _, key, version = expected_downloads[0]
    original = objects[(key, version)]
    objects[(key, version)] = bytes([original[0] ^ 1]) + original[1:]
    os.environ["DSIMAGING_CACHE_DIR"] = str(root / "s3-tampered-cache")
    try:
        collection_sample_groups("dicom", extensions=(".dcm",))
        raise AssertionError("Tampered versioned S3 slice accepted")
    except RuntimeError as error:
        assert "integrity verification" in str(error)
    return {"samples": 3, "verified_files": 9, "versioned_downloads": 9,
            "negative_cases": 3}


def case_dicom(root, context, path, env):
    env["DSHPC_CFG_DICOM_ASSET"] = "dicom"
    output = root / "converted"
    expect_success(run_script("dsimaging_dicom_convert.py", root, env, output))
    result = output_map(output, "image_root")
    for sample in result["samples"]:
        assert sitk.ReadImage(str(output / sample["primary"])).GetSize() == (10, 8, 3)
    from dsimaging_dicom import read_series
    os.environ.update(env)
    sources = collection_sample_groups("dicom")
    file_path = Path(sources[0][0][0])
    original = file_path.read_bytes()
    mutations = [lambda obj: setattr(obj, "PatientID", "another-patient"),
                 lambda obj: setattr(obj, "SeriesInstanceUID", generate_uid()),
                 lambda obj: setattr(obj, "NumberOfSeriesRelatedInstances", 4),
                 lambda obj: setattr(obj, "ImagePositionPatient", [0, 0, 1]),
                 lambda obj: setattr(obj, "SOPClassUID", RTDoseStorage)]
    for mutate in mutations:
        obj = pydicom.dcmread(file_path)
        mutate(obj)
        obj.save_as(file_path, enforce_file_format=True)
        try:
            read_series(sources[0][0], IDS[0])
            raise AssertionError("Ambiguous series accepted")
        except RuntimeError:
            pass
        file_path.write_bytes(original)
    return {"samples": 3, "negative_cases": len(mutations)}


def case_rt(root, context, path, env):
    env.update(DSHPC_CFG_RT_ASSET="rt_struct", DSHPC_CFG_DICOM_ASSET="dicom", DSHPC_CFG_ROIS="target")
    output = root / "rt_output"
    expect_success(run_script("dsimaging_rt_convert.py", root, env, output))
    result = output_map(output, "mask_root")
    for sample in result["samples"]:
        assert len(sample["files"]) == 1
        image = sitk.ReadImage(str(output / sample["primary"]))
        assert image.GetSize() == (10, 8, 3)
        assert int(sitk.GetArrayFromImage(image).sum()) == 16
    expect_failure(run_script("dsimaging_rt_convert.py", root,
        {**env, "DSHPC_CFG_ROIS": "absent"}, root / "missing_roi"))
    original = copy.deepcopy(context)
    rt_path = Path(context["collection_map"]["records_by_asset"]["rt_struct"][0]["uri"])
    rt_bytes = rt_path.read_bytes()
    mutations = [lambda obj: setattr(obj, "PatientID", "another-patient"),
                 lambda obj: setattr(obj.ReferencedFrameOfReferenceSequence[0], "FrameOfReferenceUID", generate_uid()),
                 lambda obj: setattr(obj.ROIContourSequence[0].ContourSequence[0].ContourImageSequence[0], "ReferencedSOPInstanceUID", generate_uid()),
                 lambda obj: setattr(obj, "Modality", "SEG"),
                 lambda obj: setattr(obj, "SOPClassUID", RTDoseStorage),
                 lambda obj: setattr(obj.ROIContourSequence[0].ContourSequence[0], "ContourData",
                                     [2, 2, 1, 25, 2, 1, 25, 5, 1, 2, 5, 1])]
    for i, mutate in enumerate(mutations):
        obj = pydicom.dcmread(rt_path)
        mutate(obj)
        obj.save_as(rt_path, enforce_file_format=True)
        refresh_record(context, "rt_struct", 0)
        save_context(context, path)
        expect_failure(run_script("dsimaging_rt_convert.py", root, env, root / f"invalid_rt_{i}"))
        rt_path.write_bytes(rt_bytes)
        context = copy.deepcopy(original)
    save_context(context, path)
    return {"masks": 3, "negative_cases": len(mutations) + 1}


def case_dose(root, context, path, env):
    env.update(DSHPC_CFG_DOSE_ASSET="rt_dose", DSHPC_CFG_PLAN_ASSET="rt_plan", DSHPC_CFG_MASK_ASSET="masks")
    output = root / "dose_output"
    expect_success(run_script("dsimaging_rt_dose.py", root, env, output))
    with open(output / "rt_dose_metrics.csv") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 6
    assert {row["sample_id"] for row in rows} == set(IDS)
    assert {row["roi"] for row in rows} == {"whole_grid", "mask"}
    assert all(float(row["dose_mean"]) >= 0 for row in rows)
    original = copy.deepcopy(context)
    dose_path = Path(context["collection_map"]["records_by_asset"]["rt_dose"][0]["uri"])
    dose_bytes = dose_path.read_bytes()
    mutations = [lambda obj: setattr(obj.ReferencedRTPlanSequence[0], "ReferencedSOPInstanceUID", generate_uid()),
                 lambda obj: setattr(obj, "PatientID", "another-patient"),
                 lambda obj: setattr(obj, "FrameOfReferenceUID", generate_uid()),
                 lambda obj: setattr(obj, "GridFrameOffsetVector", [0, 1, 3])]
    for i, mutate in enumerate(mutations):
        obj = pydicom.dcmread(dose_path)
        mutate(obj)
        obj.save_as(dose_path, enforce_file_format=True)
        refresh_record(context, "rt_dose", 0)
        save_context(context, path)
        expect_failure(run_script("dsimaging_rt_dose.py", root, env, root / f"invalid_dose_{i}"))
        dose_path.write_bytes(dose_bytes)
        context = copy.deepcopy(original)
    save_context(context, path)
    mask_path = context["collection_map"]["records_by_asset"]["masks"][0]["uri"]
    mask = sitk.ReadImage(mask_path)
    mask.SetOrigin((2, 0, 0))
    sitk.WriteImage(mask, mask_path)
    refresh_record(context, "masks", 0)
    save_context(context, path)
    expect_failure(run_script("dsimaging_rt_dose.py", root, env, root / "misplaced_mask"))
    return {"samples": 3, "rows": 6, "negative_cases": len(mutations) + 1}


def case_wsi(root, context, path, env):
    env.update(DSHPC_CFG_WSI_ASSET="wsi", DSHPC_CFG_TILE_SIZE="16", DSHPC_CFG_STRIDE="16", DSHPC_CFG_MAX_TILES="2")
    output = root / "wsi_output"
    expect_success(run_script("dsimaging_wsi_tile.py", root, env, output))
    result = output_map(output, "wsi_tile_root")
    counts = []
    for sample in result["samples"]:
        private = json.loads((output / sample["primary"]).read_text())
        assert private["sample_id"] == sample["sample_id"]
        assert private["n_tiles"] == len(private["tiles"])
        assert len(sample["files"]) == 1 + private["n_tiles"]
        assert not any(sid in name for name in sample["files"] for sid in IDS)
        counts.append(private["n_tiles"])
    assert counts == [2, 2, 0]
    body = Path(context["collection_map"]["records_by_asset"]["wsi"][0]["uri"])
    body.write_bytes(body.read_bytes() + b"changed")
    expect_failure(run_script("dsimaging_wsi_tile.py", root, env, root / "changed_slide"))
    return {"slides": 3, "tiles": 4, "zero_tile_slides": 1, "negative_cases": 1}


def install_fake_monai(root):
    from model_bundles import fake_distribution, make_bundle
    modules = root / "fake_modules" / "monai"
    modules.mkdir(parents=True)
    (modules / "__init__.py").write_text("")
    (modules / "bundle.py").write_text('''import os
import SimpleITK as sitk
def run(**kwargs):
    mode = os.environ.get("FAKE_MONAI_MODE", "one")
    image = sitk.ReadImage(kwargs["image"])
    if mode == "wrong_geometry": image.SetOrigin((99, 99, 99))
    mask = sitk.Cast(image > 0, sitk.sitkUInt8)
    count = 0 if mode == "missing" else (2 if mode in ("multiple", "hidden") else 1)
    for index in range(count):
        name = ".hidden.nii.gz" if mode == "hidden" and index == 1 else f"model_{index}.nii.gz"
        sitk.WriteImage(mask, os.path.join(kwargs["output_dir"], name))
    if mode == "misplaced":
        sitk.WriteImage(mask, os.path.join(os.path.dirname(kwargs["output_dir"]), "outside.nii.gz"))
    if mode == "symlink":
        os.symlink(kwargs["image"], os.path.join(kwargs["output_dir"], "linked.nii.gz"))
''')
    model_root = root / "models"
    make_bundle(model_root, "monai", "synthetic")
    fake_distribution(modules.parent, "monai", "0.0.1")
    return model_root, modules.parent


def case_monai(root, context, path, env):
    model_root, modules_root = install_fake_monai(root)
    env.update(DSIMAGING_MODELS=str(model_root), DSHPC_CFG_IMAGE_ASSET="images",
               PYTHONPATH=str(modules_root) + os.pathsep + env.get("PYTHONPATH", ""))
    output = root / "monai_output"
    expect_success(run_script("dsimaging_seg_monai.py", root, env, output, "--bundle", "synthetic"))
    result = output_map(output, "mask_root")
    assert all(len(item["files"]) == 1 for item in result["samples"])
    for mode in ("missing", "multiple", "wrong_geometry", "hidden", "symlink", "misplaced"):
        expect_failure(run_script("dsimaging_seg_monai.py", root, {**env, "FAKE_MONAI_MODE": mode}, root / mode,
                                  "--bundle", "synthetic"))
    return {"masks": 3, "negative_cases": 6}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--case", choices=("fixture", "mapping", "s3_mapping", "dicom", "rt", "dose", "wsi", "monai"), required=True)
    args = parser.parse_args()
    root = Path(args.root).resolve()
    context, path, env = build_fixture(root)
    if args.case == "fixture":
        install_fake_monai(root)
        result = {"manifest": str(root / "manifest.yaml"), "context": str(path)}
    else:
        result = globals()["case_" + args.case](root, context, path, env)
    print(json.dumps(result))
