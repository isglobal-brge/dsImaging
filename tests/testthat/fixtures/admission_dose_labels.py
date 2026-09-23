"""Synthetic public-schema labelled dose measurements and refusal fixtures."""

import argparse
import copy
import csv
import json
from pathlib import Path

import numpy as np
import nibabel as nib
import pydicom
from pydicom.uid import generate_uid
import SimpleITK as sitk
import yaml

from admission_contract import (
    IDS, build_fixture, expect_failure, expect_success, record, refresh_record,
    run_script, save_context, write_table,
)


LABELS = ["tumour", "organ_at_risk", "absent"]
MEASUREMENTS = ["dose_min", "dose_max", "dose_mean", "dose_std"]


def build_labelled_fixture(root):
    context, path, env = build_fixture(root)
    for name in ("tumour_masks", "oar_masks"):
        (root / name).mkdir()
    for name in ("masks", "tumour_masks", "oar_masks"):
        records = []
        for index, sid in enumerate(IDS):
            reference = sitk.ReadImage(str(root / "images" / f"image_{index}.nii.gz"))
            labels = np.zeros((3, 8, 10), dtype="uint16")
            if index < 2:
                labels[0, 0, :3] = 1
            labels[1, 2, 3:7] = 2
            labels[2, 7, 9] = 99
            if name != "masks":
                labels = (labels == (1 if name == "tumour_masks" else 2)).astype("uint8")
            mask = sitk.GetImageFromArray(labels)
            mask.CopyInformation(reference)
            output = root / name / f"mask_{index}.nii.gz"
            sitk.WriteImage(mask, str(output))
            records.append(record(output, root / name, sid))
        index_rows = [{key: item[key] for key in
                       ("sample_id", "source_kind", "uri", "content_hash", "size")}
                      for item in records]
        manifest_rows = [{"sample_id": item["sample_id"], "source_kind": "single_file",
                          "primary_uri": item["relative_path"],
                          "files_json": json.dumps([{
                              "path": item["relative_path"], "role": "primary",
                              "size": item["size"], "content_hash": item["content_hash"]}]),
                          "content_hash": item["content_hash"], "n_files": 1}
                         for item in records]
        index_path = root / f"{name}_index.csv"
        manifest_path = root / f"{name}_manifest.csv"
        write_table(index_path, index_rows)
        write_table(manifest_path, manifest_rows)
        context["manifest"]["assets"][name] = {
            "kind": "mask_root", "uri": str(root / name),
            "content_hash_index": {"uri": str(index_path), "format": "csv"},
            "sample_manifests": {"uri": str(manifest_path), "format": "csv"}}
        if name not in context["collection_map"]["asset_names"]:
            context["collection_map"]["asset_names"].append(name)
        context["collection_map"]["records_by_asset"][name] = records
    manifest = copy.deepcopy(context["manifest"])
    manifest.pop(".dsimaging_privacy_roster")
    with open(root / "manifest.yaml", "w") as handle:
        yaml.safe_dump(manifest, handle)
    save_context(context, path)
    env.update(DSHPC_CFG_DOSE_ASSET="rt_dose", DSHPC_CFG_PLAN_ASSET="rt_plan",
               DSHPC_CFG_MASK_ASSET="masks", DSHPC_CFG_ROI_LABELS=",".join(LABELS),
               DSHPC_CFG_MASK_LABELS="1,2,7")
    return context, path, env


def read_rows(output):
    with open(output / "rt_dose_metrics.csv") as handle:
        rows = list(csv.DictReader(handle))
    assert list(rows[0]) == ["sample_id", "roi_label"] + MEASUREMENTS + [
        "dose_voxels", "n_beams", "n_fraction_groups", "n_fractions"]
    return rows


def verify_rows(rows, labels):
    assert [(row["sample_id"], row["roi_label"]) for row in rows] == [
        (sid, label) for sid in IDS for label in labels]
    for row in rows:
        label = row["roi_label"]
        if label == "absent" or (label == "tumour" and row["sample_id"] == IDS[2]):
            assert all(row[field] == "" for field in MEASUREMENTS)
            assert row["dose_voxels"] == "0"
        else:
            expected = np.array([0, 0.1, 0.2]) if label == "tumour" else np.array([10.3, 10.4, 10.5, 10.6])
            assert np.allclose([float(row[field]) for field in MEASUREMENTS],
                               [expected.min(), expected.max(), expected.mean(), expected.std()])
            assert int(row["dose_voxels"]) == expected.size
        assert [row[field] for field in ("n_beams", "n_fraction_groups", "n_fractions")] == ["1", "1", "5"]


def case_labels(root, context, path, env):
    output = root / "labelled_output"
    expect_success(run_script("dsimaging_rt_dose.py", root, env, output))
    rows = read_rows(output)
    verify_rows(rows, LABELS)
    assert len(rows) == 9
    assert sum(row["dose_voxels"] == "0" for row in rows) == 4
    # Undeclared positive values cannot alter the public vocabulary or measurements.
    for index, item in enumerate(context["collection_map"]["records_by_asset"]["masks"]):
        mask = sitk.ReadImage(item["uri"])
        labels = sitk.GetArrayFromImage(mask)
        labels[labels == 99] = 123
        changed = sitk.GetImageFromArray(labels)
        changed.CopyInformation(mask)
        sitk.WriteImage(changed, item["uri"])
        refresh_record(context, "masks", index)
    save_context(context, path)
    ignored_output = root / "ignored_private_labels"
    expect_success(run_script("dsimaging_rt_dose.py", root, env, ignored_output))
    assert read_rows(ignored_output) == rows
    # NIfTI scaling participates in the integer voxel label definition.
    for index, item in enumerate(context["collection_map"]["records_by_asset"]["masks"]):
        mask = nib.load(item["uri"])
        scaled = nib.Nifti1Image(np.asanyarray(mask.dataobj), mask.affine, header=mask.header)
        scaled.header.set_slope_inter(2, 0)
        nib.save(scaled, item["uri"])
        refresh_record(context, "masks", index)
    save_context(context, path)
    scaled_output = root / "scaled_labels"
    expect_success(run_script("dsimaging_rt_dose.py", root,
                              {**env, "DSHPC_CFG_MASK_LABELS": "2,4,14"}, scaled_output))
    assert read_rows(scaled_output) == rows
    # Every region can be absent without changing success or row shape.
    for index, item in enumerate(context["collection_map"]["records_by_asset"]["masks"]):
        mask = sitk.ReadImage(item["uri"])
        empty = sitk.Image(mask.GetSize(), sitk.sitkUInt8)
        empty.CopyInformation(mask)
        sitk.WriteImage(empty, item["uri"])
        refresh_record(context, "masks", index)
    save_context(context, path)
    empty_output = root / "empty_labels"
    expect_success(run_script("dsimaging_rt_dose.py", root, env, empty_output))
    empty_rows = read_rows(empty_output)
    assert len(empty_rows) == 9
    assert all(row["dose_voxels"] == "0" and
               all(row[field] == "" for field in MEASUREMENTS) for row in empty_rows)
    return {"samples": 3, "rows": 9, "missing_rows": 4,
            "empty_rows": 9, "ignored_private_labels": True, "scaled_labels": True}


def case_sets(root, context, path, env):
    env = {**env, "DSHPC_CFG_MASK_ASSET": "", "DSHPC_CFG_MASK_ASSETS": "tumour_masks,oar_masks",
           "DSHPC_CFG_ROI_LABELS": "tumour,organ_at_risk", "DSHPC_CFG_MASK_LABELS": "1,1"}
    output = root / "mask_set_output"
    expect_success(run_script("dsimaging_rt_dose.py", root, env, output))
    rows = read_rows(output)
    verify_rows(rows, LABELS[:2])
    failures = 0
    original = copy.deepcopy(context)
    for asset in ("tumour_masks", "oar_masks"):
        # Each independently selected mask must verify all hashes and the full roster.
        filename = Path(context["collection_map"]["records_by_asset"][asset][0]["uri"])
        content = filename.read_bytes()
        filename.write_bytes(content + b"changed")
        expect_failure(run_script("dsimaging_rt_dose.py", root, env, root / f"hash_{asset}"))
        filename.write_bytes(content)
        failures += 1
        context["collection_map"]["records_by_asset"][asset].pop()
        save_context(context, path)
        expect_failure(run_script("dsimaging_rt_dose.py", root, env, root / f"roster_{asset}"))
        failures += 1
        context = copy.deepcopy(original)
        save_context(context, path)
    return {"samples": 3, "rows": 6, "mask_assets": 2, "negative_cases": failures}


def case_failures(root, context, path, env):
    cases = [
        {"ROI_LABELS": ""}, {"MASK_LABELS": ""}, {"MASK_ASSET": ""},
        {"ROI_LABELS": "tumour,tumour,absent"}, {"ROI_LABELS": "private label,organ,absent"},
        {"ROI_LABELS": "a,b"}, {"MASK_LABELS": "1,2,0"}, {"MASK_LABELS": "1,2,-1"},
        {"MASK_LABELS": "1,2,1.5"}, {"MASK_LABELS": "1,2,2147483648"},
        {"MASK_LABELS": "1,1,7"}, {"MASK_ASSETS": "masks,masks,masks"},
        {"MASK_ASSET": "", "MASK_ASSETS": "tumour_masks,oar_masks"},
        {"ROI_LABELS": ",".join(f"roi{i}" for i in range(129)),
         "MASK_LABELS": ",".join(str(i + 1) for i in range(129))},
        {"ROI_LABELS": "", "MASK_LABELS": "", "MASK_ASSET": "", "MASK_ASSETS": "masks"},
    ]
    failures = 0
    for index, update in enumerate(cases):
        trial = {**env, **{"DSHPC_CFG_" + key: value for key, value in update.items()}}
        expect_failure(run_script("dsimaging_rt_dose.py", root, trial, root / f"schema_{index}"))
        failures += 1
    original = copy.deepcopy(context)
    context["manifest"]["assets"]["alias_masks"] = copy.deepcopy(context["manifest"]["assets"]["masks"])
    context["collection_map"]["asset_names"].append("alias_masks")
    context["collection_map"]["records_by_asset"]["alias_masks"] = copy.deepcopy(
        context["collection_map"]["records_by_asset"]["masks"])
    save_context(context, path)
    alias_env = {**env, "DSHPC_CFG_MASK_ASSET": "", "DSHPC_CFG_MASK_ASSETS": "masks,alias_masks,masks",
                 "DSHPC_CFG_MASK_LABELS": "1,1,7"}
    expect_failure(run_script("dsimaging_rt_dose.py", root, alias_env, root / "aliased_region"))
    failures += 1
    context = copy.deepcopy(original)
    for mode in ("missing", "duplicate", "unmatched"):
        records = context["collection_map"]["records_by_asset"]["masks"]
        if mode == "missing":
            records.pop()
        elif mode == "duplicate":
            records[1]["sample_id"] = records[0]["sample_id"]
        else:
            records[1]["sample_id"] = "unexpected-sample"
        save_context(context, path)
        expect_failure(run_script("dsimaging_rt_dose.py", root, env, root / mode))
        failures += 1
        context = copy.deepcopy(original)
    dose_path = Path(context["collection_map"]["records_by_asset"]["rt_dose"][0]["uri"])
    dose_bytes = dose_path.read_bytes()
    mutations = [lambda obj: setattr(obj, "PatientID", "another-patient"),
                 lambda obj: setattr(obj, "StudyInstanceUID", generate_uid()),
                 lambda obj: setattr(obj, "FrameOfReferenceUID", generate_uid()),
                 lambda obj: setattr(obj.ReferencedRTPlanSequence[0], "ReferencedSOPInstanceUID", generate_uid())]
    for index, mutate in enumerate(mutations):
        obj = pydicom.dcmread(dose_path)
        mutate(obj)
        obj.save_as(dose_path, enforce_file_format=True)
        refresh_record(context, "rt_dose", 0)
        save_context(context, path)
        expect_failure(run_script("dsimaging_rt_dose.py", root, env, root / f"association_{index}"))
        failures += 1
        dose_path.write_bytes(dose_bytes)
        context = copy.deepcopy(original)
    filename = Path(context["collection_map"]["records_by_asset"]["masks"][0]["uri"])
    content = filename.read_bytes()
    # A large patient-coordinate origin must not widen the geometry tolerance.
    dose = pydicom.dcmread(dose_path)
    dose.ImagePositionPatient = [100000, 0, 0]
    dose.save_as(dose_path, enforce_file_format=True)
    refresh_record(context, "rt_dose", 0)
    mask = sitk.ReadImage(str(filename))
    mask.SetOrigin((100000, 0, 0))
    sitk.WriteImage(mask, str(filename))
    refresh_record(context, "masks", 0)
    save_context(context, path)
    expect_success(run_script("dsimaging_rt_dose.py", root, env, root / "matched_large_origin"))
    mask.SetOrigin((100000.5, 0, 0))
    assert np.allclose(dose.ImagePositionPatient, mask.GetOrigin(), atol=1e-4)
    assert not np.allclose(dose.ImagePositionPatient, mask.GetOrigin(), rtol=0, atol=1e-4)
    sitk.WriteImage(mask, str(filename))
    refresh_record(context, "masks", 0)
    save_context(context, path)
    expect_failure(run_script("dsimaging_rt_dose.py", root, env, root / "mismatched_large_origin"))
    failures += 1
    dose_path.write_bytes(dose_bytes)
    filename.write_bytes(content)
    context = copy.deepcopy(original)
    for index, mode in enumerate(("hash", "origin", "spacing", "fractional", "negative", "nan", "infinite",
                                 "forms", "nan_mha", "infinite_mha")):
        written_filename = filename
        if mode == "hash":
            filename.write_bytes(content + b"changed")
        elif mode == "forms":
            mask = nib.load(filename)
            affine = mask.affine.copy()
            affine[0, 3] += 2
            mask.set_qform(affine, code=1)
            nib.save(mask, filename)
            refresh_record(context, "masks", 0)
        else:
            mask = sitk.ReadImage(str(filename))
            if mode == "origin":
                mask.SetOrigin((2, 0, 0))
            elif mode == "spacing":
                mask.SetSpacing((2, 1, 1))
            else:
                labels = sitk.GetArrayFromImage(mask).astype(float)
                labels[0, 0, 0] = {"fractional": 1.5, "negative": -1, "nan": np.nan,
                                    "infinite": np.inf}[mode.removesuffix("_mha")]
                changed = sitk.GetImageFromArray(labels)
                changed.CopyInformation(mask)
                mask = changed
            if mode.endswith("_mha"):
                written_filename = filename.with_name("invalid_mask.mha")
                item = context["collection_map"]["records_by_asset"]["masks"][0]
                item["uri"] = str(written_filename)
                item["relative_path"] = written_filename.name
            sitk.WriteImage(mask, str(written_filename))
            refresh_record(context, "masks", 0)
        save_context(context, path)
        expect_failure(run_script("dsimaging_rt_dose.py", root, env, root / f"mask_{index}"))
        failures += 1
        filename.write_bytes(content)
        if written_filename != filename:
            written_filename.unlink()
        context = copy.deepcopy(original)
    save_context(context, path)
    return {"negative_cases": failures, "schema_refusals": len(cases)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--case", choices=("fixture", "labels", "sets", "failures"), required=True)
    args = parser.parse_args()
    root = Path(args.root).resolve()
    context, path, env = build_labelled_fixture(root)
    if args.case == "fixture":
        result = {"manifest": str(root / "manifest.yaml"), "context": str(path)}
    else:
        result = globals()["case_" + args.case](root, context, path, env)
    print(json.dumps(result))
