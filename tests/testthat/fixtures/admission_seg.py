"""Synthetic binary SEG admission, exact-reference and geometry fixtures."""

import argparse
import copy
import json
import os
from pathlib import Path

import numpy as np
import pydicom
from pydicom.dataset import Dataset
from pydicom.pixels import pack_bits
from pydicom.sequence import Sequence
from pydicom.uid import SegmentationStorage, RTDoseStorage, generate_uid
import SimpleITK as sitk
import yaml

from admission_contract import (
    IDS, PATIENTS, build_fixture, expect_failure, expect_success, new_dicom,
    output_map, record, ref, refresh_record, run_script, save_context, write_table,
)
from dsimaging_dicom import read_series, seg_mask
from dsimaging_utils import mapped_sample_files, mapped_sample_groups


def code(value, meaning):
    item = Dataset()
    item.CodeValue, item.CodingSchemeDesignator, item.CodeMeaning = value, "DCM", meaning
    return item


def build_seg_fixture(root):
    root = Path(root)
    context, context_path, env = build_fixture(root)
    directory = root / "rt_seg"
    directory.mkdir()
    records = []
    for index, (sid, patient) in enumerate(zip(IDS, PATIENTS)):
        sources = [pydicom.dcmread(root / "dicom" / f"source_{index}" / f"slice_{z}.dcm")
                   for z in range(3)]
        first = sources[0]
        path = directory / f"seg_{index}.dcm"
        obj = new_dicom(path, SegmentationStorage, "SEG", patient,
                        first.StudyInstanceUID, generate_uid(), first.FrameOfReferenceUID)
        obj.ImageType = ["DERIVED", "PRIMARY"]
        obj.ContentLabel = "SYNTHETIC"
        obj.ContentDate = "20260101"
        obj.ContentTime = "120000"
        obj.InstanceNumber = 1
        obj.SegmentationType = "BINARY"
        obj.SegmentsOverlap = "YES"
        obj.LossyImageCompression = "00"
        obj.Rows, obj.Columns = 8, 10
        obj.SamplesPerPixel = 1
        obj.PhotometricInterpretation = "MONOCHROME2"
        obj.PixelRepresentation = 0
        obj.BitsAllocated = obj.BitsStored = 1
        obj.HighBit = 0
        segments = []
        for number, label in ((1, "target"), (7, "organ")):
            segment = Dataset()
            segment.SegmentNumber, segment.SegmentLabel = number, label
            segment.SegmentAlgorithmType = "MANUAL"
            category = code("T-D0050", "Tissue")
            category.CodingSchemeDesignator = "SRT"
            segment.SegmentedPropertyCategoryCodeSequence = Sequence([category])
            segment.SegmentedPropertyTypeCodeSequence = Sequence([copy.deepcopy(category)])
            segments.append(segment)
        obj.SegmentSequence = Sequence(segments)
        refs = Dataset()
        refs.SeriesInstanceUID = first.SeriesInstanceUID
        refs.ReferencedInstanceSequence = Sequence([
            ref(source.SOPInstanceUID, source.SOPClassUID) for source in sources])
        obj.ReferencedSeriesSequence = Sequence([refs])
        shared, measures, orientation = Dataset(), Dataset(), Dataset()
        measures.PixelSpacing = first.PixelSpacing
        measures.SliceThickness = measures.SpacingBetweenSlices = 1
        orientation.ImageOrientationPatient = first.ImageOrientationPatient
        shared.PixelMeasuresSequence = Sequence([measures])
        shared.PlaneOrientationSequence = Sequence([orientation])
        obj.SharedFunctionalGroupsSequence = Sequence([shared])
        organization = Dataset()
        organization.DimensionOrganizationUID = generate_uid()
        obj.DimensionOrganizationSequence = Sequence([organization])
        dimensions = []
        for pointer, group in ((0x0062000B, 0x0062000A), (0x00200032, 0x00209113)):
            dimension = Dataset()
            dimension.DimensionOrganizationUID = organization.DimensionOrganizationUID
            dimension.DimensionIndexPointer, dimension.FunctionalGroupPointer = pointer, group
            dimensions.append(dimension)
        obj.DimensionIndexSequence = Sequence(dimensions)
        frames, pixels = [], []
        # Deliberately nonspatial frame order; the empty target slices are omitted.
        for number, z, rows, columns in ((7, 2, slice(0, 2), slice(0, 3)),
                                         (1, 1, slice(2, 6), slice(2, 6)),
                                         (7, 0, slice(0, 2), slice(0, 2)),
                                         (7, 1, slice(4, 7), slice(4, 7))):
            frame, identification, position, derivation = Dataset(), Dataset(), Dataset(), Dataset()
            identification.ReferencedSegmentNumber = number
            position.ImagePositionPatient = sources[z].ImagePositionPatient
            source = ref(sources[z].SOPInstanceUID, sources[z].SOPClassUID)
            source.SpatialLocationsPreserved = "YES"
            source.PurposeOfReferenceCodeSequence = Sequence([
                code("121322", "Source Image for Image Processing Operation")])
            derivation.SourceImageSequence = Sequence([source])
            derivation.DerivationCodeSequence = Sequence([code("113076", "Segmentation")])
            frame.SegmentIdentificationSequence = Sequence([identification])
            frame.PlanePositionSequence = Sequence([position])
            frame.DerivationImageSequence = Sequence([derivation])
            content = Dataset()
            content.DimensionIndexValues = [1 if number == 1 else 2, z + 1]
            frame.FrameContentSequence = Sequence([content])
            frames.append(frame)
            pixels_frame = np.zeros((8, 10), dtype="uint8")
            pixels_frame[rows, columns] = 1
            pixels.append(pixels_frame)
        obj.PerFrameFunctionalGroupsSequence = Sequence(frames)
        obj.NumberOfFrames = len(frames)
        obj.PixelData = pack_bits(np.asarray(pixels))
        obj.save_as(path, enforce_file_format=True)
        records.append(record(path, directory, sid))
    index_path, manifest_path = root / "rt_seg_index.csv", root / "rt_seg_manifest.csv"
    write_table(index_path, [{key: item[key] for key in
                             ("sample_id", "source_kind", "uri", "content_hash", "size")}
                            for item in records])
    write_table(manifest_path, [{"sample_id": item["sample_id"], "source_kind": "single_file",
        "primary_uri": item["relative_path"], "files_json": json.dumps([
            {"path": item["relative_path"], "role": "primary", "size": item["size"],
             "content_hash": item["content_hash"]}]),
        "content_hash": item["content_hash"], "n_files": 1} for item in records])
    context["manifest"]["assets"]["rt_seg"] = {"kind": "rt_seg_root", "uri": str(directory),
        "content_hash_index": {"uri": str(index_path), "format": "csv"},
        "sample_manifests": {"uri": str(manifest_path), "format": "csv"}}
    context["collection_map"]["records_by_asset"]["rt_seg"] = records
    context["collection_map"]["asset_names"].append("rt_seg")
    save_context(context, context_path)
    manifest = copy.deepcopy(context["manifest"])
    manifest.pop(".dsimaging_privacy_roster")
    with open(root / "manifest.yaml", "w") as handle:
        yaml.safe_dump(manifest, handle)
    env.update(DSHPC_CFG_RT_ASSET="rt_seg", DSHPC_CFG_DICOM_ASSET="dicom")
    return context, context_path, env


def case_seg(root, context, context_path, env):
    os.environ.update(env)
    outputs = 0
    for selection, count in (({}, 31), ({"DSHPC_CFG_ROIS": "target"}, 16),
                             ({"DSHPC_CFG_ROIS": "organ"}, 19),
                             ({"DSHPC_CFG_SEGMENT_NUMBERS": "1"}, 16),
                             ({"DSHPC_CFG_SEGMENT_NUMBERS": "7"}, 19),
                             ({"DSHPC_CFG_SEGMENT_NUMBERS": "7,1"}, 31),
                             ({"DSHPC_CFG_ROIS": "organ,target"}, 31)):
        output = root / f"positive_{outputs}"
        execution = run_script("dsimaging_rt_convert.py", root, {**env, **selection}, output)
        expect_success(execution)
        assert all(value not in execution.stdout + execution.stderr for value in IDS + PATIENTS)
        result = output_map(output, "mask_root")
        for sample in result["samples"]:
            assert len(sample["files"]) == 1
            image = sitk.ReadImage(str(output / sample["primary"]))
            assert image.GetSize() == (10, 8, 3)
            assert image.GetSpacing() == (1, 1, 1)
            assert image.GetOrigin() == (0, 0, 0)
            mask = sitk.GetArrayFromImage(image)
            assert set(np.unique(mask)) == {0, 1}
            assert int(mask.sum()) == count
            if count == 16:
                assert not np.any(mask[0]) and not np.any(mask[2])
                assert np.all(mask[1, 2:6, 2:6] == 1)
        outputs += 1
    negative = 0
    for selection in ({"DSHPC_CFG_ROIS": "absent"},
                      {"DSHPC_CFG_SEGMENT_NUMBERS": "2"},
                      {"DSHPC_CFG_SEGMENT_NUMBERS": "0"},
                      {"DSHPC_CFG_SEGMENT_NUMBERS": "1.0"},
                      {"DSHPC_CFG_SEGMENT_NUMBERS": "-1"},
                      {"DSHPC_CFG_SEGMENT_NUMBERS": "65536"},
                      {"DSHPC_CFG_SEGMENT_NUMBERS": "1,1"},
                      {"DSHPC_CFG_ROIS": "target,target"},
                      {"DSHPC_CFG_ROIS": "target", "DSHPC_CFG_SEGMENT_NUMBERS": "1"},
                      {"DSHPC_CFG_RT_ASSET": "rt_struct", "DSHPC_CFG_SEGMENT_NUMBERS": "1"}):
        expect_failure(run_script("dsimaging_rt_convert.py", root, {**env, **selection},
                                  root / f"invalid_selection_{negative}"))
        negative += 1
    path = Path(context["collection_map"]["records_by_asset"]["rt_seg"][0]["uri"])
    original_bytes, original_context = path.read_bytes(), copy.deepcopy(context)
    mutations = [
        lambda obj: setattr(obj, "PatientID", "another-patient"),
        lambda obj: setattr(obj, "StudyInstanceUID", generate_uid()),
        lambda obj: setattr(obj, "FrameOfReferenceUID", generate_uid()),
        lambda obj: setattr(obj, "Modality", "RTSTRUCT"),
        lambda obj: setattr(obj, "SOPClassUID", RTDoseStorage),
        lambda obj: setattr(obj.file_meta, "MediaStorageSOPInstanceUID", generate_uid()),
        lambda obj: setattr(obj, "SegmentationType", "FRACTIONAL"),
        lambda obj: setattr(obj, "SegmentationType", "LABELMAP"),
        lambda obj: setattr(obj, "BitsAllocated", 8),
        lambda obj: setattr(obj, "Rows", 9),
        lambda obj: setattr(obj, "NumberOfFrames", 5),
        lambda obj: setattr(obj, "PixelData", b"\x00\x00"),
        lambda obj: setattr(obj.ReferencedSeriesSequence[0], "SeriesInstanceUID", generate_uid()),
        lambda obj: obj.ReferencedSeriesSequence.append(copy.deepcopy(obj.ReferencedSeriesSequence[0])),
        lambda obj: obj.ReferencedSeriesSequence[0].ReferencedInstanceSequence.pop(),
        lambda obj: obj.ReferencedSeriesSequence[0].ReferencedInstanceSequence.append(
            copy.deepcopy(obj.ReferencedSeriesSequence[0].ReferencedInstanceSequence[0])),
        lambda obj: setattr(obj.ReferencedSeriesSequence[0].ReferencedInstanceSequence[0],
                            "ReferencedSOPInstanceUID", generate_uid()),
        lambda obj: setattr(obj.ReferencedSeriesSequence[0].ReferencedInstanceSequence[0],
                            "ReferencedSOPClassUID", RTDoseStorage),
        lambda obj: setattr(obj, "StudiesContainingOtherReferencedInstancesSequence", Sequence([Dataset()])),
        lambda obj: setattr(obj, "SourceImageSequence", Sequence([ref(generate_uid(), RTDoseStorage)])),
        lambda obj: setattr(obj.SegmentSequence[0], "SegmentNumber", 0),
        lambda obj: setattr(obj.SegmentSequence[1], "SegmentNumber", 1),
        lambda obj: setattr(obj.SegmentSequence[1], "SegmentLabel", "target"),
        lambda obj: setattr(obj.SegmentSequence[0], "SegmentLabel", ""),
        lambda obj: obj.SegmentSequence.pop(),
        lambda obj: obj.SharedFunctionalGroupsSequence.append(Dataset()),
        lambda obj: obj.PerFrameFunctionalGroupsSequence.pop(),
        lambda obj: setattr(obj.PerFrameFunctionalGroupsSequence[0].SegmentIdentificationSequence[0],
                            "ReferencedSegmentNumber", 2),
        lambda obj: obj.PerFrameFunctionalGroupsSequence[0].SegmentIdentificationSequence.append(Dataset()),
        lambda obj: setattr(obj.PerFrameFunctionalGroupsSequence[0].DerivationImageSequence[0],
                            "SourceImageSequence", Sequence([])),
        lambda obj: obj.PerFrameFunctionalGroupsSequence[0].DerivationImageSequence[0].SourceImageSequence.append(
            copy.deepcopy(obj.PerFrameFunctionalGroupsSequence[0].DerivationImageSequence[0].SourceImageSequence[0])),
        lambda obj: setattr(obj.PerFrameFunctionalGroupsSequence[0].DerivationImageSequence[0].SourceImageSequence[0],
                            "ReferencedSOPInstanceUID", generate_uid()),
        lambda obj: setattr(obj.PerFrameFunctionalGroupsSequence[0].DerivationImageSequence[0].SourceImageSequence[0],
                            "ReferencedSOPClassUID", RTDoseStorage),
        lambda obj: setattr(obj.PerFrameFunctionalGroupsSequence[0].DerivationImageSequence[0].SourceImageSequence[0],
                            "ReferencedFrameNumber", 2),
        lambda obj: setattr(obj.PerFrameFunctionalGroupsSequence[0].DerivationImageSequence[0].SourceImageSequence[0],
                            "SpatialLocationsPreserved", "NO"),
        lambda obj: setattr(obj.PerFrameFunctionalGroupsSequence[0].PlanePositionSequence[0],
                            "ImagePositionPatient", [0, 0, 0]),
        lambda obj: setattr(obj.PerFrameFunctionalGroupsSequence[0].PlanePositionSequence[0],
                            "ImagePositionPatient", [1, 0, 2]),
        lambda obj: setattr(obj.SharedFunctionalGroupsSequence[0].PlaneOrientationSequence[0],
                            "ImageOrientationPatient", [-1, 0, 0, 0, 1, 0]),
        lambda obj: setattr(obj.SharedFunctionalGroupsSequence[0].PixelMeasuresSequence[0], "PixelSpacing", [2, 1]),
        lambda obj: setattr(obj.SharedFunctionalGroupsSequence[0].PixelMeasuresSequence[0], "SpacingBetweenSlices", 2),
        lambda obj: setattr(obj.SharedFunctionalGroupsSequence[0].PixelMeasuresSequence[0], "SliceThickness", 2),
        lambda obj: setattr(obj.PerFrameFunctionalGroupsSequence[0], "PixelMeasuresSequence",
                            copy.deepcopy(obj.SharedFunctionalGroupsSequence[0].PixelMeasuresSequence)),
        lambda obj: delattr(obj.SharedFunctionalGroupsSequence[0], "PlaneOrientationSequence"),
        lambda obj: obj.PerFrameFunctionalGroupsSequence.__setitem__(0, copy.deepcopy(obj.PerFrameFunctionalGroupsSequence[2])),
        lambda obj: setattr(obj.PerFrameFunctionalGroupsSequence[0].DerivationImageSequence[0],
                            "StudyInstanceUID", generate_uid()),
    ]
    for index, mutate in enumerate(mutations):
        obj = pydicom.dcmread(path)
        mutate(obj)
        # write_like_original preserves deliberate file-meta identity disagreement.
        pydicom.dcmwrite(path, obj, enforce_file_format=False)
        refresh_record(context, "rt_seg", 0)
        save_context(context, context_path)
        execution = run_script("dsimaging_rt_convert.py", root, env, root / f"invalid_seg_{index}")
        assert execution.returncode != 0, f"SEG mutation {index} was accepted"
        expect_failure(execution)
        path.write_bytes(original_bytes)
        context = copy.deepcopy(original_context)
        negative += 1
    save_context(context, context_path)
    source_paths = mapped_sample_groups("dicom", extensions=(".dcm",))[0][0]
    _, series = read_series(source_paths, IDS[0])
    source_path = Path(source_paths[0])
    source_bytes = source_path.read_bytes()
    source = pydicom.dcmread(source_path)
    source.PixelSpacing = [1.000005, 1]
    source.save_as(source_path, enforce_file_format=True)
    try:
        read_series(source_paths, IDS[0])
        raise AssertionError("Relative tolerance widened the source pixel spacing contract")
    except RuntimeError:
        negative += 1
    finally:
        source_path.write_bytes(source_bytes)
    obj = pydicom.dcmread(path)
    # No shared macros is valid when every frame supplies its exact geometry.
    shared = obj.SharedFunctionalGroupsSequence[0]
    for frame in obj.PerFrameFunctionalGroupsSequence:
        frame.PixelMeasuresSequence = copy.deepcopy(shared.PixelMeasuresSequence)
        frame.PlaneOrientationSequence = copy.deepcopy(shared.PlaneOrientationSequence)
    obj.SharedFunctionalGroupsSequence = Sequence([])
    assert int(seg_mask(obj, series, labels=["target"]).sum()) == 16
    # The geometry of an unselected organ must still pass full association.
    obj.PerFrameFunctionalGroupsSequence[0].PlanePositionSequence[0].ImagePositionPatient = [1, 0, 2]
    try:
        seg_mask(obj, series, labels=["target"])
        raise AssertionError("Invalid unselected SEG frame was accepted")
    except RuntimeError:
        negative += 1
    obj = pydicom.dcmread(path)
    pixels = obj.pixel_array
    # A one-frame, one-segment SEG may put identification in the shared group.
    obj.SegmentSequence = Sequence([obj.SegmentSequence[0]])
    obj.PerFrameFunctionalGroupsSequence = Sequence([obj.PerFrameFunctionalGroupsSequence[1]])
    obj.SharedFunctionalGroupsSequence[0].SegmentIdentificationSequence = (
        obj.PerFrameFunctionalGroupsSequence[0].SegmentIdentificationSequence)
    del obj.PerFrameFunctionalGroupsSequence[0].SegmentIdentificationSequence
    obj.NumberOfFrames = 1
    obj.PixelData = pack_bits(pixels[1])
    assert int(seg_mask(obj, series, segment_numbers=[1]).sum()) == 16
    # Empty but explicitly present frames remain well-defined binary masks.
    obj.PixelData = pack_bits(np.zeros_like(pixels[1]))
    assert int(seg_mask(obj, series, labels=["target"]).sum()) == 0
    return {"samples": len(IDS), "masks": len(IDS), "positive_selections": outputs,
            "negative_cases": negative, "single_frame": True, "empty_mask": True}


def case_mapping(root, context, context_path, env):
    os.environ.update(env)
    assert [sid for _, sid in mapped_sample_files("rt_seg", extensions=(".dcm",))] == IDS
    path = Path(context["collection_map"]["records_by_asset"]["rt_seg"][0]["uri"])
    original_bytes, original_context = path.read_bytes(), copy.deepcopy(context)
    negative = 0
    for asset in ("rt_seg", "dicom"):
        source = (path if asset == "rt_seg" else
                  root / "dicom" / "source_0" / "slice_2.dcm")
        original = source.read_bytes()
        source.write_bytes(original[:-1] + bytes([original[-1] ^ 1]))
        expect_failure(run_script("dsimaging_rt_convert.py", root, env, root / f"changed_{asset}"))
        source.write_bytes(original)
        negative += 1
    path.unlink()
    expect_failure(run_script("dsimaging_rt_convert.py", root, env, root / "missing_seg"))
    path.write_bytes(original_bytes)
    negative += 1
    for records in (original_context["collection_map"]["records_by_asset"]["rt_seg"][:-1],
                    original_context["collection_map"]["records_by_asset"]["rt_seg"] +
                    [original_context["collection_map"]["records_by_asset"]["rt_seg"][0]]):
        context = copy.deepcopy(original_context)
        context["collection_map"]["records_by_asset"]["rt_seg"] = records
        save_context(context, context_path)
        expect_failure(run_script("dsimaging_rt_convert.py", root, env, root / f"coverage_{negative}"))
        negative += 1
    context = copy.deepcopy(original_context)
    entries = context["collection_map"]["records_by_asset"]["rt_seg"]
    entries[0]["sample_id"], entries[1]["sample_id"] = entries[1]["sample_id"], entries[0]["sample_id"]
    save_context(context, context_path)
    expect_failure(run_script("dsimaging_rt_convert.py", root, env, root / "swapped_samples"))
    negative += 1
    for mode in ("duplicate_source", "multiple_files"):
        context = copy.deepcopy(original_context)
        entries = context["collection_map"]["records_by_asset"]["rt_seg"]
        if mode == "duplicate_source":
            entries[1] = dict(entries[0], sample_id=IDS[1])
        else:
            entries[0].update(source_kind="dicom_series", n_files=2)
        save_context(context, context_path)
        expect_failure(run_script("dsimaging_rt_convert.py", root, env, root / mode))
        negative += 1
    save_context(original_context, context_path)
    return {"samples": len(IDS), "verified_files": 12, "negative_cases": negative}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", choices=("fixture", "seg", "mapping"), required=True)
    parser.add_argument("--root", required=True)
    args = parser.parse_args()
    fixture_root = Path(args.root).resolve()
    context, context_path, env = build_seg_fixture(fixture_root)
    result = ({"samples": len(IDS)} if args.case == "fixture" else
              globals()["case_" + args.case](fixture_root, context, context_path, env))
    print(json.dumps(result))
