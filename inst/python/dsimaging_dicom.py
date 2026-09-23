"""Strict patient, instance and geometry checks for admitted DICOM sources."""

import numpy as np

from dsimaging_utils import sample_patient_id


def read_object(path, sample_id, modality=None):
    import pydicom
    obj = pydicom.dcmread(path)
    patient = str(getattr(obj, "PatientID", "")).strip(" \t\r\n")
    if patient != sample_patient_id(sample_id):
        raise RuntimeError("DICOM patient does not match the admitted sample")
    if modality and str(getattr(obj, "Modality", "")) != modality:
        raise RuntimeError("An admitted DICOM object has an incompatible modality")
    for field in ("StudyInstanceUID", "SeriesInstanceUID", "SOPInstanceUID", "SOPClassUID"):
        if not str(getattr(obj, field, "")):
            raise RuntimeError("An admitted DICOM object lacks instance identity")
    for meta_field, field in (("MediaStorageSOPClassUID", "SOPClassUID"),
                              ("MediaStorageSOPInstanceUID", "SOPInstanceUID")):
        if str(getattr(obj.file_meta, meta_field, "")) != str(getattr(obj, field)):
            raise RuntimeError("DICOM file and dataset instance identities disagree")
    expected_class = {"CT": "1.2.840.10008.5.1.4.1.1.2", "MR": "1.2.840.10008.5.1.4.1.1.4",
                      "RTSTRUCT": "1.2.840.10008.5.1.4.1.1.481.3",
                      "RTDOSE": "1.2.840.10008.5.1.4.1.1.481.2",
                      "RTPLAN": "1.2.840.10008.5.1.4.1.1.481.5",
                      "SEG": "1.2.840.10008.5.1.4.1.1.66.4"}.get(
                          modality or str(getattr(obj, "Modality", "")))
    if expected_class and str(obj.SOPClassUID) != expected_class:
        raise RuntimeError("DICOM SOP class does not match the admitted object modality")
    return obj


def read_series(paths, sample_id):
    """Require a complete, regular single-frame CT/MR series and order by position."""
    objects = [read_object(path, sample_id) for path in paths]
    if len(objects) < 2:
        raise RuntimeError("A reference DICOM series requires multiple slices")
    first = objects[0]
    for field in ("StudyInstanceUID", "SeriesInstanceUID", "FrameOfReferenceUID",
                  "Modality", "Rows", "Columns"):
        values = {str(getattr(obj, field, "")) for obj in objects}
        if len(values) != 1 or "" in values:
            raise RuntimeError("An admitted DICOM series is ambiguous")
    if first.Modality not in ("CT", "MR"):
        raise RuntimeError("An admitted DICOM series modality is unsupported")
    expected_class = {"CT": "1.2.840.10008.5.1.4.1.1.2", "MR": "1.2.840.10008.5.1.4.1.1.4"}[first.Modality]
    if any(str(obj.SOPClassUID) != expected_class for obj in objects):
        raise RuntimeError("An admitted DICOM series has unsupported SOP classes")
    sop_ids = [str(obj.SOPInstanceUID) for obj in objects]
    instances = [int(getattr(obj, "InstanceNumber", 0)) for obj in objects]
    if (len(set(sop_ids)) != len(objects) or len(set(instances)) != len(objects) or
            sorted(instances) != list(range(min(instances), max(instances) + 1))):
        raise RuntimeError("An admitted DICOM series has duplicate or missing slices")
    for obj in objects:
        if (int(getattr(obj, "NumberOfSeriesRelatedInstances", 0)) != len(objects) or
                int(getattr(obj, "NumberOfFrames", 1)) != 1):
            raise RuntimeError("An admitted DICOM series has an incomplete declared count")
    orientation = np.asarray(first.ImageOrientationPatient, dtype=float)
    spacing = np.asarray(first.PixelSpacing, dtype=float)
    if (orientation.shape != (6,) or spacing.shape != (2,) or
            not np.all(np.isfinite(orientation)) or not np.all(np.isfinite(spacing)) or not np.all(spacing > 0) or
            not np.allclose(np.linalg.norm(orientation.reshape(2, 3), axis=1), 1, atol=1e-5, rtol=0) or
            not np.isclose(np.dot(orientation[:3], orientation[3:]), 0, rtol=0)):
        raise RuntimeError("An admitted DICOM series has unsupported geometry")
    normal = np.cross(orientation[:3], orientation[3:])
    positions = []
    for obj in objects:
        position = np.asarray(obj.ImagePositionPatient, dtype=float)
        if (position.shape != (3,) or not np.all(np.isfinite(position)) or
                not np.allclose(obj.ImageOrientationPatient, orientation, atol=1e-6, rtol=0) or
                not np.allclose(obj.PixelSpacing, spacing, atol=1e-6, rtol=0)):
            raise RuntimeError("An admitted DICOM series has inconsistent geometry")
        positions.append(position)
    positions = np.asarray(positions)
    offsets = positions @ normal
    order = np.argsort(offsets)
    increments = np.diff(offsets[order])
    if (np.any(increments <= 0) or not np.allclose(increments, increments[0], atol=1e-4, rtol=0) or
            not np.allclose(positions[order] - positions[order][0],
                np.outer(offsets[order] - offsets[order][0], normal), atol=1e-4, rtol=0)):
        raise RuntimeError("An admitted DICOM series has duplicate or noncontiguous geometry")
    for obj in objects:
        declared_spacing = getattr(obj, "SpacingBetweenSlices", None)
        if declared_spacing is not None and not np.isclose(
                abs(float(declared_spacing)), increments[0], atol=1e-4, rtol=0):
            raise RuntimeError("An admitted DICOM series has missing slices")
    ordered = [objects[int(i)] for i in order]
    return [paths[int(i)] for i in order], ordered


def series_image(paths):
    import SimpleITK as sitk
    reader = sitk.ImageSeriesReader()
    reader.SetFileNames(paths)
    return reader.Execute()


def validate_rtstruct(obj, series):
    first = series[0]
    if str(obj.StudyInstanceUID) != str(first.StudyInstanceUID):
        raise RuntimeError("RTSTRUCT study does not match its admitted series")
    frames = list(getattr(obj, "ReferencedFrameOfReferenceSequence", []))
    if len(frames) != 1 or str(frames[0].FrameOfReferenceUID) != str(first.FrameOfReferenceUID):
        raise RuntimeError("RTSTRUCT frame association is ambiguous")
    studies = list(getattr(frames[0], "RTReferencedStudySequence", []))
    if len(studies) != 1 or str(studies[0].ReferencedSOPInstanceUID) != str(first.StudyInstanceUID):
        raise RuntimeError("RTSTRUCT study association is ambiguous")
    refs = list(getattr(studies[0], "RTReferencedSeriesSequence", []))
    if len(refs) != 1 or str(refs[0].SeriesInstanceUID) != str(first.SeriesInstanceUID):
        raise RuntimeError("RTSTRUCT series association is ambiguous")
    expected = {str(item.SOPInstanceUID) for item in series}
    sop_classes = {str(item.SOPInstanceUID): str(item.SOPClassUID) for item in series}
    referenced = [str(item.ReferencedSOPInstanceUID)
                  for item in getattr(refs[0], "ContourImageSequence", [])]
    if (set(referenced) != expected or len(referenced) != len(expected) or
            any(str(getattr(item, "ReferencedSOPClassUID", "")) !=
                sop_classes.get(str(item.ReferencedSOPInstanceUID))
                for item in refs[0].ContourImageSequence)):
        raise RuntimeError("RTSTRUCT does not reference the complete admitted series")
    roi_items = list(getattr(obj, "StructureSetROISequence", []))
    names = [str(item.ROIName) for item in roi_items]
    numbers = [int(item.ROINumber) for item in roi_items]
    if not names or len(set(names)) != len(names) or len(set(numbers)) != len(numbers):
        raise RuntimeError("RTSTRUCT ROI association is ambiguous")
    if any(str(getattr(item, "ReferencedFrameOfReferenceUID", "")) !=
           str(first.FrameOfReferenceUID) for item in roi_items):
        raise RuntimeError("RTSTRUCT ROI frame does not match the admitted series")
    contours = list(getattr(obj, "ROIContourSequence", []))
    if (len(contours) != len(numbers) or
            {int(item.ReferencedROINumber) for item in contours} != set(numbers)):
        raise RuntimeError("RTSTRUCT contours do not cover the declared ROIs")
    for roi in contours:
        parts = list(getattr(roi, "ContourSequence", []))
        if not parts:
            raise RuntimeError("RTSTRUCT ROI has no contours")
        for contour in parts:
            references = list(getattr(contour, "ContourImageSequence", []))
            points = np.asarray(contour.ContourData, dtype=float)
            if (str(contour.ContourGeometricType) != "CLOSED_PLANAR" or
                    len(references) != 1 or
                    str(references[0].ReferencedSOPInstanceUID) not in expected or
                    str(getattr(references[0], "ReferencedSOPClassUID", "")) !=
                    sop_classes.get(str(references[0].ReferencedSOPInstanceUID)) or
                    int(contour.NumberOfContourPoints) < 3 or
                    points.size != 3 * int(contour.NumberOfContourPoints) or
                    not np.all(np.isfinite(points))):
                raise RuntimeError("RTSTRUCT contour association or geometry is invalid")
            source = next(item for item in series if str(item.SOPInstanceUID) ==
                          str(references[0].ReferencedSOPInstanceUID))
            normal = np.cross(np.asarray(source.ImageOrientationPatient[:3], float),
                              np.asarray(source.ImageOrientationPatient[3:], float))
            relative_points = points.reshape(-1, 3) - np.asarray(source.ImagePositionPatient, float)
            x = relative_points @ np.asarray(source.ImageOrientationPatient[:3], float) / float(source.PixelSpacing[1])
            y = relative_points @ np.asarray(source.ImageOrientationPatient[3:], float) / float(source.PixelSpacing[0])
            if (np.any(x < -0.5) or np.any(x > int(source.Columns) - 0.5) or
                    np.any(y < -0.5) or np.any(y > int(source.Rows) - 0.5)):
                raise RuntimeError("RTSTRUCT contour leaves the admitted image grid")
            if not np.allclose((points.reshape(-1, 3) -
                                np.asarray(source.ImagePositionPatient, float)) @ normal,
                               0, atol=1e-4, rtol=0):
                raise RuntimeError("RTSTRUCT contour is outside its referenced slice")
    return names


def seg_mask(obj, series, labels=None, segment_numbers=None):
    """Decode source-grid binary SEG frames after exact source/segment association.

    Complete explicit series references permit omitted empty segment/slice frames;
    present frames must each identify one unique segment and source slice.
    """
    first = series[0]
    if (str(getattr(obj, "Modality", "")) != "SEG" or
            str(obj.SOPClassUID) != "1.2.840.10008.5.1.4.1.1.66.4" or
            str(obj.StudyInstanceUID) != str(first.StudyInstanceUID) or
            str(getattr(obj, "FrameOfReferenceUID", "")) != str(first.FrameOfReferenceUID)):
        raise RuntimeError("SEG does not share the admitted study and frame")
    if (str(getattr(obj, "SegmentationType", "")) != "BINARY" or
            int(obj.BitsAllocated) != 1 or int(obj.BitsStored) != 1 or
            int(obj.HighBit) != 0 or int(obj.PixelRepresentation) != 0 or
            int(obj.SamplesPerPixel) != 1 or str(obj.PhotometricInterpretation) != "MONOCHROME2" or
            int(obj.Rows) != int(first.Rows) or int(obj.Columns) != int(first.Columns)):
        raise RuntimeError("Only binary SEG on the admitted source grid is supported")
    if getattr(obj, "StudiesContainingOtherReferencedInstancesSequence", []):
        raise RuntimeError("SEG references another study")
    sources = {str(item.SOPInstanceUID): (index, item) for index, item in enumerate(series)}
    refs = list(getattr(obj, "ReferencedSeriesSequence", []))
    if len(refs) != 1 or str(refs[0].SeriesInstanceUID) != str(first.SeriesInstanceUID):
        raise RuntimeError("SEG series association is ambiguous")
    instances = list(getattr(refs[0], "ReferencedInstanceSequence", []))
    referenced = [str(item.ReferencedSOPInstanceUID) for item in instances]
    if len(referenced) != len(sources) or set(referenced) != set(sources):
        raise RuntimeError("SEG does not reference the complete admitted series")
    # Check every nested reference, including optional general source references.
    def check_references(dataset):
        if hasattr(dataset, "ReferencedSOPInstanceUID") != hasattr(dataset, "ReferencedSOPClassUID"):
            raise RuntimeError("SEG source instance reference is incomplete")
        if dataset is not obj:
            for field in ("PatientID", "StudyInstanceUID", "SeriesInstanceUID", "FrameOfReferenceUID"):
                if hasattr(dataset, field) and str(getattr(dataset, field)) != str(getattr(first, field)):
                    raise RuntimeError("SEG contains a conflicting source identity")
        if hasattr(dataset, "ReferencedSOPInstanceUID"):
            source = sources.get(str(dataset.ReferencedSOPInstanceUID))
            frame = getattr(dataset, "ReferencedFrameNumber", 1)
            if (source is None or str(getattr(dataset, "ReferencedSOPClassUID", "")) !=
                    str(source[1].SOPClassUID) or str(frame) != "1"):
                raise RuntimeError("SEG source instance association is ambiguous")
        for element in dataset:
            if element.VR == "SQ":
                for item in element.value:
                    check_references(item)
    check_references(obj)
    segments = list(getattr(obj, "SegmentSequence", []))
    numbers = [int(item.SegmentNumber) for item in segments]
    names = [str(item.SegmentLabel) for item in segments]
    if (not numbers or any(number < 1 or number > 65535 for number in numbers) or
            len(set(numbers)) != len(numbers) or len(set(names)) != len(names) or
            not all(name.strip() for name in names)):
        raise RuntimeError("SEG segment identity is ambiguous")
    labels, segment_numbers = labels or [], segment_numbers or []
    if (labels and segment_numbers or len(set(labels)) != len(labels) or
            len(set(segment_numbers)) != len(segment_numbers)):
        raise RuntimeError("SEG selection is ambiguous")
    selected = set(segment_numbers or [number for number, name in zip(numbers, names)
                                     if not labels or name in labels])
    if not selected.issubset(numbers) or not set(labels).issubset(names):
        raise RuntimeError("A selected SEG segment is unavailable")
    shared = list(getattr(obj, "SharedFunctionalGroupsSequence", []))
    frames = list(getattr(obj, "PerFrameFunctionalGroupsSequence", []))
    if len(shared) > 1 or not frames or len(frames) != int(obj.NumberOfFrames):
        raise RuntimeError("SEG functional groups are incomplete")
    shared_group = shared[0] if shared else None

    def functional_group(frame, name):
        shared_items, frame_items = getattr(shared_group, name, None), getattr(frame, name, None)
        if shared_items is not None and frame_items is not None:
            raise RuntimeError("SEG functional group is duplicated")
        items = list(shared_items if shared_items is not None else frame_items or [])
        if len(items) != 1:
            raise RuntimeError("SEG functional group is ambiguous")
        return items[0]

    def geometry_equal(value, expected, tolerance):
        values, expected = np.asarray(value, dtype=float), np.asarray(expected, dtype=float)
        return (values.shape == expected.shape and np.all(np.isfinite(values)) and
                np.allclose(values, expected, atol=tolerance, rtol=0))

    associations, seen = [], set()
    for frame in frames:
        number = int(functional_group(frame, "SegmentIdentificationSequence").ReferencedSegmentNumber)
        derivation = functional_group(frame, "DerivationImageSequence")
        source_refs = list(getattr(derivation, "SourceImageSequence", []))
        if len(source_refs) != 1 or number not in numbers:
            raise RuntimeError("SEG frame source or segment is ambiguous")
        source_ref = source_refs[0]
        index, source = sources[str(source_ref.ReferencedSOPInstanceUID)]
        if str(getattr(source_ref, "SpatialLocationsPreserved", "YES")) != "YES":
            raise RuntimeError("SEG frame does not preserve its source grid")
        key = (number, index)
        if key in seen:
            raise RuntimeError("SEG has duplicate frames for a segment and source slice")
        seen.add(key)
        position = functional_group(frame, "PlanePositionSequence")
        orientation = functional_group(frame, "PlaneOrientationSequence")
        measures = functional_group(frame, "PixelMeasuresSequence")
        if (not geometry_equal(position.ImagePositionPatient, source.ImagePositionPatient, 1e-4) or
                not geometry_equal(orientation.ImageOrientationPatient, source.ImageOrientationPatient, 1e-6) or
                not geometry_equal(measures.PixelSpacing, source.PixelSpacing, 1e-6) or
                (hasattr(measures, "SpacingBetweenSlices") and not geometry_equal(
                    measures.SpacingBetweenSlices,
                    np.linalg.norm(np.asarray(series[1].ImagePositionPatient, float) -
                                   np.asarray(first.ImagePositionPatient, float)), 1e-4)) or
                (hasattr(measures, "SliceThickness") and (
                    not np.isfinite(float(measures.SliceThickness)) or float(measures.SliceThickness) <= 0 or
                    (hasattr(source, "SliceThickness") and not geometry_equal(
                        measures.SliceThickness, source.SliceThickness, 1e-4))))):
            raise RuntimeError("SEG frame geometry differs from its admitted source")
        associations.append(key)
    if {number for number, _ in seen} != set(numbers):
        raise RuntimeError("SEG frames do not identify every declared segment")
    pixels = np.asarray(obj.pixel_array)
    if len(frames) == 1 and pixels.ndim == 2:
        pixels = pixels[np.newaxis, ...]
    if (pixels.shape != (len(frames), int(first.Rows), int(first.Columns)) or
            not np.all(np.isin(pixels, [0, 1]))):
        raise RuntimeError("SEG pixel data do not match the declared binary frames")
    union = np.zeros((len(series), int(first.Rows), int(first.Columns)), dtype="uint8")
    for pixels_frame, (number, index) in zip(pixels, associations):
        if number in selected:
            union[index] |= pixels_frame.astype("uint8")
    return union


def validate_dose_plan(dose, plan):
    if (str(dose.StudyInstanceUID) != str(plan.StudyInstanceUID) or
            not str(getattr(dose, "FrameOfReferenceUID", "")) or
            str(dose.FrameOfReferenceUID) != str(getattr(plan, "FrameOfReferenceUID", ""))):
        raise RuntimeError("RTDOSE and RTPLAN do not share the admitted frame and study")
    refs = list(getattr(dose, "ReferencedRTPlanSequence", []))
    if (len(refs) != 1 or str(refs[0].ReferencedSOPInstanceUID) != str(plan.SOPInstanceUID) or
            str(getattr(refs[0], "ReferencedSOPClassUID", "")) != str(plan.SOPClassUID)):
        raise RuntimeError("RTDOSE plan association is ambiguous")
    if str(getattr(dose, "DoseUnits", "")) != "GY" or str(getattr(dose, "DoseType", "")) != "PHYSICAL":
        raise RuntimeError("Only physical RTDOSE in Gy is supported")


def dose_image(dose):
    import SimpleITK as sitk
    pixels = np.asarray(dose.pixel_array, dtype=np.float64)
    scale = float(dose.DoseGridScaling)
    offsets = np.asarray(dose.GridFrameOffsetVector, dtype=float)
    orientation = np.asarray(dose.ImageOrientationPatient, dtype=float)
    spacing = np.asarray(dose.PixelSpacing, dtype=float)
    origin = np.asarray(dose.ImagePositionPatient, dtype=float)
    if (pixels.ndim != 3 or len(offsets) != pixels.shape[0] or len(offsets) < 2 or
            not np.isclose(offsets[0], 0, rtol=0) or np.any(np.diff(offsets) <= 0) or
            not np.allclose(np.diff(offsets), np.diff(offsets)[0], atol=1e-4, rtol=0) or
            orientation.shape != (6,) or spacing.shape != (2,) or origin.shape != (3,) or
            not np.isfinite(scale) or scale <= 0 or not np.all(np.isfinite(pixels)) or
            not np.all(np.isfinite(origin)) or not np.all(np.isfinite(orientation)) or
            not np.all(np.isfinite(offsets)) or not np.all(np.isfinite(spacing)) or not np.all(spacing > 0) or
            not np.allclose(np.linalg.norm(orientation.reshape(2, 3), axis=1), 1, atol=1e-5, rtol=0) or
            not np.isclose(np.dot(orientation[:3], orientation[3:]), 0, rtol=0)):
        raise RuntimeError("RTDOSE geometry is unsupported")
    scaled = pixels * scale
    if not np.all(np.isfinite(scaled)):
        raise RuntimeError("RTDOSE scaling produced nonfinite values")
    image = sitk.GetImageFromArray(scaled)
    normal = np.cross(orientation[:3], orientation[3:])
    image.SetDirection(np.column_stack((orientation[:3], orientation[3:], normal)).ravel().tolist())
    image.SetSpacing([float(spacing[1]), float(spacing[0]), float(offsets[1])])
    image.SetOrigin(origin.tolist())
    return image


def require_same_geometry(left, right):
    if (left.GetSize() != right.GetSize() or
            not np.allclose(left.GetSpacing(), right.GetSpacing(), atol=1e-5, rtol=0) or
            not np.allclose(left.GetOrigin(), right.GetOrigin(), atol=1e-4, rtol=0) or
            not np.allclose(left.GetDirection(), right.GetDirection(), atol=1e-5, rtol=0)):
        raise RuntimeError("Derived mask geometry does not match its admitted image")
