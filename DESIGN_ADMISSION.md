# Exact association for clinical imaging admission

Decision: 2026-09-23. Target: dsImaging 0.5.0, from 0.4.0.

The admission authority remains the complete sealed sample-to-canonical-patient
roster (`trim-utf8-v2`). An image, slice, ROI, or tile is never a new privacy
unit. Analyst requests select logical assets and bounded processing options,
never files, patient IDs, sample subsets, scripts, or output paths.

## Inputs and seal

The existing top-level sample manifest and content index describe `images`.
Additional source assets declare their own `sample_manifests` and
`content_hash_index` tables using the same schema. Each table must cover exactly
the admitted sample roster. They are included in the immutable collection seal;
source assets without exact mappings remain unusable by these workflows.
An asset root is confined to the authorized collection. Each sample has one
single file or one explicitly enumerated `dicom_series` group. The manifest's
sample ID, not a basename or DICOM search, establishes the association.

Every group file declares `path`, `role`, `content_hash` (SHA-256), `size`, and
optionally an immutable object `version_id`. Paths are relative to the asset
root, unique across samples and confined to the sample directory. The group
size is the sum of file sizes. Its SHA-256 is computed from UTF-8 records sorted
by path, each `path\tdecimal-size\tsha256\n`. The reader checks the exact file
set and every size/hash before conversion. Missing, extra, duplicate, escaping,
symlinked, mixed-patient, mixed-series, or incomplete inputs fail closed.
Single-file source bytes retain their existing SHA-256 contract.

DICOM readers compare canonical PatientID with the sealed patient identifier;
they do not replace or infer that identifier. A multi-slice series must have
one StudyInstanceUID, SeriesInstanceUID and FrameOfReferenceUID, unique SOP
instances, consistent dimensions/orientation/spacing and an unambiguous ordered
slice grid. Declared slice counts and contiguous slice positions must agree.
Completeness means the sealed published file set plus those DICOM consistency
checks; no software can discover an omitted terminal slice when the source
contains no authoritative expected count. Such series require a declared
expected count. Unsupported geometry and ambiguous references are refused.

## Route contracts and analyst boundary

| Route | Exact input and output association | Analyst obtains | Remains on node |
| --- | --- | --- | --- |
| `dicom_convert` | One sealed single-file DICOM or complete series per sample; one spatially valid NIfTI per sample, with size/hash and full-roster output manifest. | Opaque workflow/asset reference and coarse completion state; subsequent authorized server workflows can consume it. | All DICOM instances, headers, identifiers, NIfTI bodies, paths and exact progress. |
| `rt_convert` | One mapped RTSTRUCT and its mapped reference series per sample; patient, study, frame, referenced series and SOP instances must agree. Selected ROIs are combined into exactly one binary mask per sample; missing ROIs, invalid contours or ambiguous references fail. DICOM SEG remains refused unless equivalent frame association is implemented and tested. | Opaque mask asset usable by segmentation/radiomics workflows. | Contours, ROI labels, reference images, mask bodies and local manifests. |
| `rt_dose_plan` | Exactly one mapped RTDOSE and RTPLAN per sample, with matching canonical patient and DICOM study/frame and referenced plan. Optional mapped masks are joined by sample ID and require compatible spatial geometry. Output is a long per-ROI dose table keyed uniquely by `(sample_id, roi)`; every admitted sample must occur. | Opaque asset reference; an authorized ASSIGN can place the complete derived table in the server session, subject to the same patient admission and downstream disclosure controls as radiomics. | Per-ROI rows, numeric individual results, plan metadata, paths and all DICOM bodies; no aggregate method returns the raw table. |
| `wsi_tile` | Exactly one mapped self-contained slide per sample. Each slide has a private manifest, including zero-tile cases, and an exact integrity map covering every emitted tile. Sidecar-based slide formats without a complete mapping remain refused. | Opaque tile asset and coarse workflow state; public metadata retains its existing threshold/bucket policy. | Per-slide tile counts (private fan-out), coordinates, tissue fractions, tile manifests and tile bodies. No per-slide count or identifier is added to public metadata. |
| MONAI | Exact mapped image per sample. A locally installed, administrator-controlled bundle runs in a fresh per-sample output directory; exactly one mask is required, checked against the input geometry and renamed pseudonymously. Zero, multiple or misplaced masks fail. | Opaque mask asset and the existing segment-and-extract workflows. | Model paths, input images, output masks, worker files and diagnostics. |

Dose-table fan-out is a dedicated validation branch, not a relaxation of the
one-row-per-sample invariant for radiomics, QC or embedding tables. It verifies
the distinct sample roster, each sample's canonical patient mapping, unique ROI
keys and a fixed numeric schema. Supported ROI keys are `whole_grid` and one
optional `mask`, the union of positive voxels in the mapped mask; arbitrary
multi-label ROI export is not admitted. The minimum cohort threshold uses distinct
patients, never ROI rows. Only the declared training label may be joined.
Repeated-observation analysis remains the responsibility of the downstream
trusted DataSHIELD package. Raw dose-table assignment cannot make rows public.

All file publication validates full roster, confined unique artifacts and exact
size/hash. Additional route-specific checks bind per-slide manifests and dose
rows to their declared samples. Worker summaries, asset provenance and local
CSVs remain private. Existing public metadata counts remain thresholded and
bucketed; counts of child work, tiles or ROIs never become patient counts.

## QC thumbnail count

`max_tiles` defaults to 64, with an analyst range of 1 through 1024, alongside
`max_size` (default 192, range 16 through 4096). The runner validates all admitted
inputs, then renders at most `max_tiles` thumbnails in stable sample order.
The exact output manifest retains every sample and explicitly records samples
omitted by the cap. This does not authorize a partial cohort or a sample filter.
The existing local CSV schema remains unchanged and lists rendered thumbnails
only. Both it and pseudonymously named thumbnails remain server-side.

## Aerts profiles

`aerts_signature_v1.yaml` remains byte-identical. Its registry metadata will
identify it as an Aerts-inspired four-feature historical profile: Energy,
Compactness1, original GLRLM RunLengthNonUniformity and wavelet-HLH GLRLM
RunLengthNonUniformity. A separate `aerts_signature_v2` will select the published
Energy, Compactness and original/wavelet-HLH GLRLM GrayLevelNonUniformity,
using the same PyRadiomics settings. Compactness is implemented as
Compactness2 (sphericity cubed), identified in Figure 1 and feature 16 of the
original supplementary material:
<https://www.ebi.ac.uk/europepmc/webservices/rest/PMC4059926/supplementaryFiles>.
The author-coauthored replication also states this mapping:
<https://pmc.ncbi.nlm.nih.gov/articles/PMC6805885/>. No historical demo is
rerun or rewritten.

## Verification

Synthetic fixtures are generated inside the suite: pydicom CT slices,
RTSTRUCT/RTDOSE/RTPLAN, a Pillow slide and fake-provider MONAI inference.
Tests exercise DataSHIELD request admission, exact worker associations,
publication and assignment, and negative mutations of roster, patient/UID
references, file sets, hashes, output multiplicity and paths. Disclosure tests
check public responses for absence of raw rows, slide fan-out and filesystem
state. Profile and QC tests check selection and bounds. Both full test suites
and R CMD check run against clean committed checkouts; the validation receipt
records exact tested commits, commands, counts, runtime versions and any skips
or unclosed limitations. No tags or pushes are part of this change.
