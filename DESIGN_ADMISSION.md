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
| `rt_convert` | One mapped RTSTRUCT or binary DICOM SEG and its mapped reference series per sample; patient, study, frame, referenced series and SOP instances must agree. SEG additionally binds every voxel frame to its source grid and segment. Labels or numbers select one segment or a union, publishing exactly one mask per sample. See the additional admission contract below. | Opaque mask asset usable by segmentation/radiomics workflows. | Contours, ROI labels, reference images, mask bodies and local manifests. |
| `rt_dose_plan` | Exactly one mapped RTDOSE and RTPLAN per sample, with matching canonical patient and DICOM study/frame and referenced plan. Optional mapped masks are joined by sample ID and require compatible spatial geometry. Output is a long per-ROI dose table keyed uniquely by `(sample_id, roi_label)` for a declared public vocabulary (legacy tables retain `roi`); every sample/declared-label pair occurs, including missing measurements for absent labels. | Opaque asset reference; an authorized ASSIGN can place the complete derived table in the server session, subject to the same patient admission and downstream disclosure controls as radiomics. | Per-ROI rows, numeric individual results, plan metadata, paths and all DICOM bodies; no aggregate method returns the raw table. |
| `wsi_tile` | Exactly one mapped self-contained slide per sample. Each slide has a private manifest, including zero-tile cases, and an exact integrity map covering every emitted tile. Sidecar-based slide formats without a complete mapping remain refused. | Opaque tile asset and coarse workflow state; public metadata retains its existing threshold/bucket policy. | Per-slide tile counts (private fan-out), coordinates, tissue fractions, tile manifests and tile bodies. No per-slide count or identifier is added to public metadata. |
| MONAI | Exact mapped image per sample. A locally installed, administrator-controlled bundle runs in a fresh per-sample output directory; exactly one mask is required, checked against the input geometry and renamed pseudonymously. Zero, multiple or misplaced masks fail. | Opaque mask asset and the existing segment-and-extract workflows. | Model paths, input images, output masks, worker files and diagnostics. |

Dose-table fan-out is a dedicated validation branch, not a relaxation of the
one-row-per-sample invariant for radiomics, QC or embedding tables. It verifies
the distinct sample roster, each sample's canonical patient mapping, unique ROI
keys and a fixed numeric schema. The initial ROI keys `whole_grid` and one optional `mask` (positive-voxel
union) remain compatible. The additional admission below adds a declared
public vocabulary for labelled masks and sets of masks. The minimum cohort threshold uses distinct
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

## Additional admission: SEG voxels and declared dose ROIs

Decision: 2026-09-23, `DSIMAGING_RAISE2_2026-09-23`, still version 0.5.0.
This section supersedes the initial SEG refusal and restriction to two dose
ROI groups above. The complete sealed patient roster, source integrity,
publication and DataSHIELD trust boundaries remain mandatory.

`rt_convert` accepts exactly one mapped DICOM SEG object per sample and that
sample's mapped, complete CT/MR reference series. Each file is size/SHA-256
verified. PatientID, StudyInstanceUID and FrameOfReferenceUID must match;
the SEG's single referenced series must enumerate exactly the mapped SOP
instances with matching SOP classes. Each voxel frame identifies exactly one
segment and one mapped source slice, with matching rows, columns, orientation,
position and spacing. Duplicate segment numbers/labels, duplicate segment/slice
frames, ambiguous references and unsupported grids are refused. The admitted
profile is BINARY Segmentation Storage. Fractional/LABELMAP segmentations,
resampled grids and multiframe reference images remain unsupported. Sparse
frames mean zero on omitted slices only when the full source series is
explicitly referenced and each declared segment has a frame.

The analyst selects SEG SegmentLabel values with `rois`, or positive
`segment_numbers`, never both. Selecting one segment produces its individual
binary mask asset; separate requests/output names produce per-segment assets.
Selecting several (or omitting selection) produces their binary union. Each
request still publishes exactly one hashed NIfTI mask per admitted sample;
there is no implicit primary chosen from several segment files. Labels,
segment inventory, mask bytes and individual presence remain private.

`rt_dose_plan` accepts either one mapped labelled `mask_asset` or a set of
mapped `mask_assets`. In labelled mode the analyst declares `roi_labels`
(public names, 1–128 distinct tokens) and corresponding positive integer
`mask_labels`. A single asset supplies all mask values, or `mask_assets`
provides one asset per public name/value pair. Repeating an asset for distinct
values is allowed; assigning the same asset/value pair to several names is
ambiguous and refused. Every distinct asset covers the exact admitted roster,
is hashed, and is joined by sample ID. Every mask must have the dose grid's
size, origin, spacing and direction; dose/plan patient, study, frame and plan
references retain their existing checks. Source mask identity comes from the
sealed sample map; NIfTI files cannot supply DICOM identity themselves.

The output has exactly one `(sample_id, roi_label)` row for every admitted
sample and every declared public label, in the declared order. Columns are
`dose_min`, `dose_max`, `dose_mean`, `dose_std` (population standard deviation),
`dose_voxels`, and the existing plan counts `n_beams`, `n_fraction_groups`,
`n_fractions`. Dose values are physical Gy. An absent label has four missing
measurements and zero voxels; its row is retained, and workflow success never
depends on that label being present. Undeclared private mask values are ignored,
not discovered or included in errors. Invalid geometry/bytes/label encoding
still fails with the runner's generic private error. No labelled request adds
an implicit `whole_grid` row. Without the public schema, legacy `roi` rows
(`whole_grid` and optional positive-mask union `mask`) remain compatible.

Publication and ASSIGN both validate the complete public cross-product,
unique keys, numeric/missing-value rules and distinct patient threshold.
The submitted schema is retained in private asset provenance and rechecked on
ASSIGN; the observed private table never defines its own permitted vocabulary.
Only the manifest-declared training label may join these repeated sample rows.
Dose fan-out remains refused by feature views requiring one row per sample.
Opaque asset references and coarse workflow state cross the boundary; raw dose
rows enter only the authorized server-side ASSIGN, with radiomics-equivalent
controls and downstream trusted-package disclosure responsibilities.

Verification adds pydicom-built binary SEG, several labelled dose regions,
missing-label rows, sets of mapped masks, positive admission/publication/ASSIGN,
and mutations of association, ambiguity, integrity and disclosure. Complete
suites and clean-checkout R package checks are recorded as a new receipt,
retaining the initial admission results.
