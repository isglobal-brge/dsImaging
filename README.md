# dsImaging

`dsImaging` is the server-side DataSHIELD package for clinical imaging. It
manages imaging dataset manifests, storage backends, content hashes, derived
asset catalogs, segmentation masks, radiomics feature tables, model/profile
registries, and dsHPC-backed image analysis workflows.

Heavy work is delegated to `dsHPC`. Installing
and loading `dsImaging` registers imaging runners and publishers with the shared
job runtime so image workflows can run locally, in containers, or through an
external/HPC backend configured in `dsHPC`.

## What It Provides

- Manifest-backed imaging datasets on file or S3/MinIO-like storage.
- Asset catalog for masks, radiomics tables, embeddings, QC outputs, and other
  derived imaging artifacts.
- Store-backed existing mask assets with their own content hash index, so
  manual/consensus segmentations can drive radiomics without recomputation and
  still participate in derivation hashing.
- Feature assets can be loaded with the manifest-declared `label_col` joined on
  the declared sample identifier. Other clinical/outcome metadata is never
  joined implicitly.
- Immutable derivation hashes, aliases, lineage, and per-image generation state.
- Image preprocessing runners for resampling, normalization, clamping/windowing,
  and float32 casting.
- Exact single-file and complete multi-slice DICOM-to-NIfTI conversion.
- RTSTRUCT-to-mask conversion and per-ROI RTDOSE/RTPLAN tables.
- Whole-slide tiling with private per-slide manifests and bounded tile output.
- Spatial runners for resampling, registration, cropping, and N4 bias
  correction.
- Segmentation runners: existing masks, CT lung threshold, LungMask,
  TotalSegmentator, nnU-Net v2, and conforming administrator-installed MONAI bundles.
- Mask/ROI operations: label selection, binarization, union, intersection,
  difference, morphology, connected components, and mask-to-image resampling.
- QC metrics for images and masks, including size, spacing, intensity summaries,
  and mask volumes.
- Server-side QC thumbnails/overlays with pseudonymous filenames, a size cap
  (`max_size = 192`) and a count cap (`max_tiles = 64`, maximum 1024). The local
  CSV lists rendered thumbnails and stays server-side; the output association
  manifest accounts for every admitted sample, including cap omissions.
- Image embedding tables using a deterministic local baseline, scheduled as
  GPU-optional so external HPC units can accelerate future model-backed
  embedding runners.
- PyRadiomics extraction with bundled IBSI, demo, force-2D, voxel-map, and Aerts
  signature profiles.
- Runner summaries with Python/library versions captured in derived asset
  provenance.
- Per-image collection orchestration with server-side drip-feed and safe
  reconnect/status/publish flow.
- One durable public tracking root per logical workflow, with all per-image
  execution children and exact fan-out cardinality hidden.
- Server-reusable validated outputs that trusted DataSHIELD packages can
  consume through opaque references without transferring raw data or paths to
  the client.
- dsHPC publisher hooks that register job outputs as `dsImaging` assets.

Clinical workflows use the same complete sealed sample-to-patient roster as
radiomics. Analysts receive opaque workflow/asset references and coarse status;
images, masks, tiles, per-slide tile counts, ROI rows, local manifests and paths
stay on the node. The complete dose table can be assigned to the authorized
DataSHIELD session for analysis under downstream packages' disclosure controls.

### Source association for DICOM, RT and slides

The top-level `sample_manifests` and `content_hash_index` describe `images`.
Each additional source asset (`dicom`, `rt_struct`, `rt_dose`, `rt_plan`, `wsi`)
declares both tables under its asset entry, with one row for every admitted
sample. For example:

```yaml
assets:
  rt_dose:
    kind: rt_dose_file
    uri: /srv/store/datasets/study/source/rt_dose
    sample_manifests:
      uri: /srv/store/datasets/study/metadata/rt_dose_samples.csv
      format: csv
    content_hash_index:
      uri: /srv/store/datasets/study/indexes/rt_dose.csv
      format: csv
```

An indexed RTDOSE/RTPLAN asset URI is a directory/prefix of per-sample files.
The sample table columns are `sample_id,source_kind,primary_uri,files_json,
content_hash,n_files`; the content index columns are `sample_id,source_kind,uri,
content_hash,size`. Single-file samples retain the existing mapping. A
`dicom_series` row has empty `primary_uri`, a sample-directory `uri`, and
`files_json` entries containing `path,role,content_hash,size` for **every** slice.
Group sizes and hashes follow [DESIGN_ADMISSION.md](DESIGN_ADMISSION.md).
All source association tables enter the collection seal. S3 files can also pin
`version_id`. No filename-to-patient inference is permitted.

Multi-slice DICOM requires canonical PatientID agreement, a single study/series/
frame, consistent slice geometry and `NumberOfSeriesRelatedInstances` on every
slice matching the exact file count. Missing counts, gaps, duplicate instances,
mixed series and unsupported geometry are refused. Convert DICOM to a mapped
NIfTI asset before generic image segmentation/radiomics. RTSTRUCT conversion
checks its reference series and SOP instances and unions selected ROIs into one
binary mask per sample. Dose analysis checks the dose-to-plan association and
optional masks in physical coordinates; `whole_grid` and optional `mask` rows
remain individual-level data.

WSI accepts self-contained SVS/NDPI/TIFF or raster slides. `max_tiles` is a
per-slide bound (default 2048); slides yielding zero accepted tiles still have
a private manifest. Sidecar slide formats and DICOM SEG remain refused.
MONAI bundles must be locally installed by the administrator, bind `image` and
`output_dir` in their inference configuration, and produce exactly one mask
matching each input geometry. Arbitrary Model Zoo bundles may need a site-owned
adapter to meet this contract; missing or multiple masks fail publication.

### Aerts profiles

`aerts_signature_v1.yaml` is unchanged historical evidence: an Aerts-inspired
four-feature profile selecting Energy, Compactness1 and original/wavelet-HLH
GLRLM RunLengthNonUniformity. `aerts_signature_v2` selects the published four
features: original Energy, Compactness2, original GLRLM GrayLevelNonUniformity
and wavelet-HLH GLRLM GrayLevelNonUniformity, using the same bundled PyRadiomics
settings. Compactness2 is sphericity cubed, as documented in the
[original supplementary material](https://www.ebi.ac.uk/europepmc/webservices/rest/PMC4059926/supplementaryFiles)
(Figure 1 and feature 16) and the
[author-coauthored replication](https://pmc.ncbi.nlm.nih.gov/articles/PMC6805885/).
The server applies the exact four-column selection for v2; the client selects
it with `ds.imaging.radiomics.profile.aerts_signature(version = "v2")`.
No historical demonstration or result has been rerun.

## Runtime Setup

The package is designed to be self-registering:

```r
library(dsImaging)
imagingCapabilitiesDS()
```

On load it registers:

- the `ImagingDatasetResourceResolver`;
- runner YAMLs under `DSHPC_HOME/runners`;
- publishers for generic imaging assets and radiomics outputs;
- load-time issues in `imagingCapabilitiesDS()$onload_errors`.

### Resource registration

Opal and DSLite can continue to use a direct `imaging+dataset://` Resource. In
Opal, `inst/resources/resource.js` supplies the administrator UI for that
descriptor. For compatibility, an unrelated `format` value on a direct
Resource remains ordinary metadata and is ignored. A value beginning with
`dsimaging-dataset` is instead treated as a locator claim and must match the
strict syntax and the URL dataset identifier.

Armadillo rewrites a Resource's backing URL and replaces its secret with a
short-lived internal JWT. Use an inert marker table for that transport and put
only the dataset identifier in `format`:

```r
marker <- data.frame(selector = TRUE)
MolgenisArmadillo::armadillo.upload_table(
  project = "imaging", folder = "markers", table = marker,
  name = "imaging_contract_marker")

images <- resourcer::newResource(
  name = "imaging.contract",
  url = paste0(
    sub("/$", "", armadillo_url),
    "/storage/projects/imaging/objects/",
    "markers%2Fimaging_contract_marker.parquet"
  ),
  format = "dsimaging-dataset:imaging.contract"
)
MolgenisArmadillo::armadillo.upload_resource(
  project = "imaging", folder = "resources",
  resource = images, name = "imaging_contract")

# After the normal DataSHIELD login:
dsImagingClient::ds.imaging.init(
  conns, resource = "imaging/resources/imaging_contract", symbol = "img")
```

Current Armadillo servers parse marker URLs narrowly. Use only letters,
digits, and underscores for the marker project, folder, and object name. The
dataset identifier in `format` may also contain dots and hyphens.

An Armadillo Resource never carries object-store credentials or an object-store
endpoint. Its dataset identifier resolves through the node registry, for
example:

```yaml
schema_version: 1
imaging.contract:
  enabled: true
  backend: s3
  manifest_uri: s3://imaging-data/datasets/imaging.contract/manifest.yaml
  endpoint: http://minio:9000
  credentials_ref: imaging_store_ro
```

`credentials_ref` must resolve from protected server deployment configuration,
such as `/var/lib/dsimaging/credentials.yaml` mounted with mode `0600`. Never
put an access key, secret key, endpoint, bucket, or manifest path in the
Resource `format` locator.

Publishing the Armadillo marker and Resource is a trusted node-administrator
operation. The dataset identifier is a selector, not an authorization token:
analysts should receive read/assign access only to curated Resources, never
permission to upload or replace their RDS descriptors.

During package installation, `configure` also performs best-effort Python
provisioning for the analysis runners under `/var/lib/dsimaging/venvs`. Set
`DSIMAGING_SKIP_ANALYSIS_PROVISION=1` to skip all analysis venvs, or
`DSIMAGING_SKIP_HEAVY_PROVISION=1` to provision only the radiomics/CT-threshold
environment and leave torch-heavy segmenters to containers or external HPC
images.

The radiomics analysis environment also carries the shared clinical imaging IO
dependencies used by the lightweight runners: `pydicom`, `rt-utils`,
`highdicom`, `Pillow`, `openslide-python`, and `openslide-bin`.

## Important Options

```r
options(
  # SHA-256 of the exact dsHPC/Python/container/model deployment.
  dshpc.runtime_revision = Sys.getenv("DSHPC_RUNTIME_REVISION"),
  dsimaging.data_dir = "/var/lib/dsimaging",
  dsimaging.asset_db = "/var/lib/dsimaging/imaging_assets.sqlite",
  dsimaging.analysis.max_inflight = 2L,
  dsimaging.analysis.batch_size = 1L,
  dsimaging.analysis.claim_timeout_secs = 3600L,
  dsimaging.analysis.container_runtime = "auto",
  dsimaging.analysis.container_pull = "missing",
  dsimaging.analysis.container_images = list(
    pyradiomics_extract = paste0(
      "ghcr.io/isglobal-brge/dsimaging-runner@sha256:",
      "<64-hex-digest>")
  )
)
```

Clinical runner images must use an immutable `@sha256:<digest>` reference;
mutable tags such as `:latest` are rejected.

Reusable imaging derivations additionally require dsHPC's lowercase 64-hex
`runtime_revision`. It must change whenever Python locks, containers, remote
execution images, or model weights change. Configure it on the local/site
default or separately in each selectable execution-unit catalogue entry.
dsImaging fails closed before claiming reusable work when this seal is absent.

`dsHPC` controls the shared scheduler, adaptive resource limits, GPU detection,
container execution, and external/HPC backend. `dsImaging` only declares the
domain runners and publishes domain outputs.

Generation recovery is automatic during status/publish checks. If a process dies
after claiming samples but before submitting their dsHPC jobs, claimed items older
than `dsimaging.analysis.claim_timeout_secs` are returned to `pending` and the
drip-feed loop can submit them again. Destructive generation cancellation is
admin-only and reuses `dshpc.admin_key` or `DSHPC_ADMIN_KEY`.

Bundled Python runners and radiomics profiles are copied to content-addressed
runtime directories under `dsimaging.analysis.home`, so queued jobs are not
broken by package upgrades or temporary `00LOCK` install paths.

Collection publication also enforces the generation profile's
`selected_features`, so stale or wider per-image artifacts cannot silently
expand the published schema at one site. Completed artifacts that no longer
match the selected feature contract are requeued automatically during
status/recovery/publish checks, and per-image deduplication only reuses stored
assets whose artifact path still exists and satisfies that same contract.
Running items without any active dsHPC job are also returned to `pending`, so a
worker crash or package reinstall cannot leave a generation permanently stuck.

Collection-level runners do not infer patient identity from filenames or scan
an asset directory. They consume the exact `sample_id` routes pinned in the
admitted collection snapshot, verify each source object's size and SHA-256, and
publish either one table row per admitted sample or an exact per-sample artifact
manifest. Missing, duplicate, extra, or cross-attributed outputs are rejected
before they enter the asset catalog.

## DataSHIELD Methods

The registered analyst surface starts with `imagingInitDS()` and its opaque
session handle. It exposes disclosure-controlled metadata/validation, a
sanitized global asset catalog, handle-bound workflow submission, and coarse
workflow status/publication. dsHPC exposes one durable, cardinality-free
tracking root per logical workflow; the public id can assign a server-side
opaque reference to a validated reusable output, but cannot read its data.
The exact `AggregateMethods` and `AssignMethods` allowlists are declared in
`DESCRIPTION` and should be resynchronized in Opal/Rock after every package
upgrade.

Legacy dataset enumeration, raw asset details/lineage, raw collection scans,
batch submission, exact generation status/recovery, and raw mask-path methods
are not registered. Their old exported names are fail-closed stubs so a stale
server allowlist cannot restore the previous behavior.

See [`DISCLOSURE_CONTROL.md`](DISCLOSURE_CONTROL.md) for the release invariants,
operator obligations, supported formats, and residual disclosure signals.

## Architecture

See [`CLINICAL_IMAGING_ARCHITECTURE.md`](CLINICAL_IMAGING_ARCHITECTURE.md) for
the full package structure, boundaries with `dsHPC`, feature plan, and
validation strategy.
