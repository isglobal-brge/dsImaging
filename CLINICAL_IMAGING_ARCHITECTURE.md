# dsImaging Clinical Imaging Architecture

`dsImaging` is the canonical DataSHIELD package for clinical imaging data and
derived imaging artifacts. It owns imaging dataset descriptors, storage access,
asset lineage, segmentation masks, radiomics profiles, model registries, and the
domain runners needed to transform images into analysis-ready assets.

Heavy computation is not implemented as direct R calls. `dsImaging` declares
allowlisted runners and publishers; `dsJobs` (future product name `dsHPC`) owns
scheduling, quotas, containers, GPU/resource policy, external/HPC backends, and
durable execution.

## Package Boundaries

| Package | Responsibility |
| --- | --- |
| `dsJobs` / `dsHPC` | Durable job and pipeline runtime, shared scheduler state, adaptive resource limits, backend execution, container/GPU policy, result references. |
| `dsImaging` | Server-side imaging domain package: manifests, backends, catalog, masks, segmentation, radiomics, derived imaging assets, runner registration, publishers. |
| `dsImagingClient` | User-facing client DSL: dataset/catalog queries, segmenter/profile constructors, workflow submission, collection status/publish helpers. |
| `dsimaging-store` | Optional data-store/controller layer that prepares and publishes image collections as manifest-backed datasets consumable by `dsImaging`. |
| `dsFlower` | Consumer of analysis-ready feature tables, masks, and image-derived datasets; it should not run segmentation/radiomics itself. |

`dsRadiomics` should no longer be a required production package. Radiomics is a
module inside `dsImaging`, because the operational unit is the full image
analysis lifecycle: images -> masks -> features -> derived assets.

## Internal Module Layout

The package is organized around stable responsibilities:

| Module | Files | Purpose |
| --- | --- | --- |
| Storage/backends | `backend*.R`, `registry.R`, `manifest.R`, `resource_client.R`, `interface.R` | Resolve file/S3 datasets, parse manifests, expose DataSHIELD-safe dataset metadata and backend handles. |
| Asset catalog | `asset_db.R`, `assets_interface.R` | Store immutable derived assets, aliases, lineage, generations, per-sample status, derivation hashes, and content hashes. |
| Analysis options | `analysis_options.R` | Read `dsimaging.*` and `dsimaging.analysis.*` options with legacy fallback. |
| Runner registration | `analysis_runners.R`, `inst/python/*`, `inst/runners/*` | Register segmentation/radiomics runners in `DSJOBS_HOME/runners` on package load. |
| Profiles/models | `analysis_profiles.R`, `analysis_model_registry.R`, `analysis_install_models.R`, `inst/profiles/*` | Manage bundled radiomics profiles and admin-installed segmentation model metadata. |
| Orchestration | `analysis_orchestrator.R` | DataSHIELD aggregate methods for per-image collection scans, submission, status sync, publication, and mask validation. |
| Publishers | `analysis_publish_hooks.R` | dsJobs publisher hooks that register feature tables, mask roots, and per-image results as first-class imaging assets. |
| Capabilities/admin | `analysis_capabilities.R` | Health, runner, model, profile, scheduler, and admin-visible capability reporting. |
| Compatibility | `analysis_aliases.R` | Legacy `radiomics*DS` server method wrappers while clients migrate to `imaging*DS`. |

## Data Model

The durable model is content-addressed and restart-safe:

1. A dataset is described by a manifest and resolved through a backend.
2. Samples are identified by stable sample ids and content hashes.
3. Source assets are image roots, mask roots, labels, RT objects, or feature
   tables declared in the manifest.
4. Derived assets are immutable outputs such as mask roots, radiomics tables,
   embeddings, QC summaries, transformed images, and per-image artifacts.
5. A generation tracks a collection-level derivation request.
6. Generation items track per-sample status (`pending`, `claimed`, `running`,
   `completed`, `failed`) and output references.
7. Derivation hashes prevent recomputation of identical image + parameters +
   model/profile combinations.

Radiomic features are never treated as original input data for the demo path.
They are produced from image and mask assets, registered as job results, and then
published as derived feature-table assets.

## Runtime Contract

On package load, `dsImaging` should be self-registering:

1. Register the imaging resource resolver.
2. Prepare the asset DB path when possible.
3. Write dsJobs runner YAML files for bundled image-analysis runners.
4. Register dsJobs publishers for generic imaging assets and radiomics outputs.
5. Record load-time issues in `imagingCapabilitiesDS()` instead of failing
   silently.

At install time, `configure` prepares the shared state directory and
best-effort Python environments for analysis runners. It skips heavy
provisioning during `R CMD check`, supports
`DSIMAGING_SKIP_ANALYSIS_PROVISION=1`, and supports
`DSIMAGING_SKIP_HEAVY_PROVISION=1` when an install should rely on containers or
an external HPC image for torch-heavy segmenters.

This makes installation plug-and-play in a DataSHIELD/Rock setup: installing and
loading `dsImaging` is enough to expose domain runners to the shared job runtime.

## Compute Model

`dsImaging` never decides how the machine runs work directly. It declares:

| Field | Meaning |
| --- | --- |
| `resource_class` | CPU/GPU-oriented scheduling class. |
| `resources.memory_mb` | Expected memory envelope for adaptive scheduling. |
| `resources.cpu_slots` | CPU slot request. |
| `resources.optional_gpus` | GPU use when available, without requiring GPU in every Rock. |
| `resources.max_concurrent` | Runner-level local concurrency cap. |
| `resources.concurrency_group` | Shared pressure group, e.g. heavy torch segmenters. |
| `container` | Optional image/runtime/pull policy interpreted by dsJobs. |

The shared scheduler belongs to `dsJobs`/`dsHPC`. Multiple Rocks or R sessions on
the same scheduler home should contribute to one shared job state, not isolated
queues. External HPC units are reached through dsJobs backends and path mappings,
while `dsImaging` keeps ownership of the domain inputs/outputs and asset
publication.

## Current Analysis Capabilities

Implemented server-side capabilities:

| Capability | Status |
| --- | --- |
| Existing mask asset use | Implemented. |
| Lightweight CT lung threshold segmenter | Implemented for demos/QC. |
| LungMask runner | Registered; requires Python deps/model cache. |
| TotalSegmentator runner | Registered; supports optional GPU/container execution. |
| nnU-Net v2 runner | Registered; requires admin-registered model pack. |
| MONAI bundle runner | Registered; requires admin-installed bundle. |
| PyRadiomics extraction | Registered with bundled IBSI/demo/Aerts profiles. |
| Per-image collection orchestration | Implemented with scan, first-batch submit, server drip-feed, status, publish. |
| Derived feature table publication | Implemented as `feature_table` assets. |
| Mask validation for downstream packages | Implemented for generation-backed masks. |

## Required Clinical Imaging Feature Set

To become the complete imaging package, `dsImaging` should include these feature
families under one package namespace:

| Family | Required Functions |
| --- | --- |
| Dataset ingestion | File/S3 manifests, `dsimaging-store` manifest consumption, content hash indexes, sample manifests, labels, multi-root datasets. |
| Format support | NIfTI, NRRD, MHA/MHD, DICOM series, DICOM SEG, RTSTRUCT/RTDOSE/RTPLAN references, PNG/JPEG masks for 2D workflows. |
| Preprocessing | DICOM-to-NIfTI conversion hooks, orientation canonicalization, resampling, cropping, intensity normalization, bias correction, registration, anonymized QC metadata. |
| Segmentation | Existing masks, CT threshold, LungMask, TotalSegmentator, nnU-Net, MONAI bundles, external/custom runner registration, multi-label masks, ROI selection, mask post-processing. |
| Mask/ROI operations | Label selection, binary extraction, union/intersection, connected components, morphology, resampling-to-image, contour/RTSTRUCT conversion, coverage checks. |
| Radiomics | IBSI PyRadiomics profiles, lightweight demo profiles, Aerts signature profile, force-2D, voxel maps, selected features, profile registry, reproducibility metadata. |
| Derived analytics | Feature tables, embeddings, image-level QC metrics, mask volumes/shape summaries, thumbnails/overlays as non-disclosive QC artifacts where allowed. |
| Provenance | Content hashes, model/profile signatures, runner/container versions, lineage graph, deduplication, immutable assets and aliases. |
| DataSHIELD safety | Disclosure-safe counts, no raw sample ids unless explicitly server-side, no client-side image paths, permission-aware assets. |
| HPC integration | Runner resources, adaptive backpressure, shared scheduler, container images, optional GPU use, external backend path mapping. |
| Client workflow | Segment, extract, segment-and-extract, process collection, fire-and-forget status/publish, model install/list, capabilities, asset discovery. |

## Public API Direction

Canonical server methods:

- `imagingCapabilitiesDS()`
- `imagingInstallModelDS()`
- `imagingListModelsDS()`
- `imagingRadiomicsScanCollectionDS()`
- `imagingRadiomicsSubmitBatchDS()`
- `imagingRadiomicsCollectionStatusDS()`
- `imagingRadiomicsPublishCollectionDS()`
- `imagingSegmentationValidateMasksDS()`

Canonical client functions:

- `ds.imaging.capabilities()`
- `ds.imaging.install_model()`
- `ds.imaging.models()`
- `ds.imaging.segmenter.*()`
- `ds.imaging.segment()`
- `ds.imaging.radiomics.profile.*()`
- `ds.imaging.radiomics.extract()`
- `ds.imaging.radiomics.segment_and_extract()`
- `ds.imaging.radiomics.process_collection()`
- `ds.imaging.radiomics.collection_status()`
- `ds.imaging.radiomics.collection_publish()`
- `ds.imaging.radiomics.features()`
- `ds.imaging.masks()`

Legacy `radiomics*DS`, `ds.radiomics.*`, and `ds.segmenter.*` wrappers may
remain during development, but docs and demos should use `ds.imaging.*`.

## Configuration Surface

All regular configuration must be expressible as R/DataSHIELD options:

- Core: `dsimaging.registry_path`, `dsimaging.registry_backend`,
  `dsimaging.registry_uri`, `dsimaging.asset_db`, `dsimaging.data_dir`.
- Analysis: `dsimaging.analysis.home`, `dsimaging.analysis.venv_root`,
  `dsimaging.analysis.models_dir`, `dsimaging.analysis.model_registry`.
- Backpressure: `dsimaging.analysis.max_inflight`,
  `dsimaging.analysis.batch_size`.
- Containers: `dsimaging.analysis.container_images`,
  `dsimaging.analysis.container_image_<runner>`,
  `dsimaging.analysis.container_image`,
  `dsimaging.analysis.container_runtime`,
  `dsimaging.analysis.container_pull`.
- Compute runtime: `dsjobs.*` options for shared scheduler state, executor
  backend, containers, GPU discovery, quotas, and path mappings.

Defaults should be adaptive and conservative. Admins should tune options, not
edit files inside containers.

## Validation Strategy

Minimum validation before a release:

1. Unit tests for manifest parsing, asset catalog, model/profile registries,
   runner registration, and option fallback.
2. Load-time smoke test verifying runner YAMLs are written to a temporary
   `dsjobs.home`.
3. Python compile checks for all bundled runner scripts and compatibility
   wrappers.
4. Package checks for `dsImaging` and `dsImagingClient`.
5. Local single-site demo using CT threshold segmentation plus PyRadiomics demo
   profile to produce a real derived `feature_table`.
6. Multi-site simulation by running the same workflow sequentially over three
   Opal/Rock sites, validating independent derived assets and combined client
   reporting.
7. External/HPC simulation with dsJobs container or external backend path
   mappings, proving that the source Rock can submit work to a scheduler unit
   that has different CPU/GPU capabilities.

## Near-Term Backlog

The current integration provides the structural foundation. The next production
hardening items are:

- Replace remaining radiomics-oriented internal names where doing so does not
  reduce compatibility.
- Add DICOM series and RTSTRUCT/SEG ingestion helpers.
- Add mask operation runners and QA metrics.
- Add explicit runner version capture in published provenance.
- Add end-to-end tests that generate a tiny synthetic NIfTI, run CT-threshold
  segmentation, run PyRadiomics extraction, and publish a feature-table asset.
- Add an external backend demo that verifies path mapping and GPU capability
  discovery through dsJobs.
