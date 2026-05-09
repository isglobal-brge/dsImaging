# dsImaging

`dsImaging` is the server-side DataSHIELD package for clinical imaging. It
manages imaging dataset manifests, storage backends, content hashes, derived
asset catalogs, segmentation masks, radiomics feature tables, model/profile
registries, and dsJobs-backed image analysis workflows.

Heavy work is delegated to `dsJobs` (`dsHPC` in product language). Installing
and loading `dsImaging` registers imaging runners and publishers with the shared
job runtime so image workflows can run locally, in containers, or through an
external/HPC backend configured in `dsJobs`.

## What It Provides

- Manifest-backed imaging datasets on file or S3/MinIO-like storage.
- Asset catalog for masks, radiomics tables, embeddings, QC outputs, and other
  derived imaging artifacts.
- Store-backed existing mask assets with their own content hash index, so
  manual/consensus segmentations can drive radiomics without recomputation and
  still participate in derivation hashing.
- Feature assets can be loaded with dataset metadata joined on `sample_id`,
  enabling radiomics plus clinical/outcome analysis without ad hoc client-side
  merges.
- Immutable derivation hashes, aliases, lineage, and per-image generation state.
- DICOM series conversion to NIfTI with `dcm2niix` or SimpleITK fallback.
- Image preprocessing runners for resampling, normalization, clamping/windowing,
  and float32 casting.
- Segmentation runners: existing masks, CT lung threshold, LungMask,
  TotalSegmentator, nnU-Net v2, and MONAI bundles.
- Mask/ROI operations: label selection, binarization, union, intersection,
  difference, morphology, connected components, and mask-to-image resampling.
- QC metrics for images and masks, including size, spacing, intensity summaries,
  and mask volumes.
- PyRadiomics extraction with bundled IBSI, demo, force-2D, voxel-map, and Aerts
  signature profiles.
- Runner summaries with Python/library versions captured in derived asset
  provenance.
- Per-image collection orchestration with server-side drip-feed and safe
  reconnect/status/publish flow.
- dsJobs publisher hooks that register job outputs as `dsImaging` assets.

## Runtime Setup

The package is designed to be self-registering:

```r
library(dsImaging)
imagingCapabilitiesDS()
```

On load it registers:

- the `ImagingDatasetResourceResolver`;
- runner YAMLs under `DSJOBS_HOME/runners`;
- publishers for generic imaging assets and radiomics outputs;
- load-time issues in `imagingCapabilitiesDS()$onload_errors`.

During package installation, `configure` also performs best-effort Python
provisioning for the analysis runners under `/var/lib/dsimaging/venvs`. Set
`DSIMAGING_SKIP_ANALYSIS_PROVISION=1` to skip all analysis venvs, or
`DSIMAGING_SKIP_HEAVY_PROVISION=1` to provision only the radiomics/CT-threshold
environment and leave torch-heavy segmenters to containers or external HPC
images.

## Important Options

```r
options(
  dsimaging.data_dir = "/var/lib/dsimaging",
  dsimaging.asset_db = "/var/lib/dsimaging/imaging_assets.sqlite",
  dsimaging.analysis.max_inflight = 2L,
  dsimaging.analysis.batch_size = 1L,
  dsimaging.analysis.container_runtime = "auto",
  dsimaging.analysis.container_pull = "missing",
  dsimaging.analysis.container_images = list(
    pyradiomics_extract = "ghcr.io/isglobal-brge/dsimaging-runner:latest"
  )
)
```

`dsJobs` controls the shared scheduler, adaptive resource limits, GPU detection,
container execution, and external/HPC backend. `dsImaging` only declares the
domain runners and publishes domain outputs.

## Server Methods

Primary analysis methods:

- `imagingCapabilitiesDS()`
- `imagingInstallModelDS()`
- `imagingListModelsDS()`
- `imagingRadiomicsScanCollectionDS()`
- `imagingRadiomicsSubmitBatchDS()`
- `imagingRadiomicsCollectionStatusDS()`
- `imagingRadiomicsPublishCollectionDS()`
- `imagingLoadAssetDS()`
- `imagingSegmentationValidateMasksDS()`

Legacy `radiomics*DS` aliases remain available for development compatibility.

## Architecture

See [`CLINICAL_IMAGING_ARCHITECTURE.md`](CLINICAL_IMAGING_ARCHITECTURE.md) for
the full package structure, boundaries with `dsJobs`/`dsHPC`, feature plan, and
validation strategy.
