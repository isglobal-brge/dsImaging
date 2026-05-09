#' dsImaging: DataSHIELD Clinical Imaging Platform
#'
#' dsImaging is the server-side clinical imaging package for DataSHIELD. It
#' manages imaging dataset descriptors, storage backends, manifests, content
#' hashes, derived asset catalogs, segmentation masks, radiomics feature tables,
#' and clinical imaging analysis workflows.
#'
#' Heavy computation is declared by dsImaging as allowlisted runners and is
#' executed by dsJobs, the durable HPC-capable runtime. This keeps clinical image
#' handling, mask management, radiomics, and derived imaging analytics in one
#' package while leaving queueing, containers, Slurm/external backends, and GPU
#' policy to the compute layer.
#'
#' @import R6
#' @import resourcer
#' @keywords internal
"_PACKAGE"

#' dsImaging Server Options
#'
#' @description
#' dsImaging uses R options with DataSHIELD-friendly fallbacks. Core options use
#' `dsimaging.<name>` or `default.dsimaging.<name>`. Analysis-specific options
#' additionally support `dsimaging.analysis.<name>` and
#' `default.dsimaging.analysis.<name>`.
#'
#' @section Core storage and catalog:
#' - `dsimaging.registry_path`: local YAML dataset registry path.
#' - `dsimaging.registry_backend`: `"file"` or `"s3"`.
#' - `dsimaging.registry_uri`: registry URI when not using local path.
#' - `dsimaging.asset_db`: SQLite asset catalog path.
#' - `dsimaging.data_dir`: default state directory. Defaults to
#'   `/var/lib/dsimaging`.
#' - `dsimaging.venv_root`: Python venv root for lightweight imaging utilities.
#'
#' @section Analysis state:
#' - `dsimaging.analysis.home`: root for model/profile analysis state.
#'   Defaults to `dsimaging.data_dir` or `/var/lib/dsimaging`.
#' - `dsimaging.analysis.venv_root`: Python env root for PyRadiomics,
#'   LungMask, TotalSegmentator, nnU-Net, and MONAI runners. Defaults to
#'   `/var/lib/dsimaging/venvs`.
#' - `dsimaging.analysis.models_dir`: segmentation model directory.
#' - `dsimaging.analysis.model_registry`: model registry path.
#' - `dsimaging.max_inflight`: per-generation active image-analysis job cap.
#' - `dsimaging.batch_size`: server-side drip-feed batch size.
#'
#' @section Containerized clinical imaging runners:
#' Runners may use local Python environments or container images. Container
#' options are written into dsJobs runner YAML and interpreted by dsJobs:
#' - `dsimaging.analysis.container_images`: named list mapping runner names to
#'   image references.
#' - `dsimaging.analysis.container_image_<runner>`: per-runner image.
#' - `dsimaging.analysis.container_image`: fallback image.
#' - `dsimaging.analysis.container_runtime`: `"auto"`, `"docker"`, `"podman"`,
#'   `"apptainer"`, `"singularity"`, or `"none"`.
#' - `dsimaging.analysis.container_pull`: `"missing"`, `"always"`, or
#'   `"never"`.
#'
#' @section dsJobs / dsHPC runtime:
#' dsImaging does not run heavy work directly. Configure dsJobs with
#' `dsjobs.executor_backend`, `dsjobs.backend_path_mappings`,
#' `dsjobs.backend_gpu_count`, and related options to choose embedded,
#' Slurm, external, or containerized execution.
#'
#' @name dsimaging-options
NULL
