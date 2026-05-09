# Module: Runner Registration
# dsImaging registers its runners into DSJOBS_HOME/runners/ at load time.
# dsJobs reads the YAML and executes -- it never knows about dsImaging.

#' Register dsImaging runners in DSJOBS_HOME
#'
#' Called from .onLoad. Writes runner YAMLs with absolute paths to the
#' dsImaging venvs and Python scripts.
#'
#' @keywords internal
.register_imaging_analysis_runners <- function() {
  # Default must match dsJobs' own default (.dsjobs_home in dsJobs/R/zzz.R)
  # so that runner YAMLs land where the validator looks for them.
  dsjobs_home <- getOption("dsjobs.home",
    getOption("default.dsjobs.home", "/srv/dsjobs"))
  runners_dir <- file.path(dsjobs_home, "runners")
  if (!dir.exists(runners_dir)) {
    dir.create(runners_dir, recursive = TRUE, showWarnings = FALSE)
  }
  if (!dir.exists(runners_dir))
    stop("Cannot create dsJobs runners directory: ", runners_dir,
         call. = FALSE)
  if (file.access(runners_dir, mode = 2) != 0)
    stop("dsJobs runners directory is not writable: ", runners_dir,
         call. = FALSE)
  tryCatch(Sys.chmod(runners_dir, "0777"), error = function(e) NULL)

  venv_root <- .imaging_analysis_option("venv_root", "/var/lib/dsimaging/venvs")
  scripts_dir <- system.file("python", package = "dsImaging")
  if (!nzchar(scripts_dir) || !dir.exists(scripts_dir))
    stop("Cannot locate dsImaging Python runner scripts.", call. = FALSE)

  # MKL workaround envs needed when running torch under amd64-on-arm64 Rosetta
  # emulation; harmless on native amd64 and aarch64 hosts.
  rosetta_mkl_env <- list(
    MKL_SERVICE_FORCE_INTEL = "0",
    MKL_THREADING_LAYER = "GNU"
  )

  # PyRadiomics extraction runner
  .write_runner_yaml(runners_dir, "pyradiomics_extract", .with_container(list(
    name = "pyradiomics_extract",
    plane = "artifact",
    resource_class = "cpu_heavy",
    resources = list(
      memory_mb = 6144L,
      cpu_slots = 2L,
      max_concurrent = 2L,
      concurrency_group = "pyradiomics_cpu"
    ),
    command = "python",
    python = file.path(venv_root, "radiomics", "bin", "python"),
    args_template = list(
      file.path(scripts_dir, "dsimaging_extract.py"),
      "--input", "{input_dir}",
      "--output", "{output_dir}",
      "--settings", "{settings_file}"
    ),
    timeout_secs = 3600L,
    env = rosetta_mkl_env,
    allowed_params = c("settings_file", "mask_asset", "image_asset",
                        "dataset_id", "label_channel", "force2D", "voxel_based",
                        "profile_name", "bin_width", "feature_classes",
                        "name", "normalize", "resampled_spacing", "image_types",
                        "selected_features", "image", "mask", "sample_id",
                        "generation_id")
  ), "pyradiomics_extract", list(
    "-m", "dsimaging_extract",
    "--input", "{input_dir}",
    "--output", "{output_dir}",
    "--settings", "{settings_file}"
  )))

  # TotalSegmentator runner
  .write_runner_yaml(runners_dir, "totalsegmentator_infer", .with_container(list(
    name = "totalsegmentator_infer",
    plane = "artifact",
    resource_class = "gpu_optional",
    resources = list(
      memory_mb = 8192L,
      cpu_slots = 4L,
      optional_gpus = 1L,
      max_concurrent = 1L,
      concurrency_group = "segmentation_heavy"
    ),
    command = "python",
    python = file.path(venv_root, "seg_totalseg", "bin", "python"),
    args_template = list(
      file.path(scripts_dir, "dsimaging_seg_totalseg.py"),
      "--input", "{input_dir}",
      "--output", "{output_dir}",
      "--task", "{task}"
    ),
    timeout_secs = 7200L,
    env = rosetta_mkl_env,
    allowed_params = c("task", "fast", "roi_subset", "statistics",
                        "image_asset", "provider",
                        "image", "sample_id", "generation_id")
  ), "totalsegmentator_infer", list(
    "-m", "dsimaging_seg_totalseg",
    "--input", "{input_dir}",
    "--output", "{output_dir}",
    "--task", "{task}"
  )))

  # LungMask runner
  .write_runner_yaml(runners_dir, "lungmask_infer", .with_container(list(
    name = "lungmask_infer",
    plane = "artifact",
    resource_class = "cpu_heavy",
    resources = list(
      memory_mb = 8192L,
      cpu_slots = 6L,
      optional_gpus = 1L,
      max_concurrent = 1L,
      concurrency_group = "torch_cpu_heavy",
      oom_cooldown_secs = 1800L
    ),
    command = "python",
    python = file.path(venv_root, "seg_lungmask", "bin", "python"),
    args_template = list(
      file.path(scripts_dir, "dsimaging_seg_lungmask.py"),
      "--input", "{input_dir}",
      "--output", "{output_dir}",
      "--model", "{model_name}"
    ),
    timeout_secs = 3600L,
    env = rosetta_mkl_env,
    allowed_params = c("model_name", "image_asset", "provider",
                        "image", "sample_id", "generation_id")
  ), "lungmask_infer", list(
    "-m", "dsimaging_seg_lungmask",
    "--input", "{input_dir}",
    "--output", "{output_dir}",
    "--model", "{model_name}"
  )))

  # Lightweight CT threshold segmentation runner. It reuses the PyRadiomics
  # environment because that already carries SimpleITK and NumPy.
  .write_runner_yaml(runners_dir, "ct_lung_threshold", .with_container(list(
    name = "ct_lung_threshold",
    plane = "artifact",
    resource_class = "cpu",
    resources = list(
      memory_mb = 1024L,
      cpu_slots = 1L,
      max_concurrent = 4L,
      concurrency_group = "ct_simple"
    ),
    command = "python",
    python = file.path(venv_root, "radiomics", "bin", "python"),
    args_template = list(
      file.path(scripts_dir, "dsimaging_seg_ct_threshold.py"),
      "--input", "{input_dir}",
      "--output", "{output_dir}",
      "--threshold", "{threshold}",
      "--max-components", "{max_components}",
      "--min-voxels", "{min_voxels}"
    ),
    timeout_secs = 900L,
    env = rosetta_mkl_env,
    allowed_params = c("threshold", "max_components", "min_voxels",
                        "image_asset", "provider",
                        "image", "sample_id", "generation_id")
  ), "ct_lung_threshold", list(
    "-m", "dsimaging_seg_ct_threshold",
    "--input", "{input_dir}",
    "--output", "{output_dir}",
    "--threshold", "{threshold}",
    "--max-components", "{max_components}",
    "--min-voxels", "{min_voxels}"
  )))

  # nnU-Net v2 runner
  .write_runner_yaml(runners_dir, "nnunetv2_predict", .with_container(list(
    name = "nnunetv2_predict",
    plane = "artifact",
    resource_class = "gpu_optional",
    resources = list(
      memory_mb = 8192L,
      cpu_slots = 4L,
      optional_gpus = 1L,
      max_concurrent = 1L,
      concurrency_group = "segmentation_heavy"
    ),
    command = "python",
    python = file.path(venv_root, "seg_nnunetv2", "bin", "python"),
    args_template = list(
      file.path(scripts_dir, "dsimaging_seg_nnunet.py"),
      "--input", "{input_dir}",
      "--output", "{output_dir}",
      "--model", "{model_name}"
    ),
    timeout_secs = 7200L,
    env = rosetta_mkl_env,
    allowed_params = c("model_name", "fold", "checkpoint", "step_size",
                        "image_asset", "provider",
                        "image", "sample_id", "generation_id")
  ), "nnunetv2_predict", list(
    "-m", "dsimaging_seg_nnunet",
    "--input", "{input_dir}",
    "--output", "{output_dir}",
    "--model", "{model_name}"
  )))

  # MONAI bundle runner
  .write_runner_yaml(runners_dir, "monai_bundle_infer", .with_container(list(
    name = "monai_bundle_infer",
    plane = "artifact",
    resource_class = "gpu_optional",
    resources = list(
      memory_mb = 8192L,
      cpu_slots = 4L,
      optional_gpus = 1L,
      max_concurrent = 1L,
      concurrency_group = "segmentation_heavy"
    ),
    command = "python",
    python = file.path(venv_root, "seg_monai", "bin", "python"),
    args_template = list(
      file.path(scripts_dir, "dsimaging_seg_monai.py"),
      "--input", "{input_dir}",
      "--output", "{output_dir}",
      "--bundle", "{bundle_name}"
    ),
    timeout_secs = 7200L,
    env = rosetta_mkl_env,
    allowed_params = c("bundle_name", "bundle_path", "device",
                        "image_asset", "provider",
                        "image", "sample_id", "generation_id")
  ), "monai_bundle_infer", list(
    "-m", "dsimaging_seg_monai",
    "--input", "{input_dir}",
    "--output", "{output_dir}",
    "--bundle", "{bundle_name}"
  )))

  # DICOM series conversion. Uses dcm2niix when available and SimpleITK as
  # a portable fallback.
  .write_runner_yaml(runners_dir, "dicom_convert", .with_container(list(
    name = "dicom_convert",
    plane = "artifact",
    resource_class = "cpu",
    resources = list(
      memory_mb = 2048L,
      cpu_slots = 1L,
      max_concurrent = 2L,
      concurrency_group = "imaging_io"
    ),
    command = "python",
    python = file.path(venv_root, "radiomics", "bin", "python"),
    args_template = list(
      file.path(scripts_dir, "dsimaging_dicom_convert.py"),
      "--input", "{input_dir}",
      "--output", "{output_dir}"
    ),
    timeout_secs = 3600L,
    env = rosetta_mkl_env,
    allowed_params = c("dataset_id", "dicom_asset", "dicom_root",
                        "image_asset", "converter")
  ), "dicom_convert", list(
    "-m", "dsimaging_dicom_convert",
    "--input", "{input_dir}",
    "--output", "{output_dir}"
  )))

  # General image preprocessing: resampling, normalization, intensity clamping,
  # and numeric pixel type conversion. Reuses the SimpleITK radiomics env.
  .write_runner_yaml(runners_dir, "image_preprocess", .with_container(list(
    name = "image_preprocess",
    plane = "artifact",
    resource_class = "cpu",
    resources = list(
      memory_mb = 4096L,
      cpu_slots = 2L,
      max_concurrent = 2L,
      concurrency_group = "imaging_cpu"
    ),
    command = "python",
    python = file.path(venv_root, "radiomics", "bin", "python"),
    args_template = list(
      file.path(scripts_dir, "dsimaging_preprocess.py"),
      "--input", "{input_dir}",
      "--output", "{output_dir}",
      "--operations", "{operations}"
    ),
    timeout_secs = 3600L,
    env = rosetta_mkl_env,
    allowed_params = c("dataset_id", "image_asset", "image_root",
                        "operations", "spacing", "resampled_spacing",
                        "lower", "upper")
  ), "image_preprocess", list(
    "-m", "dsimaging_preprocess",
    "--input", "{input_dir}",
    "--output", "{output_dir}",
    "--operations", "{operations}"
  )))

  # Mask/ROI operations: label selection, binarization, morphology,
  # connected-components, pairwise set ops, and image-space resampling.
  .write_runner_yaml(runners_dir, "mask_ops", .with_container(list(
    name = "mask_ops",
    plane = "artifact",
    resource_class = "cpu",
    resources = list(
      memory_mb = 2048L,
      cpu_slots = 1L,
      max_concurrent = 4L,
      concurrency_group = "mask_ops"
    ),
    command = "python",
    python = file.path(venv_root, "radiomics", "bin", "python"),
    args_template = list(
      file.path(scripts_dir, "dsimaging_mask_ops.py"),
      "--input", "{input_dir}",
      "--output", "{output_dir}",
      "--operation", "{operation}"
    ),
    timeout_secs = 1800L,
    env = rosetta_mkl_env,
    allowed_params = c("dataset_id", "mask_asset", "mask", "mask_b_asset",
                        "mask_b", "reference_asset", "reference_image",
                        "operation", "threshold", "threshold_b", "label",
                        "labels", "mode", "radius", "min_voxels",
                        "max_components")
  ), "mask_ops", list(
    "-m", "dsimaging_mask_ops",
    "--input", "{input_dir}",
    "--output", "{output_dir}",
    "--operation", "{operation}"
  )))

  # QC metrics. The output table stays server-side until published as an asset.
  .write_runner_yaml(runners_dir, "imaging_qc_metrics", .with_container(list(
    name = "imaging_qc_metrics",
    plane = "artifact",
    resource_class = "cpu",
    resources = list(
      memory_mb = 2048L,
      cpu_slots = 1L,
      max_concurrent = 4L,
      concurrency_group = "imaging_qc"
    ),
    command = "python",
    python = file.path(venv_root, "radiomics", "bin", "python"),
    args_template = list(
      file.path(scripts_dir, "dsimaging_qc_metrics.py"),
      "--input", "{input_dir}",
      "--output", "{output_dir}"
    ),
    timeout_secs = 1800L,
    env = rosetta_mkl_env,
    allowed_params = c("dataset_id", "image_asset", "image_root",
                        "mask_asset", "mask_root")
  ), "imaging_qc_metrics", list(
    "-m", "dsimaging_qc_metrics",
    "--input", "{input_dir}",
    "--output", "{output_dir}"
  )))
}

#' Add optional container metadata to a runner configuration.
#' @keywords internal
.with_container <- function(config, runner, args_template, command = "python") {
  container <- .imaging_container_config(runner, args_template, command)
  if (!is.null(container)) config$container <- container
  config
}

#' Resolve optional dsImaging container image configuration.
#' @keywords internal
.imaging_container_config <- function(runner, args_template, command = "python") {
  images <- .imaging_analysis_option("container_images", list())
  candidates <- list()
  if (is.list(images) && !is.null(images[[runner]]))
    candidates <- c(candidates, list(images[[runner]]))
  candidates <- c(candidates, list(
    .imaging_analysis_option(paste0("container_image_", runner), NULL),
    Sys.getenv(paste0("DSIMAGING_CONTAINER_IMAGE_", toupper(runner)),
      unset = ""),
    Sys.getenv(paste0("DSRADIOMICS_CONTAINER_IMAGE_", toupper(runner)),
      unset = ""),
    .imaging_analysis_option("container_image", NULL),
    Sys.getenv("DSIMAGING_CONTAINER_IMAGE", unset = ""),
    Sys.getenv("DSRADIOMICS_CONTAINER_IMAGE", unset = "")
  ))
  candidates <- vapply(candidates, function(x) {
    if (is.null(x) || length(x) == 0) return("")
    as.character(x)[1]
  }, character(1))
  image <- candidates[nzchar(candidates)]
  image <- if (length(image) > 0) image[1] else ""
  if (!nzchar(image)) return(NULL)
  list(
    image = image,
    runtime = .imaging_analysis_option("container_runtime", "auto"),
    pull = .imaging_analysis_option("container_pull", "missing"),
    command = command,
    args_template = args_template
  )
}

#' @keywords internal
.register_radiomics_runners <- function() {
  .register_imaging_analysis_runners()
}

#' @keywords internal
.radiomics_container_config <- function(runner, args_template,
                                        command = "python") {
  .imaging_container_config(runner, args_template, command)
}

#' Write a runner YAML to DSJOBS_HOME/runners/
#' @keywords internal
.write_runner_yaml <- function(runners_dir, name, config) {
  path <- file.path(runners_dir, paste0(name, ".yml"))
  if (requireNamespace("yaml", quietly = TRUE)) {
    writeLines(yaml::as.yaml(config), path)
  } else {
    writeLines(jsonlite::toJSON(config, auto_unbox = TRUE, pretty = TRUE), path)
  }
  if (!file.exists(path))
    stop("Failed to write runner YAML: ", path, call. = FALSE)
  tryCatch(Sys.chmod(path, "0666"), error = function(e) NULL)
  invisible(path)
}
