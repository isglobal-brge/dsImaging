# Module: Python Environment Provisioning

.RADIOMICS_PYTHON_DEPS <- list(
  radiomics = c("pyradiomics>=3.0.1,<3.1", "SimpleITK>=2.0.0", "pandas>=1.3.0",
                "numpy>=1.23.0,<2", "pyyaml>=5.0", "pyarrow>=10.0.0"),
  seg_lungmask = c("lungmask>=0.2.0", "torch>=2.0.0",
                   "SimpleITK>=2.0.0", "nibabel>=4.0.0"),
  seg_totalseg = c("TotalSegmentator>=2.0.0", "torch>=2.0.0",
                    "SimpleITK>=2.0.0", "nibabel>=4.0.0"),
  seg_nnunetv2 = c("nnunetv2>=2.0.0", "torch>=2.0.0",
                    "SimpleITK>=2.0.0", "nibabel>=4.0.0"),
  seg_monai = c("monai>=1.3.0", "torch>=2.0.0",
                 "SimpleITK>=2.0.0", "nibabel>=4.0.0", "pyyaml>=5.0")
)

.RADIOMICS_HEALTH_IMPORTS <- list(
  radiomics = "radiomics",
  seg_lungmask = "lungmask",
  seg_totalseg = "totalsegmentator",
  seg_nnunetv2 = "nnunetv2",
  seg_monai = "monai"
)

#' List available imaging analysis environments
#' @export
list_imaging_analysis_envs <- function() {
  venv_root <- .imaging_analysis_option("venv_root", "/var/lib/dsimaging/venvs")
  envs <- data.frame(
    framework = names(.RADIOMICS_PYTHON_DEPS),
    deps = vapply(.RADIOMICS_PYTHON_DEPS, function(d) paste(d, collapse = ", "), character(1)),
    python = file.path(venv_root, names(.RADIOMICS_PYTHON_DEPS), "bin", "python"),
    ready = file.exists(file.path(venv_root, names(.RADIOMICS_PYTHON_DEPS),
                                  ".dsimaging_ready")),
    import_ok = vapply(names(.RADIOMICS_PYTHON_DEPS), .imaging_env_import_ok,
                       logical(1)),
    stringsAsFactors = FALSE
  )
  envs
}

#' @keywords internal
.imaging_env_import_ok <- function(framework) {
  venv_root <- .imaging_analysis_option("venv_root", "/var/lib/dsimaging/venvs")
  python <- file.path(venv_root, framework, "bin", "python")
  module <- .RADIOMICS_HEALTH_IMPORTS[[framework]]
  if (is.null(module) || !file.exists(python)) return(FALSE)
  status <- tryCatch(
    system2(python, c("-c", shQuote(paste("import", module))),
            stdout = FALSE, stderr = FALSE),
    error = function(e) 1L)
  identical(status, 0L)
}

#' @keywords internal
.imaging_runner_health <- function() {
  home <- getOption("dsjobs.home", getOption("default.dsjobs.home", "/srv/dsjobs"))
  runners_dir <- file.path(home, "runners")
  expected <- c("pyradiomics_extract", "lungmask_infer",
                "ct_lung_threshold",
                "totalsegmentator_infer", "nnunetv2_predict",
                "monai_bundle_infer", "dicom_convert",
                "image_preprocess", "mask_ops", "imaging_qc_metrics")
  data.frame(
    runner = expected,
    path = file.path(runners_dir, paste0(expected, ".yml")),
    present = file.exists(file.path(runners_dir, paste0(expected, ".yml"))),
    stringsAsFactors = FALSE
  )
}

#' List available radiomics environments
#' @export
list_radiomics_envs <- function() {
  list_imaging_analysis_envs()
}

#' @keywords internal
.radiomics_env_import_ok <- function(framework) {
  .imaging_env_import_ok(framework)
}

#' @keywords internal
.radiomics_runner_health <- function() {
  .imaging_runner_health()
}
