# Module: Package Hooks

`%||%` <- function(x, y) if (is.null(x)) y else x

# Package-level environment for handle storage and runtime caches
.dsimaging_env <- new.env(parent = emptyenv())

#' @keywords internal
.onLoad <- function(libname, pkgname) {
  resourcer::registerResourceResolver(ImagingDatasetResourceResolver$new())
  tryCatch(.asset_db_prepare_path(), error = function(e) NULL)
  .dsimaging_env$onload_errors <- character(0)
  tryCatch(
    .register_imaging_analysis_runners(),
    error = function(e) {
      .dsimaging_env$onload_errors <- c(.dsimaging_env$onload_errors,
        paste("analysis runner registration failed:", conditionMessage(e)))
    })
  if (requireNamespace("dsJobs", quietly = TRUE)) {
    for (entry in list(
      list(kind = "imaging_asset", fn = .imaging_asset_publisher),
      list(kind = "imaging_radiomics_asset", fn = .radiomics_publisher),
      list(kind = "radiomics_asset", fn = .radiomics_publisher),
      list(kind = "imaging_radiomics_image_result", fn = .radiomics_image_publisher),
      list(kind = "radiomics_image_result", fn = .radiomics_image_publisher)
    )) {
      tryCatch(
        dsJobs::register_dsjobs_publisher(entry$kind, entry$fn),
        error = function(e) {
          .dsimaging_env$onload_errors <- c(.dsimaging_env$onload_errors,
            paste(entry$kind, "publisher registration failed:",
                  conditionMessage(e)))
        })
    }
  }
}

#' @keywords internal
.onAttach <- function(libname, pkgname) {
  # Re-register in case .onLoad ran in a different context
  resourcer::registerResourceResolver(ImagingDatasetResourceResolver$new())
}

#' @keywords internal
.onDetach <- function(libpath) {
  tryCatch(
    resourcer::unregisterResourceResolver("ImagingDatasetResourceResolver"),
    error = function(e) NULL)
}
