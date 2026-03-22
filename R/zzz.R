# Module: Package Hooks

`%||%` <- function(x, y) if (is.null(x)) y else x

# Package-level environment for handle storage and runtime caches
.dsimaging_env <- new.env(parent = emptyenv())

#' @keywords internal
.onLoad <- function(libname, pkgname) {
  resourcer::registerResourceResolver(ImagingDatasetResourceResolver$new())
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
