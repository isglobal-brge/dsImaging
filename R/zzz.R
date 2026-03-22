# Module: Package Hooks + Environments

`%||%` <- function(x, y) if (is.null(x)) y else x

# Package-level environment for handle storage
.dsimaging_env <- new.env(parent = emptyenv())

#' Register the ImagingDatasetResourceResolver with resourcer
#' @keywords internal
.register_resolver <- function() {
  if (requireNamespace("resourcer", quietly = TRUE) &&
      requireNamespace("R6", quietly = TRUE)) {
    .build_resolver_class()
    .build_client_class()
    resolver <- .dsimaging_env$resolver_class
    if (!is.null(resolver)) {
      resourcer::registerResourceResolver(resolver$new())
    }
  }
}

#' Package load hook (runs on loadNamespace)
#' @keywords internal
.onLoad <- function(libname, pkgname) {
  .register_resolver()
}

#' Package attach hook (runs on library)
#' @keywords internal
.onAttach <- function(libname, pkgname) {
  # Re-register in case .onLoad didn't run in the right context
  .register_resolver()
}

#' Package detach hook
#' @keywords internal
.onDetach <- function(libpath) {
  if (requireNamespace("resourcer", quietly = TRUE)) {
    tryCatch(
      resourcer::unregisterResourceResolver("ImagingDatasetResourceResolver"),
      error = function(e) NULL)
  }
}
