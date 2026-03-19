# Module: Package Hooks + Environments

`%||%` <- function(x, y) if (is.null(x)) y else x

# Package-level environment for handle storage
.dsimaging_env <- new.env(parent = emptyenv())

#' Package load hook
#'
#' Builds R6 classes (if resourcer is available) and registers the
#' ImagingDatasetResourceResolver with resourcer.
#'
#' @param libname Library path.
#' @param pkgname Package name.
#' @keywords internal
.onLoad <- function(libname, pkgname) {
  # Build R6 classes (resolver + client) if resourcer is available
  if (requireNamespace("resourcer", quietly = TRUE) &&
      requireNamespace("R6", quietly = TRUE)) {
    .build_resolver_class()
    .build_client_class()

    # Register resolver
    if (!is.null(ImagingDatasetResourceResolver)) {
      resourcer::registerResourceResolver(ImagingDatasetResourceResolver$new())
    }
  }
}
