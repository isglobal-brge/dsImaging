# Module: Resource Resolver
# ImagingDatasetResourceResolver for imaging+dataset:// URLs.

#' ImagingDatasetResourceResolver R6 class
#'
#' Resolves \code{imaging+dataset://} URLs within the resourcer framework.
#' The URL format is:
#' \itemize{
#'   \item \code{imaging+dataset://dataset_id} -- resolve via registry
#'   \item \code{imaging+dataset://manifest?path=/path/to/manifest.yml} --
#'     direct manifest path
#' }
#'
#' @export
ImagingDatasetResourceResolver <- NULL

.build_resolver_class <- function() {
  if (!requireNamespace("resourcer", quietly = TRUE)) return(NULL)

  cls <- R6::R6Class(
    "ImagingDatasetResourceResolver",
    inherit = resourcer::ResourceResolver,
    public = list(
      isFor = function(x) {
        url <- x$url %||% ""
        grepl("^imaging\\+dataset://", url)
      },
      newClient = function(x) {
        .dsimaging_env$client_class$new(x)
      }
    )
  )

  .dsimaging_env$resolver_class <- cls
  # Also set the exported variable if not locked
  tryCatch(
    ImagingDatasetResourceResolver <<- cls,
    error = function(e) NULL)
}

# Build on load if resourcer is available
.onLoad_resolver <- function() {
  .build_resolver_class()
}
