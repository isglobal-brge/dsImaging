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

  R6_class <- resourcer::ResourceResolver

  ImagingDatasetResourceResolver <<- R6::R6Class(
    "ImagingDatasetResourceResolver",
    inherit = R6_class,
    public = list(
      #' @description Check if this resolver handles the given resource.
      #' @param x A resource object.
      #' @return Logical.
      isFor = function(x) {
        url <- x$url %||% ""
        grepl("^imaging\\+dataset://", url)
      },

      #' @description Create a ResourceClient for the resource.
      #' @param x A resource object.
      #' @return An ImagingDatasetResourceClient.
      newClient = function(x) {
        ImagingDatasetResourceClient$new(x)
      }
    )
  )
}

# Build on load if resourcer is available
.onLoad_resolver <- function() {
  .build_resolver_class()
}
