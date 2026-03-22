# Module: Resource Resolver
# ImagingDatasetResourceResolver for imaging+dataset:// URLs.
# Defined at file level (resourcer is in Imports).

#' ImagingDatasetResourceResolver R6 class
#'
#' Resolves \code{imaging+dataset://} URLs within the resourcer framework.
#'
#' @export
ImagingDatasetResourceResolver <- R6::R6Class(
  "ImagingDatasetResourceResolver",
  inherit = resourcer::ResourceResolver,
  public = list(
    isFor = function(x) {
      url <- x$url %||% ""
      grepl("^imaging\\+dataset://", url)
    },
    newClient = function(x) {
      ImagingDatasetResourceClient$new(x)
    }
  )
)
