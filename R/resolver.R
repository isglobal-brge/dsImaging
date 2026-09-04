# Module: Resource Resolver
# ImagingDatasetResourceResolver for direct and Armadillo dataset Resources.
# Defined at file level (resourcer is in Imports).

#' ImagingDatasetResourceResolver R6 class
#'
#' Resolves direct \code{imaging+dataset://} URLs and Armadillo Resources whose
#' strict \code{format} locator is \code{dsimaging-dataset:<dataset_id>}.
#'
#' @export
ImagingDatasetResourceResolver <- R6::R6Class(
  "ImagingDatasetResourceResolver",
  inherit = resourcer::ResourceResolver,
  public = list(
    #' @description Check whether this resolver handles a resource.
    #' @param x Resource-like object with `url` and optional `format` fields.
    isFor = function(x) {
      url <- tryCatch(x[["url", exact = TRUE]], error = function(e) "")
      format <- tryCatch(
        x[["format", exact = TRUE]], error = function(e) NULL)
      (is.character(url) && length(url) == 1L && !is.na(url) &&
       startsWith(url, "imaging+dataset://")) ||
        .is_imaging_dataset_format_locator(format)
    },
    #' @description Create a resource client for the resource.
    #' @param x Resource-like object.
    newClient = function(x) {
      ImagingDatasetResourceClient$new(x)
    }
  )
)
