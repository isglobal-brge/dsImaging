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
    #' @description Check whether this resolver handles a resource.
    #' @param x Resource-like object with a `url` field.
    isFor = function(x) {
      url <- x$url %||% ""
      grepl("^imaging\\+dataset://", url)
    },
    #' @description Create a resource client for the resource.
    #' @param x Resource-like object.
    newClient = function(x) {
      ImagingDatasetResourceClient$new(x)
    }
  )
)
