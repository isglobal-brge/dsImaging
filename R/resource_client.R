# Module: Resource Client
# ImagingDatasetResourceClient extends ResourceClient.

#' ImagingDatasetResourceClient R6 class
#'
#' A resourcer ResourceClient that resolves \code{imaging+dataset://} URLs
#' to imaging manifests. Provides access to the parsed manifest and can
#' produce an \code{ImagingDatasetDescriptor} for dsFlower integration.
#'
#' @export
ImagingDatasetResourceClient <- NULL

.build_client_class <- function() {
  if (!requireNamespace("resourcer", quietly = TRUE)) return(NULL)

  RC_class <- resourcer::ResourceClient

  ImagingDatasetResourceClient <<- R6::R6Class(
    "ImagingDatasetResourceClient",
    inherit = RC_class,
    private = list(
      .manifest = NULL,
      .dataset_id = NULL
    ),
    public = list(
      #' @description Create a new ImagingDatasetResourceClient.
      #' @param resource A resource object with imaging+dataset:// URL.
      initialize = function(resource) {
        super$initialize(resource)
        private$.resolve()
      },

      #' @description Get the parsed manifest.
      #' @return List; the validated manifest.
      getManifest = function() {
        private$.manifest
      },

      #' @description Get the dataset_id.
      #' @return Character.
      getDatasetId = function() {
        private$.dataset_id
      },

      #' @description Get the metadata as a data.frame.
      #'
      #' Reads the metadata file from the manifest (samples table).
      #' This is what \code{resourcer::as.data.frame()} would call.
      #'
      #' @return A data.frame.
      asDataFrame = function() {
        meta <- private$.manifest$metadata
        if (is.null(meta) || is.null(meta$file)) {
          stop("No metadata file in manifest.", call. = FALSE)
        }

        fmt <- meta$format %||% .guess_format(meta$file)
        if (identical(fmt, "parquet")) {
          if (!requireNamespace("arrow", quietly = TRUE)) {
            stop("arrow package required for Parquet metadata.", call. = FALSE)
          }
          return(as.data.frame(arrow::read_parquet(meta$file)))
        }
        utils::read.csv(meta$file, stringsAsFactors = FALSE)
      },

      #' @description Create an ImagingDatasetDescriptor.
      #' @return An ImagingDatasetDescriptor.
      asImagingDescriptor = function() {
        imaging_dataset_descriptor(private$.manifest)
      }
    )
  )

  # Add private resolve method
  ImagingDatasetResourceClient$set("private", ".resolve", function() {
    url <- self$getResource()$url %||% ""
    parsed <- .parse_imaging_url(url)

    if (!is.null(parsed$manifest_path)) {
      # Direct manifest path
      private$.manifest <- parse_manifest(parsed$manifest_path)
      private$.dataset_id <- private$.manifest$dataset_id
    } else if (!is.null(parsed$dataset_id)) {
      # Registry lookup
      private$.dataset_id <- parsed$dataset_id
      manifest_path <- resolve_dataset(parsed$dataset_id)
      private$.manifest <- parse_manifest(manifest_path)
    } else {
      stop("Cannot resolve imaging+dataset:// URL: ", url, call. = FALSE)
    }
  })
}

#' Parse an imaging+dataset:// URL
#'
#' @param url Character; the URL to parse.
#' @return Named list with \code{dataset_id} and/or \code{manifest_path}.
#' @keywords internal
.parse_imaging_url <- function(url) {
  # Remove scheme
  body <- sub("^imaging\\+dataset://", "", url)

  # Check for query parameter: manifest?path=/path/to/manifest.yml
  if (grepl("\\?path=", body)) {
    path <- sub("^manifest\\?path=", "", body)
    path <- utils::URLdecode(path)
    return(list(dataset_id = NULL, manifest_path = path))
  }

  # Otherwise the body is a dataset_id
  dataset_id <- utils::URLdecode(body)
  list(dataset_id = dataset_id, manifest_path = NULL)
}

#' Guess file format from extension
#' @keywords internal
.guess_format <- function(path) {
  if (grepl("\\.parquet$", path, ignore.case = TRUE)) return("parquet")
  if (grepl("\\.csv$", path, ignore.case = TRUE)) return("csv")
  "csv"
}
