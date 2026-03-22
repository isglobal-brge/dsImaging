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
      .dataset_id = NULL,
      .backend = NULL
    ),
    public = list(
      #' @description Create a new ImagingDatasetResourceClient.
      #' @param resource A resource object with imaging+dataset:// URL.
      initialize = function(resource) {
        super$initialize(resource)
        private$.resolve()
      },

      #' @description Get the parsed manifest.
      getManifest = function() private$.manifest,

      #' @description Get the dataset_id.
      getDatasetId = function() private$.dataset_id,

      #' @description Get the storage backend.
      getBackend = function() private$.backend,

      #' @description Get the metadata as a data.frame.
      asDataFrame = function() {
        meta <- private$.manifest$metadata
        if (is.null(meta) || is.null(meta$uri))
          stop("No metadata URI in manifest.", call. = FALSE)

        fmt <- meta$format %||% .guess_format(meta$uri)

        # Download metadata file if on S3
        if (grepl("^s3://", meta$uri)) {
          tmp <- tempfile(fileext = paste0(".", fmt))
          backend_get_file(private$.backend, meta$uri, tmp)
          local_path <- tmp
        } else {
          local_path <- meta$uri
        }

        if (identical(fmt, "parquet")) {
          if (!requireNamespace("arrow", quietly = TRUE))
            stop("arrow package required for Parquet metadata.", call. = FALSE)
          return(as.data.frame(arrow::read_parquet(local_path)))
        }
        utils::read.csv(local_path, stringsAsFactors = FALSE)
      },

      #' @description Create an ImagingDatasetDescriptor.
      asImagingDescriptor = function() {
        imaging_dataset_descriptor(private$.manifest)
      }
    )
  )

  ImagingDatasetResourceClient$set("private", ".resolve", function() {
    url <- self$getResource()$url %||% ""
    parsed <- .parse_imaging_url(url)

    if (!is.null(parsed$dataset_id)) {
      resolved <- resolve_dataset(parsed$dataset_id)
      private$.dataset_id <- resolved$dataset_id
      private$.backend <- resolved$backend
      private$.manifest <- parse_manifest(resolved$manifest_uri, resolved$backend)
    } else {
      stop("Cannot resolve imaging+dataset:// URL: ", url, call. = FALSE)
    }
  })
}

#' Parse an imaging+dataset:// URL
#' @keywords internal
.parse_imaging_url <- function(url) {
  body <- sub("^imaging\\+dataset://", "", url)
  dataset_id <- utils::URLdecode(body)
  list(dataset_id = dataset_id)
}

#' Guess file format from extension
#' @keywords internal
.guess_format <- function(path) {
  if (grepl("\\.parquet$", path, ignore.case = TRUE)) return("parquet")
  "csv"
}
