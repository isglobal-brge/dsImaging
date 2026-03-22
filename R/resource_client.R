# Module: Resource Client
# ImagingDatasetResourceClient for imaging+dataset:// URLs.
# Defined at file level (resourcer is in Imports).

#' ImagingDatasetResourceClient R6 class
#'
#' Resolves \code{imaging+dataset://} URLs to imaging manifests via
#' the registry or via S3 credentials from the Opal resource.
#'
#' @export
ImagingDatasetResourceClient <- R6::R6Class(
  "ImagingDatasetResourceClient",
  inherit = resourcer::ResourceClient,
  private = list(
    .manifest = NULL,
    .dataset_id = NULL,
    .backend = NULL,

    .resolve = function() {
      resource <- self$getResource()
      url <- resource$url %||% ""
      parsed <- .parse_imaging_url(url)
      dataset_id <- parsed$dataset_id

      if (is.null(dataset_id))
        stop("Cannot parse imaging+dataset:// URL: ", url, call. = FALSE)

      private$.dataset_id <- dataset_id

      # Try 1: registry (server has pre-configured registry)
      resolved <- tryCatch(resolve_dataset(dataset_id), error = function(e) NULL)
      if (!is.null(resolved)) {
        private$.backend <- resolved$backend
        private$.manifest <- parse_manifest(resolved$manifest_uri, resolved$backend)
        return(invisible(NULL))
      }

      # Try 2: S3 credentials from Opal resource
      params <- parsed$params %||% list()
      endpoint <- params$endpoint %||% ""
      bucket <- params$bucket %||% "imaging-data"
      prefix <- params$prefix %||% paste0("datasets/", dataset_id)
      ak <- resource$identity %||% ""
      sk <- resource$secret %||% ""

      if (nzchar(ak) && nzchar(sk)) {
        cred_ref <- paste0("resource_", dataset_id)
        store <- getOption("dsimaging.credentials", list())
        store[[cred_ref]] <- list(
          access_key = ak, secret_key = sk, endpoint = endpoint)
        options(dsimaging.credentials = store)

        backend <- storage_backend("s3", config = list(
          endpoint = endpoint, credentials_ref = cred_ref,
          region = params$region))
        manifest_uri <- paste0("s3://", bucket, "/", prefix, "/manifest.yaml")

        private$.backend <- backend
        private$.manifest <- parse_manifest(manifest_uri, backend)
        return(invisible(NULL))
      }

      stop("Cannot resolve dataset '", dataset_id,
           "': not in registry and no S3 credentials in resource.",
           call. = FALSE)
    }
  ),
  public = list(
    initialize = function(resource) {
      super$initialize(resource)
      private$.resolve()
    },
    getManifest = function() private$.manifest,
    getDatasetId = function() private$.dataset_id,
    getBackend = function() private$.backend,

    asDataFrame = function() {
      meta <- private$.manifest$metadata
      if (is.null(meta) || is.null(meta$uri))
        stop("No metadata URI in manifest.", call. = FALSE)

      fmt <- meta$format %||% .guess_format(meta$uri)

      if (grepl("^s3://", meta$uri)) {
        tmp <- tempfile(fileext = paste0(".", fmt))
        backend_get_file(private$.backend, meta$uri, tmp)
        local_path <- tmp
      } else {
        local_path <- meta$uri
      }

      if (identical(fmt, "parquet")) {
        if (!requireNamespace("arrow", quietly = TRUE))
          stop("arrow package required for Parquet.", call. = FALSE)
        return(as.data.frame(arrow::read_parquet(local_path)))
      }
      utils::read.csv(local_path, stringsAsFactors = FALSE)
    },

    asImagingDescriptor = function() {
      imaging_dataset_descriptor(private$.manifest)
    }
  )
)

#' Parse an imaging+dataset:// URL
#' @keywords internal
.parse_imaging_url <- function(url) {
  body <- sub("^imaging\\+dataset://", "", url)
  parts <- strsplit(body, "\\?", fixed = FALSE)[[1]]
  dataset_id <- utils::URLdecode(parts[1])

  params <- list()
  if (length(parts) > 1) {
    query <- parts[2]
    pairs <- strsplit(strsplit(query, "&")[[1]], "=")
    for (pair in pairs) {
      if (length(pair) == 2)
        params[[pair[1]]] <- utils::URLdecode(pair[2])
    }
  }

  list(dataset_id = dataset_id, params = params)
}

#' Guess file format from extension
#' @keywords internal
.guess_format <- function(path) {
  if (grepl("\\.parquet$", path, ignore.case = TRUE)) return("parquet")
  "csv"
}
