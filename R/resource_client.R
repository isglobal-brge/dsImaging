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

  cls <- R6::R6Class(
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

  cls$set("private", ".resolve", function() {
    resource <- self$getResource()
    url <- resource$url %||% ""
    parsed <- .parse_imaging_url(url)
    dataset_id <- parsed$dataset_id

    if (is.null(dataset_id))
      stop("Cannot resolve imaging+dataset:// URL: ", url, call. = FALSE)

    private$.dataset_id <- dataset_id

    # Try 1: resolve from registry (server has pre-configured registry)
    resolved <- tryCatch(resolve_dataset(dataset_id), error = function(e) NULL)

    if (!is.null(resolved)) {
      private$.backend <- resolved$backend
      private$.manifest <- parse_manifest(resolved$manifest_uri, resolved$backend)
      return(invisible(NULL))
    }

    # Try 2: construct from resource identity/secret (Opal passes these)
    # Parameters come from the URL query string
    params <- parsed$params %||% list()

    endpoint <- params$endpoint %||% ""
    bucket <- params$bucket %||% "imaging-data"
    prefix <- params$prefix %||% paste0("datasets/", dataset_id)

    # If the resource has identity+secret, use them as S3 credentials
    ak <- resource$identity %||% ""
    sk <- resource$secret %||% ""

    if (nzchar(ak) && nzchar(sk)) {
      # Set up credentials in the credential store for this session
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
  })

  .dsimaging_env$client_class <- cls
  tryCatch(
    ImagingDatasetResourceClient <<- cls,
    error = function(e) NULL)
}

#' Parse an imaging+dataset:// URL
#'
#' Supports query params: endpoint, bucket, prefix, region
#' @keywords internal
.parse_imaging_url <- function(url) {
  body <- sub("^imaging\\+dataset://", "", url)

  # Split dataset_id from query string
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
