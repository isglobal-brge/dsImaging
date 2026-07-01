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
    .manifest_uri = NULL,
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

      # Try 1: the resource itself carries S3 endpoint + credentials. The
      # resource is the single source of truth, so it takes PRECEDENCE over any
      # server-side registry entry (a stale registry row must never shadow the
      # credentials the caller explicitly attached to the resource).
      params <- parsed$params %||% list()
      endpoint <- params$endpoint %||% ""
      bucket <- params$bucket %||% "imaging-data"
      prefix <- params$prefix %||% paste0("datasets/", dataset_id)
      # Opal may pass identity=NULL due to credential mapping quirks; the
      # helper tries several field names. This is only a presence check to
      # decide the S3 path -- the keys are NOT copied anywhere here.
      creds <- .resource_s3_credentials(resource)

      if (nzchar(creds$access_key) && nzchar(creds$secret_key)) {
        # The S3 backend keeps a reference to the resourcer Resource object and
        # resolves identity/secret FROM it on demand -- credentials are never
        # copied into the backend, stashed in options, or written to disk.
        backend <- storage_backend("s3", config = list(
          resource = resource, endpoint = endpoint, region = params$region))
        manifest_uri <- paste0("s3://", bucket, "/", prefix, "/manifest.yaml")

        private$.backend <- backend
        private$.manifest_uri <- manifest_uri
        private$.manifest <- parse_manifest(manifest_uri, backend)
        return(invisible(NULL))
      }

      # Try 2: server pre-configured registry (bare imaging+dataset://<id>
      # URLs with no credentials on the resource).
      resolved <- tryCatch(resolve_dataset(dataset_id), error = function(e) NULL)
      if (!is.null(resolved)) {
        private$.backend <- resolved$backend
        private$.manifest_uri <- resolved$manifest_uri
        private$.manifest <- parse_manifest(resolved$manifest_uri, resolved$backend)
        return(invisible(NULL))
      }

      stop("Cannot resolve dataset '", dataset_id,
           "': no S3 credentials on the resource and no registry entry.",
           call. = FALSE)
    }
  ),
  public = list(
    #' @description Create and resolve an imaging dataset resource client.
    #' @param resource Resource object.
    initialize = function(resource) {
      super$initialize(resource)
      private$.resolve()
    },
    #' @description Return the parsed imaging manifest.
    getManifest = function() private$.manifest,
    #' @description Return the resolved manifest URI.
    getManifestUri = function() private$.manifest_uri,
    #' @description Return the dataset identifier.
    getDatasetId = function() private$.dataset_id,
    #' @description Return the resolved storage backend.
    getBackend = function() private$.backend,

    #' @description Load the manifest metadata table as a data frame.
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

    #' @description Convert the manifest to an imaging dataset descriptor.
    asImagingDescriptor = function() {
      imaging_dataset_descriptor(private$.manifest)
    }
  )
)

#' Parse an imaging+dataset:// URL
#'
#' Supports two formats:
#'   Clean: imaging+dataset://host:port/bucket/datasets/collection with an
#'   optional `@region` suffix.
#'   Legacy: imaging+dataset://dataset_id?endpoint=...&bucket=...&prefix=...
#' @keywords internal
.parse_imaging_url <- function(url) {
  body <- sub("^imaging\\+dataset://", "", url)

  # Legacy query-param format: dataset_id?endpoint=...&bucket=...&prefix=...
  qmark <- regexpr("\\?", body)
  if (qmark > 0) {
    dataset_id <- substr(body, 1, qmark - 1)
    query_str <- substr(body, qmark + 1, nchar(body))
    # URL-decode the query string then parse params
    query_str <- utils::URLdecode(query_str)
    pairs <- strsplit(query_str, "&")[[1]]
    params <- list()
    for (p in pairs) {
      kv <- strsplit(p, "=", fixed = TRUE)[[1]]
      if (length(kv) == 2) params[[kv[1]]] <- kv[2]
    }
    return(list(
      dataset_id = dataset_id,
      params = list(
        endpoint = params$endpoint %||% "",
        bucket = params$bucket %||% "imaging-data",
        prefix = params$prefix %||% paste0("datasets/", dataset_id),
        region = params$region %||% ""
      ),
      s3 = TRUE
    ))
  }

  # Clean format: host:port/bucket/collection[@region]
  # Check for @region suffix
  region_override <- ""
  at_pos <- regexpr("@[a-z]", body)
  if (at_pos > 0) {
    region_override <- substr(body, at_pos + 1, nchar(body))
    body <- substr(body, 1, at_pos - 1)
  }

  first_slash <- regexpr("/", body)
  if (first_slash > 0) {
    host <- substr(body, 1, first_slash - 1)
    rest <- substr(body, first_slash + 1, nchar(body))
    second_slash <- regexpr("/", rest)
    if (second_slash > 0) {
      bucket <- substr(rest, 1, second_slash - 1)
      collection <- substr(rest, second_slash + 1, nchar(rest))
    } else {
      bucket <- rest
      collection <- ""
    }

    # Auto-detect scheme: IPs and known local hosts -> http, else https
    scheme <- if (grepl("^(\\d|localhost|minio|host\\.docker)", host)) "http" else "https"
    endpoint <- paste0(scheme, "://", host)

    # Region: explicit @region wins, else auto-detect
    region <- if (nzchar(region_override)) {
      region_override
    } else if (grepl("^(\\d|localhost|minio|\\.local|host\\.docker)", host)) {
      ""
    } else {
      "us-east-1"
    }

    dataset_id <- basename(sub("/$", "", collection))
    return(list(
      dataset_id = dataset_id,
      params = list(
        endpoint = endpoint,
        bucket = bucket,
        prefix = collection,
        region = region
      ),
      s3 = TRUE
    ))
  }

  # Fallback: bare dataset_id (registry lookup)
  list(dataset_id = body, params = list(), s3 = FALSE)
}

#' Guess file format from extension
#' @keywords internal
.guess_format <- function(path) {
  if (grepl("\\.parquet$", path, ignore.case = TRUE)) return("parquet")
  "csv"
}
