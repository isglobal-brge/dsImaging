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
      parsed <- .validate_imaging_resource_location(.parse_imaging_url(url))
      dataset_id <- parsed$dataset_id

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
        location <- .validate_imaging_resource_location(list(
          dataset_id = dataset_id,
          params = list(endpoint = endpoint, bucket = bucket, prefix = prefix,
            region = params$region %||% ""),
          s3 = TRUE))
        endpoint <- location$params$endpoint
        bucket <- location$params$bucket
        prefix <- location$params$prefix
        # The S3 backend keeps a reference to the resourcer Resource object and
        # resolves identity/secret FROM it on demand -- credentials are never
        # copied into the backend, stashed in options, or written to disk.
        backend <- storage_backend("s3", config = list(
          resource = resource, endpoint = endpoint, region = params$region))
        manifest_uri <- paste0("s3://", bucket, "/", prefix, "/manifest.yaml")

        private$.backend <- backend
        private$.manifest_uri <- manifest_uri
        manifest <- parse_manifest(manifest_uri, backend)
        .assert_imaging_resource_dataset(manifest, dataset_id)
        .assert_imaging_manifest_scope(manifest, manifest_uri, backend)
        private$.manifest <- manifest
        return(invisible(NULL))
      }

      # Try 2: server pre-configured registry (bare imaging+dataset://<id>
      # URLs with no credentials on the resource).
      resolved <- tryCatch(resolve_dataset(dataset_id), error = function(e) NULL)
      if (!is.null(resolved)) {
        if (!identical(as.character(resolved$dataset_id), dataset_id)) {
          stop("Imaging resource does not match its dataset.", call. = FALSE)
        }
        manifest <- parse_manifest(resolved$manifest_uri, resolved$backend)
        .assert_imaging_resource_dataset(manifest, dataset_id)
        .assert_imaging_manifest_scope(
          manifest, resolved$manifest_uri, resolved$backend)
        private$.backend <- resolved$backend
        private$.manifest_uri <- resolved$manifest_uri
        private$.manifest <- manifest
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
      .validate_imaging_resource_object(resource)
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

    #' @description Refuse generic data-frame materialization. Imaging
    #' resources must pass the patient-level admission performed by
    #' \code{imagingInitDS()}.
    asDataFrame = function() {
      stop("Imaging resources must be initialized with imagingInitDS().",
           call. = FALSE)
    },

    #' @description Convert the manifest to an imaging dataset descriptor.
    asImagingDescriptor = function() {
      imaging_dataset_descriptor(private$.manifest)
    }
  )
)

#' Validate the provenance-bearing resourcer object accepted by the client
#' @keywords internal
.validate_imaging_resource_object <- function(resource) {
  if (!inherits(resource, c("resource", "Resource")) ||
      !is.list(resource)) {
    stop("Invalid imaging dataset resource.", call. = FALSE)
  }
  for (field in c("name", "url")) {
    value <- resource[[field]]
    if (!is.character(value) || length(value) != 1L || is.na(value) ||
        !nzchar(value) || nchar(value, type = "bytes") > 2048L) {
      stop("Invalid imaging dataset resource.", call. = FALSE)
    }
  }
  for (field in c("identity", "secret")) {
    value <- resource[[field]]
    if (!is.null(value) &&
        (!is.character(value) || length(value) != 1L || is.na(value))) {
      stop("Invalid imaging dataset resource.", call. = FALSE)
    }
  }
  if (!grepl("^imaging\\+dataset://", resource$url)) {
    stop("Invalid imaging dataset resource.", call. = FALSE)
  }
  invisible(TRUE)
}

#' Validate one parsed resource location before any backend access
#' @keywords internal
.validate_imaging_resource_location <- function(parsed) {
  fail <- function() stop("Invalid imaging dataset resource URL.", call. = FALSE)
  if (!is.list(parsed)) fail()
  dataset_id <- parsed$dataset_id
  if (!is.character(dataset_id) || length(dataset_id) != 1L ||
      is.na(dataset_id) || nchar(dataset_id, type = "bytes") > 128L ||
      !grepl("^[a-z0-9][a-z0-9._-]*$", dataset_id) ||
      grepl("..", dataset_id, fixed = TRUE)) fail()
  params <- parsed$params %||% list()
  if (!is.list(params) || (length(params) > 0L &&
      (is.null(names(params)) ||
       any(!names(params) %in% c("endpoint", "bucket", "prefix", "region")) ||
       anyDuplicated(names(params))))) fail()

  if (isTRUE(parsed$s3)) {
    endpoint <- params$endpoint %||% ""
    bucket <- params$bucket
    prefix <- params$prefix
    region <- params$region %||% ""
    scalar <- function(x, allow_empty = FALSE, max_bytes = 1024L) {
      is.character(x) && length(x) == 1L && !is.na(x) &&
        nchar(x, type = "bytes") <= max_bytes &&
        (isTRUE(allow_empty) || nzchar(x))
    }
    if (!scalar(endpoint, allow_empty = TRUE, max_bytes = 512L) ||
        (nzchar(endpoint) && !grepl(
          "^https?://[A-Za-z0-9][A-Za-z0-9.-]*(?::[0-9]{1,5})?$",
          endpoint, perl = TRUE))) fail()
    if (nzchar(endpoint)) {
      port <- sub("^.*:([0-9]{1,5})$", "\\1", endpoint)
      if (!identical(port, endpoint) && as.integer(port) > 65535L) fail()
    }
    if (!scalar(bucket, max_bytes = 63L) || nchar(bucket) < 3L ||
        !grepl("^[a-z0-9][a-z0-9.-]*[a-z0-9]$", bucket) ||
        grepl("..", bucket, fixed = TRUE)) fail()
    if (!scalar(prefix) || startsWith(prefix, "/") || endsWith(prefix, "/"))
      fail()
    segments <- strsplit(prefix, "/", fixed = TRUE)[[1L]]
    if (length(segments) == 0L || any(!grepl(
        "^[A-Za-z0-9][A-Za-z0-9._-]*$", segments)) ||
        any(segments %in% c(".", "..")) ||
        !identical(segments[[length(segments)]], dataset_id)) fail()
    if (!scalar(region, allow_empty = TRUE, max_bytes = 64L) ||
        (nzchar(region) && !grepl("^[A-Za-z0-9][A-Za-z0-9-]*$", region)))
      fail()
  }
  parsed
}

#' Bind a fetched manifest to the dataset named by its Resource URL
#' @keywords internal
.assert_imaging_resource_dataset <- function(manifest, dataset_id) {
  if (!is.list(manifest) ||
      !identical(as.character(manifest$dataset_id), dataset_id)) {
    stop("Imaging resource does not match its dataset.", call. = FALSE)
  }
  invisible(TRUE)
}

#' Require every manifest-declared resource to remain inside its collection
#' @keywords internal
.assert_imaging_manifest_scope <- function(manifest, manifest_uri, backend) {
  fail <- function() {
    stop("Imaging manifest references data outside its collection scope.",
         call. = FALSE)
  }
  if (!is.list(manifest) || !is.list(backend) ||
      !is.character(backend$type) || length(backend$type) != 1L ||
      !backend$type %in% c("file", "s3") ||
      !is.character(manifest_uri) || length(manifest_uri) != 1L ||
      is.na(manifest_uri) || !nzchar(manifest_uri)) fail()

  references <- list()
  remember <- function(value, label) {
    if (is.null(value)) return(invisible(NULL))
    if (is.list(value)) {
      fields <- intersect(c("uri", "file", "path", "root", "manifest"),
                          names(value) %||% character(0))
      for (field in fields) {
        remember(value[[field]], paste0(label, ".", field))
      }
      return(invisible(NULL))
    }
    if (!is.character(value) || length(value) != 1L || is.na(value) ||
        !nzchar(value) || nchar(value, type = "bytes") > 4096L ||
        grepl("[\r\n]", value)) fail()
    references[[length(references) + 1L]] <<- list(
      value = value, label = label)
    invisible(NULL)
  }
  remember(manifest$metadata$uri %||% NULL, "metadata.uri")
  remember(manifest$metadata$file %||% NULL, "metadata.file")
  for (name in names(manifest$assets %||% list())) {
    asset <- manifest$assets[[name]]
    for (field in c("uri", "file", "path", "root", "manifest",
                    "content_hash_index", "hash_index", "index_uri")) {
      remember(asset[[field]] %||% NULL,
               paste0("assets.", name, ".", field))
    }
  }
  for (i in seq_along(manifest$labels %||% list())) {
    label <- manifest$labels[[i]]
    remember(label$uri %||% NULL, paste0("labels.", i, ".uri"))
    remember(label$file %||% NULL, paste0("labels.", i, ".file"))
  }
  remember(manifest$content_hash_index %||% NULL, "content_hash_index")
  remember(manifest$sample_manifests %||% NULL, "sample_manifests")

  valid_segments <- function(path) {
    !grepl("[\\\\\r\n]", path) &&
      !any(strsplit(path, "/", fixed = TRUE)[[1L]] %in% c(".", ".."))
  }
  within <- function(candidate, root) {
    identical(candidate, root) || startsWith(candidate, paste0(root, "/"))
  }

  if (identical(backend$type, "s3")) {
    if (!grepl("^s3://", manifest_uri) || !valid_segments(manifest_uri))
      fail()
    source <- tryCatch(.parse_s3_uri(manifest_uri), error = function(e) NULL)
    if (is.null(source) || !nzchar(source$bucket) || !nzchar(source$key))
      fail()
    source_key <- sub("/+$", "", source$key)
    scope_key <- dirname(source_key)
    if (!nzchar(scope_key) || identical(scope_key, ".")) fail()
    for (reference in references) {
      uri <- reference$value
      if (!grepl("^s3://", uri) || !valid_segments(uri)) fail()
      parsed <- tryCatch(.parse_s3_uri(uri), error = function(e) NULL)
      if (is.null(parsed) || !identical(parsed$bucket, source$bucket)) fail()
      key <- sub("/+$", "", parsed$key)
      if (!nzchar(key) || !within(key, scope_key)) fail()
    }
    return(invisible(TRUE))
  }

  if (grepl("^s3://", manifest_uri) || !startsWith(manifest_uri, "/") ||
      !valid_segments(manifest_uri)) fail()
  canonical <- function(path) {
    path <- sub("/+$", "", path)
    if (!nzchar(path) || !startsWith(path, "/") || !valid_segments(path))
      fail()
    current <- path
    suffix <- character(0)
    while (!file.exists(current) && !dir.exists(current)) {
      parent <- dirname(current)
      if (identical(parent, current)) fail()
      suffix <- c(basename(current), suffix)
      current <- parent
    }
    resolved <- tryCatch(
      normalizePath(current, winslash = "/", mustWork = TRUE),
      error = function(e) NULL)
    if (is.null(resolved)) fail()
    if (length(suffix) > 0L) {
      resolved <- do.call(file.path, as.list(c(resolved, suffix)))
    }
    sub("/+$", "", resolved)
  }
  scope_path <- canonical(dirname(manifest_uri))
  if (identical(scope_path, "/")) fail()
  for (reference in references) {
    uri <- reference$value
    if (grepl("^s3://", uri)) fail()
    if (!within(canonical(uri), scope_path)) fail()
  }
  invisible(TRUE)
}

#' Parse an imaging+dataset:// URL
#'
#' Supports two formats:
#'   Clean: imaging+dataset://host:port/bucket/datasets/collection with an
#'   optional `@region` suffix.
#'   Legacy: imaging+dataset://dataset_id?endpoint=...&bucket=...&prefix=...
#' @keywords internal
.parse_imaging_url <- function(url) {
  if (!is.character(url) || length(url) != 1L || is.na(url) ||
      !grepl("^imaging\\+dataset://", url) || grepl("[\r\n]", url)) {
    stop("Invalid imaging dataset resource URL.", call. = FALSE)
  }
  body <- sub("^imaging\\+dataset://", "", url)

  # Legacy query-param format: dataset_id?endpoint=...&bucket=...&prefix=...
  qmark <- regexpr("\\?", body)
  if (qmark > 0) {
    dataset_id <- substr(body, 1, qmark - 1)
    query_str <- substr(body, qmark + 1, nchar(body))
    pairs <- strsplit(query_str, "&")[[1]]
    params <- list()
    for (p in pairs) {
      equals <- regexpr("=", p, fixed = TRUE)
      if (equals < 1L) {
        stop("Invalid imaging dataset resource URL.", call. = FALSE)
      }
      key <- utils::URLdecode(substr(p, 1L, equals - 1L))
      value <- utils::URLdecode(substr(p, equals + 1L, nchar(p)))
      if (!key %in% c("endpoint", "bucket", "prefix", "region") ||
          key %in% names(params)) {
        stop("Invalid imaging dataset resource URL.", call. = FALSE)
      }
      params[[key]] <- value
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
