# Module: S3 Backend Operations
#
# Low-level S3 operations via aws.s3 package. All functions are internal.
# The public API is in backend.R.

#' Parse an S3 URI into bucket and key
#' @param uri Character; "s3://bucket/key/path"
#' @return Named list: bucket, key.
#' @keywords internal
.parse_s3_uri <- function(uri) {
  if (!grepl("^s3://", uri))
    stop("Not an S3 URI: ", uri, call. = FALSE)
  stripped <- sub("^s3://", "", uri)
  slash_pos <- regexpr("/", stripped)
  if (slash_pos == -1L) {
    return(list(bucket = stripped, key = ""))
  }
  list(
    bucket = substr(stripped, 1, slash_pos - 1),
    key = substr(stripped, slash_pos + 1, nchar(stripped))
  )
}

#' Build an S3 URI from bucket and key
#' @keywords internal
.build_s3_uri <- function(bucket, key) {
  if (!nzchar(key)) return(paste0("s3://", bucket))
  paste0("s3://", bucket, "/", key)
}

#' Download an S3 object to a local file
#' @keywords internal
.s3_download <- function(config, bucket, key, dest, version_id = NULL) {
  if (!is.null(version_id) &&
      (!is.character(version_id) || length(version_id) != 1L ||
       is.na(version_id) || !nzchar(version_id) ||
       nchar(version_id, type = "bytes") > 1024L ||
       grepl("[\r\n]", version_id))) {
    stop("S3 object version is invalid.", call. = FALSE)
  }
  query <- if (is.null(version_id)) NULL else list(versionId = version_id)
  .s3_call(function() {
    aws.s3::save_object(
      object = key, bucket = bucket, file = dest,
      query = query,
      key = config$access_key, secret = config$secret_key,
      base_url = .s3_base_url(config$endpoint),
      region = config$region,
      use_https = .s3_use_https(config$endpoint))
  })
}

#' Upload a local file to S3
#' @keywords internal
.s3_upload <- function(config, local_path, bucket, key) {
  .s3_call(function() {
    aws.s3::put_object(
      file = local_path, object = key, bucket = bucket,
      key = config$access_key, secret = config$secret_key,
      base_url = .s3_base_url(config$endpoint),
      region = config$region,
      use_https = .s3_use_https(config$endpoint))
  })
}

#' HEAD request on S3 object
#' @return list(exists, size, last_modified) or NULL
#' @keywords internal
.s3_head <- function(config, bucket, key) {
  result <- tryCatch({
    resp <- .s3_quiet_call(function() {
      aws.s3::head_object(
        object = key, bucket = bucket,
        key = config$access_key, secret = config$secret_key,
        base_url = .s3_base_url(config$endpoint),
        region = config$region,
        use_https = .s3_use_https(config$endpoint))
    })
    if (isTRUE(as.logical(resp))) {
      headers <- attributes(resp)
      list(
        exists = TRUE,
        size = as.integer(headers$`content-length` %||% NA_integer_),
        last_modified = headers$`last-modified` %||% NA_character_)
    } else NULL
  }, error = function(e) NULL)
  result
}

#' List objects under a prefix (with pagination for >1000 objects)
#' @return Character vector of keys.
#' @keywords internal
.s3_list <- function(config, bucket, prefix) {
  all_keys <- character(0)
  marker <- ""

  repeat {
    resp <- .s3_call(function() {
      aws.s3::get_bucket(
        bucket = bucket, prefix = prefix, max = 1000L,
        marker = if (nzchar(marker)) marker else NULL,
        key = config$access_key, secret = config$secret_key,
        base_url = .s3_base_url(config$endpoint),
        region = config$region,
        use_https = .s3_use_https(config$endpoint))
    })

    if (length(resp) == 0) break
    keys <- vapply(resp, function(x) x$Key, character(1))
    all_keys <- c(all_keys, keys)

    # S3 returns IsTruncated; aws.s3::get_bucket returns attr "IsTruncated"
    truncated <- attr(resp, "IsTruncated")
    if (is.null(truncated) || !identical(tolower(as.character(truncated)), "true"))
      break

    marker <- keys[length(keys)]
  }

  all_keys
}

#' Auto-detect region for self-hosted S3 (MinIO, etc.)
#'
#' If endpoint looks like a direct IP/localhost, region should be ""
#' to prevent aws.s3 from prepending region to hostname.
#' @keywords internal
.auto_region <- function(endpoint, explicit_region = NULL) {
  if (!is.null(explicit_region) && nzchar(explicit_region))
    return(explicit_region)
  if (is.null(endpoint) || !nzchar(endpoint))
    return("us-east-1")  # AWS default
  host <- sub("^https?://", "", endpoint)
  host <- sub(":[0-9]+$", "", host)
  # IP addresses, localhost, container service names and .local hosts are
  # self-hosted endpoints, so aws.s3 must not prepend an AWS region to them.
  if (grepl("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$", host) ||
      grepl("^localhost$", host, ignore.case = TRUE) ||
      grepl("^minio", host, ignore.case = TRUE) ||
      grepl("^host\\.docker", host, ignore.case = TRUE) ||
      !grepl("\\.", host, fixed = FALSE) ||
      grepl("\\.local$", host, ignore.case = TRUE))
    return("")
  "us-east-1"
}

#' Resolve S3 credentials from ref or env vars
#'
#' Resolution order:
#'   1. Named credential in getOption("dsimaging.credentials")
#'   2. Named credential persisted under dsimaging.credentials_path
#'   3. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
#'
#' @param ref Character or NULL; credential reference name.
#' @return Named list: access_key, secret_key, endpoint, region.
#' @keywords internal
.resolve_s3_credentials <- function(ref = NULL) {
  # 1. Named credential store
  if (!is.null(ref) && nzchar(ref)) {
    store <- getOption("dsimaging.credentials", list())
    cred <- store[[ref]]
    if (!is.null(cred)) {
      return(list(
        access_key = cred$access_key %||% cred$identity,
        secret_key = cred$secret_key %||% cred$secret,
        endpoint = cred$endpoint,
        region = cred$region))
    }

    cred <- .load_persisted_s3_credential(ref)
    if (!is.null(cred)) {
      return(list(
        access_key = cred$access_key %||% cred$identity,
        secret_key = cred$secret_key %||% cred$secret,
        endpoint = cred$endpoint,
        region = cred$region))
    }
  }

  # 3. Env vars (dev/CI fallback)
  ak <- Sys.getenv("AWS_ACCESS_KEY_ID", "")
  sk <- Sys.getenv("AWS_SECRET_ACCESS_KEY", "")
  if (nzchar(ak) && nzchar(sk)) {
    ep <- Sys.getenv("AWS_S3_ENDPOINT", "")
    rg <- Sys.getenv("AWS_DEFAULT_REGION", "")
    return(list(
      access_key = ak, secret_key = sk,
      endpoint = ep,
      region = if (nzchar(rg)) rg else NULL))
  }

  stop("S3 credentials not found. Set credentials_ref in registry ",
       "or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY env vars.",
       call. = FALSE)
}

#' Persist S3 credentials for worker-side dataset resolution
#' @keywords internal
.persist_s3_credential <- function(ref, cred) {
  if (is.null(ref) || !nzchar(ref) || is.null(cred)) return(invisible(FALSE))
  path <- getOption("dsimaging.credentials_path",
    getOption("default.dsimaging.credentials_path",
      file.path(getOption("dsimaging.data_dir",
        getOption("default.dsimaging.data_dir", "/var/lib/dsimaging")),
        "credentials.yaml")))
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE,
             mode = "0700")
  tryCatch(Sys.chmod(dirname(path), "0700", use_umask = FALSE),
           error = function(e) NULL)
  lock_path <- paste0(path, ".lock")
  deadline <- Sys.time() + 5
  repeat {
    if (isTRUE(dir.create(lock_path, showWarnings = FALSE, mode = "0700"))) {
      break
    }
    if (Sys.time() >= deadline) {
      stop("Credential store is busy.", call. = FALSE)
    }
    Sys.sleep(0.02)
  }
  on.exit(unlink(lock_path, recursive = TRUE, force = TRUE), add = TRUE)

  store <- tryCatch({
    if (file.exists(path)) yaml::yaml.load_file(path) else list()
  }, error = function(e) {
    stop("Credential store is unavailable.", call. = FALSE)
  })
  if (is.null(store) || !is.list(store)) {
    stop("Credential store is unavailable.", call. = FALSE)
  }
  store[[ref]] <- list(
    access_key = cred$access_key %||% cred$identity,
    secret_key = cred$secret_key %||% cred$secret,
    endpoint = cred$endpoint %||% NULL,
    region = cred$region %||% NULL
  )
  tmp <- tempfile(pattern = ".credentials-", tmpdir = dirname(path),
                  fileext = ".yaml")
  on.exit(unlink(tmp, force = TRUE), add = TRUE)
  yaml::write_yaml(store, tmp)
  tryCatch(Sys.chmod(tmp, "0600", use_umask = FALSE),
           error = function(e) NULL)
  if (!file.rename(tmp, path)) {
    stop("Could not atomically update credential store.", call. = FALSE)
  }
  invisible(TRUE)
}

#' @keywords internal
.load_persisted_s3_credential <- function(ref) {
  path <- getOption("dsimaging.credentials_path",
    getOption("default.dsimaging.credentials_path",
      file.path(getOption("dsimaging.data_dir",
        getOption("default.dsimaging.data_dir", "/var/lib/dsimaging")),
        "credentials.yaml")))
  if (!file.exists(path)) return(NULL)
  store <- tryCatch(yaml::yaml.load_file(path), error = function(e) NULL)
  if (is.null(store) || !is.list(store)) return(NULL)
  store[[ref]]
}

#' Read S3 credentials directly from a resourcer Resource object
#'
#' The resource is the single source of truth: identity/secret are read from
#' it on demand and never copied into options, disk, or the backend. Several
#' field names are tried because Opal's credential mapping may populate
#' different ones (identity/secret vs identifier/password, etc.).
#'
#' @param resource A resourcer Resource object.
#' @return Named list: access_key, secret_key (empty strings if absent).
#' @keywords internal
.resource_s3_credentials <- function(resource) {
  ak <- ""
  for (field in c("identity", "identifier", "access_key", "username")) {
    v <- tryCatch(resource[[field]], error = function(e) NULL)
    if (!is.null(v) && nzchar(v)) { ak <- v; break }
  }
  sk <- ""
  for (field in c("secret", "password", "secret_key")) {
    v <- tryCatch(resource[[field]], error = function(e) NULL)
    if (!is.null(v) && nzchar(v)) { sk <- v; break }
  }
  list(access_key = ak, secret_key = sk)
}

#' Keep an S3 backend safe for a private, session-bound handle
#'
#' A Resource object may remain referenced by the package-private handle
#' registry so same-session consumers can fetch collection objects lazily. Its
#' credentials are never copied to options, disk, descriptors, or job records.
#' Durable worker contexts are handled separately by
#' `.portable_backend_context()` and require administrator-managed
#' credentials.
#' @keywords internal
.sanitize_imaging_backend <- function(backend, dataset_id) {
  if (is.null(backend) || !identical(backend$type, "s3")) return(backend)
  cfg <- backend$config %||% list()
  raw_fields <- c("access_key", "secret_key", "identity", "secret",
                  "password")
  if (any(raw_fields %in% names(cfg))) {
    stop("Raw storage credentials are not accepted in imaging backends.",
         call. = FALSE)
  }
  ref <- cfg$credentials_ref %||% NULL
  clean <- list(
    endpoint = cfg$endpoint %||% NULL,
    credentials_ref = ref,
    region = cfg$region %||% NULL,
    uri_prefix = cfg$uri_prefix %||% NULL
  )
  if (!is.null(cfg$resource)) clean$resource <- cfg$resource
  storage_backend("s3", config = clean)
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

#' Extract base_url from endpoint (strip scheme)
#' @keywords internal
.s3_base_url <- function(endpoint) {
  if (is.null(endpoint) || !nzchar(endpoint)) return("")
  sub("^https?://", "", endpoint)
}

#' Determine if endpoint uses HTTPS
#' @keywords internal
.s3_use_https <- function(endpoint) {
  if (is.null(endpoint) || !nzchar(endpoint)) return(TRUE)
  !grepl("^http://", endpoint)
}

#' Wrapper for aws.s3 calls with error handling
#' @keywords internal
.s3_quiet_call <- function(fn) {
  if (!is.function(fn)) {
    stop("Invalid S3 operation.", call. = FALSE)
  }
  result <- NULL
  invisible(utils::capture.output(
    result <- withCallingHandlers(
      suppressMessages(fn()),
      warning = function(w) invokeRestart("muffleWarning")),
    type = "output"))
  result
}

#' Wrapper for aws.s3 calls with error handling
#' @keywords internal
.s3_call <- function(fn) {
  tryCatch(.s3_quiet_call(fn), error = function(e) {
    stop("S3 operation failed.", call. = FALSE)
  })
}
