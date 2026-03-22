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
.s3_download <- function(config, bucket, key, dest) {
  .s3_call(function() {
    aws.s3::save_object(
      object = key, bucket = bucket, file = dest,
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
    resp <- aws.s3::head_object(
      object = key, bucket = bucket,
      key = config$access_key, secret = config$secret_key,
      base_url = .s3_base_url(config$endpoint),
      region = config$region,
      use_https = .s3_use_https(config$endpoint))
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

#' List objects under a prefix
#' @return Character vector of keys.
#' @keywords internal
.s3_list <- function(config, bucket, prefix, max_keys = 10000L) {
  result <- .s3_call(function() {
    resp <- aws.s3::get_bucket(
      bucket = bucket, prefix = prefix, max = max_keys,
      key = config$access_key, secret = config$secret_key,
      base_url = .s3_base_url(config$endpoint),
      region = config$region,
      use_https = .s3_use_https(config$endpoint))
    vapply(resp, function(x) x$Key, character(1))
  })
  result
}

#' Resolve S3 credentials from ref or env vars
#'
#' Resolution order:
#'   1. Named credential in getOption("dsimaging.credentials")
#'   2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
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
        region = cred$region %||% "us-east-1"))
    }
  }

  # 2. Env vars (dev/CI fallback)
  ak <- Sys.getenv("AWS_ACCESS_KEY_ID", "")
  sk <- Sys.getenv("AWS_SECRET_ACCESS_KEY", "")
  if (nzchar(ak) && nzchar(sk)) {
    return(list(
      access_key = ak, secret_key = sk,
      endpoint = Sys.getenv("AWS_S3_ENDPOINT", ""),
      region = Sys.getenv("AWS_DEFAULT_REGION", "us-east-1")))
  }

  stop("S3 credentials not found. Set credentials_ref in registry ",
       "or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY env vars.",
       call. = FALSE)
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
.s3_call <- function(fn) {
  tryCatch(fn(), error = function(e) {
    stop("S3 operation failed: ", conditionMessage(e), call. = FALSE)
  })
}
