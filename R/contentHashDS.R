# Module: MinIO Content Hash Provider
#
# dsHPC only sees opaque resource names and content hashes. dsImaging owns the
# mapping from DataSHIELD imaging resources to MinIO/S3 objects and keeps the
# non-disclosive resource hash index.

#' Resolve an option using the dsHPC-style option chain
#' @keywords internal
.dsimagingstore_option <- function(name, default = NULL) {
  value <- getOption(paste0("dsimagingstore.", name), NULL)
  if (!is.null(value)) return(value)
  value <- getOption(paste0("default.dsimagingstore.", name), NULL)
  if (!is.null(value)) return(value)

  env_name <- paste0("DSIMAGINGSTORE_",
    toupper(gsub("[^A-Za-z0-9]+", "_", name)))
  env_value <- Sys.getenv(env_name, unset = NA_character_)
  if (!is.na(env_value) && nzchar(env_value)) return(env_value)
  default
}

#' @keywords internal
.content_hash_settings <- function() {
  buckets <- .dsimagingstore_option("minio_buckets", character(0))
  if (length(buckets) == 1L && is.character(buckets))
    buckets <- unlist(strsplit(buckets, "[,;]", perl = TRUE), use.names = FALSE)
  buckets <- trimws(as.character(buckets %||% character(0)))
  buckets <- buckets[nzchar(buckets)]

  mode <- .dsimagingstore_option("notify_mode", "webhook")
  mode <- match.arg(as.character(mode), c("webhook", "poll"))

  list(
    minio_endpoint = .dsimagingstore_option("minio_endpoint", NULL),
    minio_access_key = .dsimagingstore_option("minio_access_key", NULL),
    minio_secret_key = .dsimagingstore_option("minio_secret_key", NULL),
    minio_buckets = buckets,
    minio_webhook_port = as.integer(
      .dsimagingstore_option("minio_webhook_port", 9100L)),
    notify_mode = mode,
    poll_interval_secs = as.numeric(
      .dsimagingstore_option("poll_interval_secs", 30))
  )
}

#' @keywords internal
.content_hash_ensure_schema <- function(db) {
  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS resource_content_hashes (
      resource_name TEXT PRIMARY KEY,
      content_hash  TEXT NOT NULL,
      updated_at    TEXT NOT NULL,
      source        TEXT NOT NULL,
      bucket        TEXT,
      object_key    TEXT
    )")
  invisible(db)
}

#' @keywords internal
.content_hash_db_connect <- function() {
  db <- .asset_db_connect()
  .content_hash_ensure_schema(db)
  db
}

#' @keywords internal
.content_hash_now <- function() {
  format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
}

#' @keywords internal
.content_hash_row <- function(db, resource_name) {
  row <- DBI::dbGetQuery(db,
    "SELECT resource_name, content_hash, updated_at, source, bucket, object_key
     FROM resource_content_hashes
     WHERE resource_name = ?
     LIMIT 1",
    params = list(resource_name))
  if (nrow(row) == 0L) return(NULL)
  as.list(row[1, , drop = FALSE])
}

#' @keywords internal
.content_hash_upsert <- function(resource_name, content_hash, source,
                                 bucket = NA_character_,
                                 object_key = NA_character_,
                                 updated_at = .content_hash_now()) {
  if (is.null(content_hash) || is.na(content_hash) || !nzchar(content_hash))
    stop("content_hash must be a non-empty string.", call. = FALSE)

  db <- .content_hash_db_connect()
  on.exit(.asset_db_close(db), add = TRUE)

  current <- .content_hash_row(db, resource_name)
  if (!is.null(current) &&
      identical(current$content_hash, as.character(content_hash)) &&
      identical(current$source, as.character(source)) &&
      identical(current$bucket %||% NA_character_, bucket %||% NA_character_) &&
      identical(current$object_key %||% NA_character_,
                object_key %||% NA_character_)) {
    return(invisible(FALSE))
  }

  DBI::dbExecute(db,
    "INSERT OR REPLACE INTO resource_content_hashes
     (resource_name, content_hash, updated_at, source, bucket, object_key)
     VALUES (?, ?, ?, ?, ?, ?)",
    params = list(resource_name, as.character(content_hash),
      as.character(updated_at), as.character(source),
      bucket %||% NA_character_, object_key %||% NA_character_))
  invisible(TRUE)
}

#' @keywords internal
.content_hash_get <- function(resource_name) {
  db <- .content_hash_db_connect()
  on.exit(.asset_db_close(db), add = TRUE)
  .content_hash_row(db, resource_name)
}

#' @keywords internal
.content_hash_uri_value <- function(x) {
  if (is.null(x)) return(NULL)
  if (is.list(x)) return(x$uri %||% NULL)
  as.character(x)[1]
}

#' @keywords internal
.resource_locator <- function(resource_name) {
  resource_name <- as.character(resource_name)[1]
  if (!nzchar(resource_name)) return(NULL)

  if (grepl("^s3://", resource_name)) {
    parsed <- .parse_s3_uri(resource_name)
    return(list(bucket = parsed$bucket, object_key = parsed$key,
      backend = NULL, resource_name = resource_name))
  }

  resolved <- tryCatch(resolve_dataset(resource_name), error = function(e) NULL)
  if (!is.null(resolved) && identical(resolved$backend$type, "s3")) {
    manifest <- tryCatch(parse_manifest(resolved$manifest_uri, resolved$backend),
      error = function(e) NULL)
    index_uri <- NULL
    if (!is.null(manifest)) {
      index_uri <- .content_hash_uri_value(manifest$content_hash_index)
      if (is.null(index_uri) && !is.null(manifest$assets)) {
        for (asset in manifest$assets) {
          index_uri <- .content_hash_uri_value(asset$content_hash_index)
          if (!is.null(index_uri)) break
        }
      }
    }
    target_uri <- index_uri %||% resolved$manifest_uri
    if (!is.null(target_uri) && grepl("^s3://", target_uri)) {
      parsed <- .parse_s3_uri(target_uri)
      return(list(bucket = parsed$bucket, object_key = parsed$key,
        backend = resolved$backend, resource_name = resource_name))
    }
  }

  asset <- tryCatch({
    db <- .asset_db_connect()
    on.exit(.asset_db_close(db), add = TRUE)
    row <- DBI::dbGetQuery(db,
      "SELECT path_or_root FROM assets
       WHERE asset_id = ? OR asset_id IN (
         SELECT asset_id FROM asset_aliases WHERE alias = ?
       )
       ORDER BY created_at DESC
       LIMIT 1",
      params = list(resource_name, resource_name))
    if (nrow(row) == 0L) NULL else row$path_or_root[1]
  }, error = function(e) NULL)
  if (!is.null(asset) && grepl("^s3://", asset)) {
    parsed <- .parse_s3_uri(asset)
    return(list(bucket = parsed$bucket, object_key = parsed$key,
      backend = NULL, resource_name = resource_name))
  }

  NULL
}

#' @keywords internal
.content_hash_s3_config <- function(locator = NULL) {
  if (!is.null(locator$backend) && identical(locator$backend$type, "s3"))
    return(.resolve_backend_s3_config(locator$backend))

  settings <- .content_hash_settings()
  list(
    endpoint = settings$minio_endpoint %||% "",
    access_key = settings$minio_access_key %||% Sys.getenv("AWS_ACCESS_KEY_ID"),
    secret_key = settings$minio_secret_key %||% Sys.getenv("AWS_SECRET_ACCESS_KEY"),
    region = .auto_region(settings$minio_endpoint %||% "",
      Sys.getenv("AWS_DEFAULT_REGION", unset = NULL))
  )
}

#' @keywords internal
.content_hash_head <- function(bucket, object_key, locator = NULL) {
  head_fn <- getOption("dsimagingstore.object_head", NULL)
  if (is.function(head_fn))
    return(head_fn(bucket, object_key, locator = locator))

  config <- .content_hash_s3_config(locator)
  resp <- aws.s3::head_object(
    object = object_key, bucket = bucket,
    key = config$access_key, secret = config$secret_key,
    base_url = .s3_base_url(config$endpoint),
    region = config$region,
    use_https = .s3_use_https(config$endpoint))
  attrs <- attributes(resp)
  list(
    etag = attrs$etag %||% attrs$ETag %||% attrs$`x-amz-meta-etag`,
    checksum_sha256 = attrs$`x-amz-checksum-sha256` %||%
      attrs$ChecksumSHA256 %||% attrs$checksum_sha256,
    last_modified = attrs$`last-modified` %||% attrs$LastModified
  )
}

#' @keywords internal
.content_hash_normalize_etag <- function(etag) {
  if (is.null(etag) || length(etag) == 0L) return(NULL)
  etag <- trimws(as.character(etag[1]))
  etag <- gsub('^"|"$', "", etag)
  if (!nzchar(etag)) return(NULL)
  etag
}

#' @keywords internal
.content_hash_stream_object <- function(bucket, object_key, locator = NULL) {
  reader <- getOption("dsimagingstore.object_reader", NULL)
  tmp <- tempfile()
  on.exit(unlink(tmp), add = TRUE)

  if (is.function(reader)) {
    value <- reader(bucket, object_key, locator = locator)
    if (is.raw(value)) {
      writeBin(value, tmp)
      return(digest::digest(file = tmp, algo = "sha256"))
    }
    if (is.character(value) && length(value) == 1L && file.exists(value))
      return(digest::digest(file = value, algo = "sha256"))
    if (is.character(value) && length(value) == 1L) {
      writeBin(charToRaw(value), tmp)
      return(digest::digest(file = tmp, algo = "sha256"))
    }
    stop("Unsupported dsimagingstore.object_reader return value.",
      call. = FALSE)
  }

  config <- .content_hash_s3_config(locator)
  .s3_download(config, bucket, object_key, tmp)
  digest::digest(file = tmp, algo = "sha256")
}

#' @keywords internal
.content_hash_compute <- function(bucket, object_key,
                                  checksum_sha256 = NULL, etag = NULL,
                                  locator = NULL) {
  checksum_sha256 <- checksum_sha256 %||% NA_character_
  if (!is.na(checksum_sha256) && nzchar(as.character(checksum_sha256)[1])) {
    return(list(content_hash = as.character(checksum_sha256)[1],
      source = "checksum_sha256"))
  }

  etag <- .content_hash_normalize_etag(etag)
  if (!is.null(etag) && grepl("^[0-9a-fA-F]{32}$", etag) &&
      !grepl("-", etag, fixed = TRUE)) {
    # Single-part S3 ETags are MD5, not SHA-256. They are stable for exact
    # content equality, but source records the caveat explicitly.
    return(list(content_hash = tolower(etag), source = "etag_md5"))
  }

  list(
    content_hash = .content_hash_stream_object(bucket, object_key, locator),
    source = "stream"
  )
}

#' @keywords internal
.content_hash_resource_name <- function(bucket, object_key) {
  mapper <- getOption("dsimagingstore.resource_mapper", NULL)
  if (is.function(mapper)) {
    mapped <- mapper(bucket, object_key)
    if (!is.null(mapped) && length(mapped) == 1L && nzchar(mapped))
      return(as.character(mapped))
  }
  .build_s3_uri(bucket, object_key)
}

#' @keywords internal
.content_hash_extract_records <- function(payload) {
  if (is.raw(payload)) payload <- rawToChar(payload)
  if (is.character(payload)) {
    payload <- paste(payload, collapse = "\n")
    payload <- jsonlite::fromJSON(payload, simplifyVector = FALSE)
  }
  records <- payload$Records %||% list(payload)
  if (!is.list(records)) stop("Invalid MinIO event payload.", call. = FALSE)
  records
}

#' Handle a MinIO/S3 bucket notification payload
#' @keywords internal
.content_hash_handle_minio_event <- function(payload) {
  records <- .content_hash_extract_records(payload)
  processed <- list()

  for (record in records) {
    bucket <- record$s3$bucket$name %||% record$bucket %||%
      record$Bucket
    object_key <- record$s3$object$key %||% record$object_key %||%
      record$key %||% record$Key
    if (is.null(bucket) || is.null(object_key))
      stop("MinIO event missing bucket or object key.", call. = FALSE)

    bucket <- as.character(bucket)[1]
    object_key <- utils::URLdecode(as.character(object_key)[1])
    object <- record$s3$object %||% list()
    etag <- object$eTag %||% object$etag %||% record$eTag %||% record$etag
    checksum <- object$ChecksumSHA256 %||% object$checksumSHA256 %||%
      object$checksum_sha256 %||%
      record$responseElements$`x-amz-checksum-sha256` %||%
      record$requestParameters$`x-amz-checksum-sha256`
    resource_name <- .content_hash_resource_name(bucket, object_key)
    computed <- .content_hash_compute(bucket, object_key,
      checksum_sha256 = checksum, etag = etag,
      locator = list(bucket = bucket, object_key = object_key, backend = NULL))
    .content_hash_upsert(resource_name, computed$content_hash, computed$source,
      bucket = bucket, object_key = object_key)
    processed[[length(processed) + 1L]] <- data.frame(
      resource_name = resource_name,
      content_hash = computed$content_hash,
      source = computed$source,
      bucket = bucket,
      object_key = object_key,
      stringsAsFactors = FALSE
    )
  }

  if (length(processed) == 0L) {
    return(data.frame(resource_name = character(0), content_hash = character(0),
      source = character(0), bucket = character(0), object_key = character(0)))
  }
  do.call(rbind, processed)
}

#' Return a non-disclosive content hash for an imaging resource
#'
#' The method returns only resource-level metadata: `resource_name`,
#' `content_hash`, `updated_at`, and `source`. A single-part S3 ETag is recorded
#' as `source = "etag_md5"` because it is a stable content-equivalent for that
#' upload mode, not a SHA-256 digest.
#'
#' @param resource_name Character scalar; DataSHIELD imaging resource name,
#'   dataset id, asset id, alias, or direct `s3://bucket/key` URI.
#' @return Single-row list with `resource_name`, `content_hash`, `updated_at`,
#'   and `source`. Unsupported non-MinIO resources return `content_hash = NA`
#'   and `source = "unsupported"`.
#' @export
contentHashDS <- function(resource_name) {
  resource_name <- as.character(resource_name)[1]
  row <- .content_hash_get(resource_name)
  if (!is.null(row)) {
    return(list(resource_name = row$resource_name,
      content_hash = row$content_hash,
      updated_at = row$updated_at,
      source = row$source))
  }

  locator <- .resource_locator(resource_name)
  if (is.null(locator)) {
    return(list(resource_name = resource_name,
      content_hash = NA_character_,
      updated_at = NA_character_,
      source = "unsupported"))
  }

  head <- .content_hash_head(locator$bucket, locator$object_key, locator)
  computed <- .content_hash_compute(locator$bucket, locator$object_key,
    checksum_sha256 = head$checksum_sha256, etag = head$etag,
    locator = locator)
  .content_hash_upsert(resource_name, computed$content_hash, computed$source,
    bucket = locator$bucket, object_key = locator$object_key)

  row <- .content_hash_get(resource_name)
  list(resource_name = row$resource_name,
    content_hash = row$content_hash,
    updated_at = row$updated_at,
    source = row$source)
}

#' Seed the resource content-hash index from configured MinIO buckets
#'
#' Walks every configured bucket and stores a stable non-disclosive hash for
#' each object. The operation is idempotent and may take minutes on large stores.
#'
#' @return Invisibly, a data.frame of indexed objects.
#' @export
seed_content_hashes <- function() {
  settings <- .content_hash_settings()
  lister <- getOption("dsimagingstore.object_lister", NULL)
  indexed <- list()

  for (bucket in settings$minio_buckets) {
    message("Indexing MinIO bucket: ", bucket)
    keys <- if (is.function(lister)) {
      lister(bucket)
    } else {
      .s3_list(.content_hash_s3_config(), bucket, prefix = "")
    }
    if (is.data.frame(keys)) keys <- keys$key %||% keys$Key
    keys <- as.character(keys %||% character(0))
    keys <- keys[nzchar(keys)]

    for (object_key in keys) {
      resource_name <- .content_hash_resource_name(bucket, object_key)
      head <- .content_hash_head(bucket, object_key,
        locator = list(bucket = bucket, object_key = object_key,
          backend = NULL))
      computed <- .content_hash_compute(bucket, object_key,
        checksum_sha256 = head$checksum_sha256, etag = head$etag,
        locator = list(bucket = bucket, object_key = object_key,
          backend = NULL))
      .content_hash_upsert(resource_name, computed$content_hash,
        computed$source, bucket = bucket, object_key = object_key)
      indexed[[length(indexed) + 1L]] <- data.frame(
        resource_name = resource_name,
        content_hash = computed$content_hash,
        source = computed$source,
        bucket = bucket,
        object_key = object_key,
        stringsAsFactors = FALSE
      )
    }
  }

  out <- if (length(indexed) == 0L) {
    data.frame(resource_name = character(0), content_hash = character(0),
      source = character(0), bucket = character(0), object_key = character(0))
  } else {
    do.call(rbind, indexed)
  }
  invisible(out)
}

#' Start or stop the MinIO content-hash listener
#'
#' `start_content_hash_listener()` runs an initial reconciliation via
#' `seed_content_hashes()` and then starts either an HTTP webhook listener
#' (`notify_mode = "webhook"`) or a polling worker (`notify_mode = "poll"`).
#'
#' @return Listener handle invisibly.
#' @export
start_content_hash_listener <- function() {
  settings <- .content_hash_settings()
  tryCatch(seed_content_hashes(), error = function(e) {
    message("Initial content-hash reconcile failed: ", conditionMessage(e))
  })

  if (identical(settings$notify_mode, "webhook")) {
    if (!requireNamespace("httpuv", quietly = TRUE))
      stop("httpuv package required for webhook mode.", call. = FALSE)

    app <- list(call = function(req) {
      if (!identical(req$REQUEST_METHOD, "POST")) {
        return(list(status = 405L, headers = list("Content-Type" = "text/plain"),
          body = "method not allowed"))
      }
      len <- suppressWarnings(as.integer(req$CONTENT_LENGTH %||% 0L))
      body <- if (len > 0L) {
        rawToChar(readBin(req$rook.input, "raw", n = len))
      } else {
        ""
      }
      result <- tryCatch({
        handled <- .content_hash_handle_minio_event(body)
        list(ok = TRUE, processed = nrow(handled))
      }, error = function(e) {
        list(ok = FALSE, error = conditionMessage(e))
      })
      status <- if (isTRUE(result$ok)) 200L else 400L
      list(status = status,
        headers = list("Content-Type" = "application/json"),
        body = as.character(jsonlite::toJSON(result, auto_unbox = TRUE)))
    })
    server <- httpuv::startServer("0.0.0.0",
      settings$minio_webhook_port, app)
    .dsimaging_env$content_hash_listener <- list(mode = "webhook",
      server = server, port = settings$minio_webhook_port)
    return(invisible(.dsimaging_env$content_hash_listener))
  }

  expr <- substitute({
    repeat {
      try(dsImaging::seed_content_hashes(), silent = TRUE)
      Sys.sleep(interval)
    }
  }, list(interval = settings$poll_interval_secs))
  process <- processx::process$new("Rscript",
    c("-e", paste(deparse(expr), collapse = "\n")),
    stdout = "|", stderr = "|")
  .dsimaging_env$content_hash_listener <- list(mode = "poll",
    process = process)
  invisible(.dsimaging_env$content_hash_listener)
}

#' @rdname start_content_hash_listener
#' @export
stop_content_hash_listener <- function() {
  handle <- .dsimaging_env$content_hash_listener
  if (is.null(handle)) return(invisible(FALSE))
  if (identical(handle$mode, "webhook")) {
    tryCatch(httpuv::stopServer(handle$server), error = function(e) NULL)
  } else if (identical(handle$mode, "poll")) {
    tryCatch(handle$process$kill(), error = function(e) NULL)
  }
  .dsimaging_env$content_hash_listener <- NULL
  invisible(TRUE)
}
