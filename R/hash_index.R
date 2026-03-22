# Module: Content Hash Index
#
# Per-dataset Parquet index of content hashes for all samples.
# Enables dedup checks without downloading files from S3.
#
# Schema: sample_id, uri, content_hash, size, last_modified, version_id, etag

#' Read a content hash index from backend
#'
#' @param backend A dsimaging_backend.
#' @param index_uri Character; URI of the Parquet index.
#' @return data.frame with columns: sample_id, uri, content_hash, size.
#' @export
read_hash_index <- function(backend, index_uri) {
  if (!requireNamespace("arrow", quietly = TRUE))
    stop("arrow package required for Parquet hash index.", call. = FALSE)

  # Download to temp if S3
  if (backend$type == "s3") {
    tmp <- tempfile(fileext = ".parquet")
    on.exit(unlink(tmp), add = TRUE)
    backend_get_file(backend, index_uri, tmp)
    local_path <- tmp
  } else {
    if (!file.exists(index_uri)) return(.empty_hash_index())
    local_path <- index_uri
  }

  tryCatch(
    as.data.frame(arrow::read_parquet(local_path)),
    error = function(e) .empty_hash_index())
}

#' Write a content hash index to backend (atomic replace)
#'
#' For S3 backends: writes to a temp key first, then overwrites the target.
#' This prevents partial writes from corrupting the index if the upload
#' is interrupted. Two concurrent writers may still race, but neither
#' will produce a corrupt file -- the last writer wins.
#'
#' @param backend A dsimaging_backend.
#' @param index_uri Character; destination URI.
#' @param df data.frame with the index data.
#' @export
write_hash_index <- function(backend, index_uri, df) {
  if (!requireNamespace("arrow", quietly = TRUE))
    stop("arrow package required for Parquet hash index.", call. = FALSE)

  tmp <- tempfile(fileext = ".parquet")
  on.exit(unlink(tmp), add = TRUE)
  arrow::write_parquet(df, tmp)

  # For S3: write to a staging key, then copy to final location
  # This ensures the final key is always a complete file
  if (backend$type == "s3") {
    staging_uri <- sub("\\.parquet$", paste0(".staging_", Sys.getpid(),
      "_", as.integer(Sys.time()), ".parquet"), index_uri)
    backend_put_file(backend, tmp, staging_uri)
    # Overwrite final key (S3 PUTs are atomic per-object)
    backend_put_file(backend, tmp, index_uri)
    # Clean up staging key (best effort)
    tryCatch({
      parsed <- .parse_s3_uri(staging_uri)
      config <- .resolve_backend_s3_config(backend)
      aws.s3::delete_object(parsed$key, bucket = parsed$bucket,
        key = config$access_key, secret = config$secret_key,
        base_url = .s3_base_url(config$endpoint),
        region = config$region,
        use_https = .s3_use_https(config$endpoint))
    }, error = function(e) NULL)
  } else {
    backend_put_file(backend, tmp, index_uri)
  }
}

#' Diff a hash index against stored hashes in SQLite
#'
#' @param index_df data.frame from read_hash_index.
#' @param dataset_id Character; for looking up stored hashes.
#' @return Named list: new, changed, unchanged (character vectors of sample_ids),
#'   plus content_hashes (named list).
#' @export
diff_hash_index <- function(index_df, dataset_id) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))

  stored <- DBI::dbGetQuery(db,
    "SELECT sample_id, content_hash FROM content_fingerprints WHERE dataset_id = ?",
    params = list(dataset_id))
  stored_map <- stats::setNames(stored$content_hash, stored$sample_id)

  new_ids <- character(0)
  changed_ids <- character(0)
  unchanged_ids <- character(0)
  ch_map <- list()

  for (i in seq_len(nrow(index_df))) {
    sid <- index_df$sample_id[i]
    ch <- index_df$content_hash[i]
    ch_map[[sid]] <- ch

    stored_ch <- stored_map[sid]
    if (is.na(stored_ch)) {
      new_ids <- c(new_ids, sid)
    } else if (stored_ch != ch) {
      changed_ids <- c(changed_ids, sid)
    } else {
      unchanged_ids <- c(unchanged_ids, sid)
    }
  }

  list(
    new = new_ids,
    changed = changed_ids,
    unchanged = unchanged_ids,
    content_hashes = ch_map,
    total = nrow(index_df)
  )
}

#' Build a content hash index for a dataset (admin tool)
#'
#' Iterates all files under a URI prefix, computes SHA-256 for each,
#' and writes the index to the specified output URI.
#'
#' @param backend A dsimaging_backend.
#' @param prefix Character; URI prefix to scan.
#' @param output_uri Character; where to write the Parquet index.
#' @param incremental Logical; if TRUE, only hash files not in existing index.
#' @return Invisible data.frame of the built index.
#' @export
build_hash_index <- function(backend, prefix, output_uri, incremental = FALSE) {
  existing <- if (incremental) {
    tryCatch(read_hash_index(backend, output_uri),
             error = function(e) .empty_hash_index())
  } else .empty_hash_index()

  existing_uris <- existing$uri

  files <- backend_list(backend, prefix)
  # Filter to image files only
  img_exts <- c("\\.nii\\.gz$", "\\.nii$", "\\.nrrd$", "\\.mha$",
                "\\.mhd$", "\\.dcm$")
  img_pattern <- paste(img_exts, collapse = "|")
  files <- files[grepl(img_pattern, files, ignore.case = TRUE)]

  if (incremental) {
    files <- setdiff(files, existing_uris)
  }

  if (length(files) == 0) {
    message("No new files to index.")
    if (nrow(existing) > 0) return(invisible(existing))
    return(invisible(.empty_hash_index()))
  }

  message("Hashing ", length(files), " files...")
  rows <- lapply(files, function(uri) {
    tmp <- tempfile()
    on.exit(unlink(tmp), add = TRUE)
    tryCatch({
      backend_get_file(backend, uri, tmp)
      ch <- .sha256_file(tmp)
      info <- file.info(tmp)
      sid <- .sample_id_from_uri(uri)
      data.frame(
        sample_id = sid, uri = uri, content_hash = ch,
        size = as.integer(info$size),
        last_modified = format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"),
        version_id = NA_character_, etag = NA_character_,
        stringsAsFactors = FALSE)
    }, error = function(e) {
      warning("Skipping ", uri, ": ", conditionMessage(e), call. = FALSE)
      NULL
    })
  })

  new_rows <- do.call(rbind, Filter(Negate(is.null), rows))
  if (is.null(new_rows)) new_rows <- .empty_hash_index()

  full_index <- rbind(existing, new_rows)
  write_hash_index(backend, output_uri, full_index)
  message("Index written: ", nrow(full_index), " entries")
  invisible(full_index)
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

.empty_hash_index <- function() {
  data.frame(
    sample_id = character(0), uri = character(0),
    content_hash = character(0), size = integer(0),
    last_modified = character(0), version_id = character(0),
    etag = character(0), stringsAsFactors = FALSE)
}

.sha256_file <- function(path) {
  digest::digest(file = path, algo = "sha256")
}

.sample_id_from_uri <- function(uri) {
  base <- basename(uri)
  for (ext in c(".nii.gz", ".nii", ".nrrd", ".mha", ".mhd", ".dcm"))
    if (endsWith(tolower(base), ext))
      return(substr(base, 1, nchar(base) - nchar(ext)))
  tools::file_path_sans_ext(base)
}
