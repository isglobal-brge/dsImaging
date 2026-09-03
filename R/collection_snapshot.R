# Private collection snapshots used at trusted server-package boundaries.

.snapshot_fail <- function() {
  stop("Imaging collection integrity could not be verified.", call. = FALSE)
}

.snapshot_scalar <- function(value, max_bytes = 4096L, empty = FALSE) {
  is.character(value) && length(value) == 1L && !is.na(value) &&
    nchar(value, type = "bytes") <= max_bytes &&
    !grepl("[\r\n]", value) && (isTRUE(empty) || nzchar(value))
}

.snapshot_safe_relative_path <- function(path, directory = FALSE) {
  if (!.snapshot_scalar(path) || grepl("[\\\\]", path) ||
      startsWith(path, "/") || grepl("^[A-Za-z]:", path)) {
    .snapshot_fail()
  }
  trimmed <- if (isTRUE(directory)) sub("/+$", "", path) else path
  parts <- strsplit(trimmed, "/", fixed = TRUE)[[1L]]
  if (!nzchar(trimmed) || any(!nzchar(parts)) ||
      any(parts %in% c(".", ".."))) {
    .snapshot_fail()
  }
  if (!isTRUE(directory) && endsWith(path, "/")) .snapshot_fail()
  trimmed
}

.snapshot_artifact_uri <- function(entry, backend) {
  if (!is.list(entry)) .snapshot_fail()
  uri <- entry$uri %||% NULL
  file <- entry$file %||% NULL
  locations <- Filter(Negate(is.null), list(uri, file))
  if (!length(locations) ||
      any(!vapply(locations, .snapshot_scalar, logical(1)))) {
    .snapshot_fail()
  }
  if (length(locations) == 2L) {
    same <- identical(uri, file)
    if (!same && identical(backend$type, "file") &&
        !grepl("^s3://", uri) && !grepl("^s3://", file)) {
      left <- tryCatch(normalizePath(uri, winslash = "/", mustWork = TRUE),
                       error = function(e) NULL)
      right <- tryCatch(normalizePath(file, winslash = "/", mustWork = TRUE),
                        error = function(e) NULL)
      same <- !is.null(left) && identical(left, right)
    }
    if (!isTRUE(same)) .snapshot_fail()
  }
  uri %||% file
}

.snapshot_read_table <- function(entry, backend, label) {
  uri <- .snapshot_artifact_uri(entry, backend)
  format <- tolower(entry$format %||% .guess_format(uri %||% ""))
  if (!.snapshot_scalar(uri) || !format %in% c("csv", "parquet")) {
    .snapshot_fail()
  }
  local <- uri
  cleanup <- FALSE
  if (grepl("^s3://", uri)) {
    local <- tempfile(fileext = paste0(".", format))
    cleanup <- TRUE
    tryCatch(backend_get_file(backend, uri, local),
             error = function(e) .snapshot_fail())
  }
  if (cleanup) on.exit(unlink(local, force = TRUE), add = TRUE)
  if (!file.exists(local) || dir.exists(local) ||
      file.access(local, 4L) != 0L) {
    .snapshot_fail()
  }
  hash <- tryCatch(
    digest::digest(file = local, algo = "sha256"),
    error = function(e) .snapshot_fail())
  data <- tryCatch({
    if (identical(format, "parquet")) {
      as.data.frame(arrow::read_parquet(local))
    } else {
      utils::read.csv(local, stringsAsFactors = FALSE, check.names = FALSE)
    }
  }, error = function(e) .snapshot_fail())
  if (!is.data.frame(data)) .snapshot_fail()
  list(name = label, uri = uri, format = format, sha256 = hash, data = data)
}

.snapshot_relative_object <- function(uri, root, backend_type) {
  if (!.snapshot_scalar(uri) || !.snapshot_scalar(root)) .snapshot_fail()
  if (identical(backend_type, "s3")) {
    parsed <- tryCatch(.parse_s3_uri(uri), error = function(e) NULL)
    parsed_root <- tryCatch(.parse_s3_uri(root), error = function(e) NULL)
    if (is.null(parsed) || is.null(parsed_root) ||
        !identical(parsed$bucket, parsed_root$bucket)) {
      .snapshot_fail()
    }
    root_key <- paste0(sub("/+$", "", parsed_root$key), "/")
    if (!startsWith(parsed$key, root_key)) .snapshot_fail()
    relative <- substring(parsed$key, nchar(root_key) + 1L)
  } else {
    canonical <- function(path, must_work) {
      tryCatch(normalizePath(path, winslash = "/", mustWork = must_work),
               error = function(e) NULL)
    }
    root_path <- canonical(root, TRUE)
    object_path <- canonical(uri, TRUE)
    if (is.null(root_path) || is.null(object_path) ||
        !startsWith(object_path, paste0(sub("/+$", "", root_path), "/"))) {
      .snapshot_fail()
    }
    relative <- substring(
      object_path, nchar(sub("/+$", "", root_path)) + 2L)
  }
  .snapshot_safe_relative_path(relative, directory = endsWith(uri, "/"))
}

.snapshot_parse_files <- function(value) {
  if (!.snapshot_scalar(value, max_bytes = 1024L * 1024L)) .snapshot_fail()
  files <- tryCatch(
    jsonlite::fromJSON(value, simplifyVector = FALSE),
    error = function(e) NULL)
  if (!is.list(files) || !length(files)) .snapshot_fail()
  parsed <- lapply(files, function(item) {
    if (!is.list(item)) .snapshot_fail()
    path <- .snapshot_safe_relative_path(item$path %||% "")
    role <- item$role %||% ""
    if (!.snapshot_scalar(role, max_bytes = 64L) ||
        !grepl("^[a-z][a-z0-9_-]*$", role)) {
      .snapshot_fail()
    }
    list(path = path, role = role)
  })
  paths <- vapply(parsed, `[[`, character(1), "path")
  if (anyDuplicated(paths)) .snapshot_fail()
  parsed
}

.new_imaging_collection_snapshot <- function(manifest, backend, admission) {
  if (!is.list(manifest) || !inherits(backend, "dsimaging_backend") ||
      !is.list(admission)) {
    .snapshot_fail()
  }
  image_asset <- (manifest$assets %||% list())$images
  image_root <- image_asset$uri %||% NULL
  image_kind <- image_asset$kind %||% image_asset$type %||% NULL
  if (!is.list(image_asset) || !.snapshot_scalar(image_root) ||
      !identical(image_kind, "image_root")) {
    .snapshot_fail()
  }

  metadata <- .snapshot_read_table(manifest$metadata, backend, "metadata")
  sample_manifests <- .snapshot_read_table(
    manifest$sample_manifests, backend, "sample_manifests")
  content_index <- .snapshot_read_table(
    manifest$content_hash_index, backend, "content_hash_index")

  meta <- metadata$data
  sm <- sample_manifests$data
  idx <- content_index$data
  if (!identical(meta, admission$data)) .snapshot_fail()
  required_sm <- c("sample_id", "source_kind", "primary_uri", "files_json",
                   "content_hash", "n_files")
  required_idx <- c("sample_id", "uri", "content_hash", "size",
                    "source_kind")
  if (!all(required_sm %in% names(sm)) ||
      !all(required_idx %in% names(idx))) {
    .snapshot_fail()
  }
  ids <- admission$roster$sample_ids
  normalize_ids <- function(values) {
    values <- .canonical_imaging_privacy_ids(values)
    if (length(values) != length(ids) || anyNA(values) ||
        any(!nzchar(values)) || anyDuplicated(values) ||
        !identical(sort(unname(values), method = "radix"), ids)) {
      .snapshot_fail()
    }
    values
  }
  meta_ids <- normalize_ids(meta[[admission$contract$id_col]])
  sm_ids <- normalize_ids(sm$sample_id)
  idx_ids <- normalize_ids(idx$sample_id)
  meta <- meta[match(ids, meta_ids), , drop = FALSE]
  sm <- sm[match(ids, sm_ids), , drop = FALSE]
  idx <- idx[match(ids, idx_ids), , drop = FALSE]

  privacy_ids <- .canonical_imaging_privacy_ids(
    meta[[admission$contract$privacy_unit_col]])
  .assert_exact_imaging_roster(ids, admission$roster,
    privacy_ids = privacy_ids, context = "imaging collection")

  scalar_column <- function(values, empty = FALSE) {
    values <- as.character(values)
    if (length(values) != length(ids) || anyNA(values) ||
        any(!vapply(values, .snapshot_scalar, logical(1), empty = empty))) {
      .snapshot_fail()
    }
    values
  }
  sm_kind <- scalar_column(sm$source_kind)
  idx_kind <- scalar_column(idx$source_kind)
  hashes <- tolower(scalar_column(idx$content_hash))
  sm_hashes <- tolower(scalar_column(sm$content_hash))
  if (!identical(sm_kind, idx_kind) ||
      any(!sm_kind %in% c("single_file", "dicom_series")) ||
      !identical(hashes, sm_hashes) ||
      any(!grepl("^[0-9a-f]{64}$", hashes))) {
    .snapshot_fail()
  }
  if ("source_kind" %in% names(meta) &&
      !identical(as.character(meta$source_kind), sm_kind)) {
    .snapshot_fail()
  }

  sizes <- suppressWarnings(as.numeric(idx$size))
  n_files <- suppressWarnings(as.numeric(sm$n_files))
  if (length(sizes) != length(ids) || anyNA(sizes) ||
      any(!is.finite(sizes)) || any(sizes < 0) || any(sizes %% 1 != 0) ||
      length(n_files) != length(ids) || anyNA(n_files) ||
      any(!is.finite(n_files)) || any(n_files < 1) || any(n_files %% 1 != 0)) {
    .snapshot_fail()
  }
  if ("n_files" %in% names(meta) &&
      !identical(as.numeric(meta$n_files), n_files)) {
    .snapshot_fail()
  }

  index_uris <- scalar_column(idx$uri)
  relative <- vapply(index_uris, .snapshot_relative_object, character(1),
    root = image_root, backend_type = backend$type)
  if (anyDuplicated(index_uris) || anyDuplicated(relative)) .snapshot_fail()
  primary <- as.character(sm$primary_uri)
  files <- lapply(as.character(sm$files_json), .snapshot_parse_files)
  if (!identical(as.integer(n_files),
                 vapply(files, length, integer(1)))) {
    .snapshot_fail()
  }

  for (i in seq_along(ids)) {
    paths <- vapply(files[[i]], `[[`, character(1), "path")
    if (identical(sm_kind[[i]], "single_file")) {
      if (!.snapshot_scalar(primary[[i]]) || n_files[[i]] != 1 ||
          !identical(.snapshot_safe_relative_path(primary[[i]]), relative[[i]]) ||
          !identical(paths[[1L]], relative[[i]])) {
        .snapshot_fail()
      }
    } else {
      prefix <- paste0(relative[[i]], "/")
      if ((!is.na(primary[[i]]) && nzchar(primary[[i]])) ||
          any(!startsWith(paths, prefix))) {
        .snapshot_fail()
      }
    }
  }

  artifacts <- list(
    metadata = metadata[c("uri", "format", "sha256")],
    sample_manifests = sample_manifests[c("uri", "format", "sha256")],
    content_hash_index = content_index[c("uri", "format", "sha256")]
  )
  manifest_hash <- digest::digest(manifest, algo = "sha256", serialize = TRUE)
  records <- lapply(seq_along(ids), function(i) list(
    sample_id = ids[[i]], source_kind = sm_kind[[i]],
    uri = index_uris[[i]], relative_path = relative[[i]],
    content_hash = hashes[[i]], size = as.numeric(sizes[[i]]),
    n_files = as.integer(n_files[[i]]), files = files[[i]]))
  seal <- digest::digest(list(
    manifest = manifest_hash,
    artifacts = lapply(artifacts, `[[`, "sha256"),
    records = records
  ), algo = "sha256", serialize = TRUE)
  list(version = 1L, seal = seal, manifest_hash = manifest_hash,
       artifacts = artifacts, records = records)
}

.validate_imaging_collection_snapshot <- function(snapshot) {
  valid <- is.list(snapshot) && identical(as.integer(snapshot$version), 1L) &&
    .snapshot_scalar(snapshot$seal, max_bytes = 64L) &&
    grepl("^[0-9a-f]{64}$", snapshot$seal) &&
    is.list(snapshot$artifacts) && is.list(snapshot$records)
  if (!isTRUE(valid)) .snapshot_fail()
  snapshot
}

.copy_imaging_snapshot_artifact <- function(snapshot, backend, name, dest) {
  snapshot <- .validate_imaging_collection_snapshot(snapshot)
  artifact <- snapshot$artifacts[[name]]
  if (!is.list(artifact) || !.snapshot_scalar(artifact$uri) ||
      !.snapshot_scalar(artifact$sha256, max_bytes = 64L) ||
      !grepl("^[0-9a-f]{64}$", artifact$sha256)) {
    .snapshot_fail()
  }
  tmp <- tempfile(pattern = ".snapshot-", tmpdir = dirname(dest))
  on.exit(unlink(tmp, force = TRUE), add = TRUE)
  tryCatch(backend_get_file(backend, artifact$uri, tmp, overwrite = TRUE),
           error = function(e) .snapshot_fail())
  actual <- tryCatch(digest::digest(file = tmp, algo = "sha256"),
                     error = function(e) .snapshot_fail())
  if (!identical(actual, artifact$sha256)) .snapshot_fail()
  if (!file.rename(tmp, dest)) .snapshot_fail()
  tryCatch(Sys.chmod(dest, "0600", use_umask = FALSE),
           error = function(e) NULL)
  invisible(dest)
}

.materialize_imaging_snapshot <- function(snapshot, backend, target_root) {
  snapshot <- .validate_imaging_collection_snapshot(snapshot)
  if (dir.exists(target_root) || file.exists(target_root)) .snapshot_fail()
  dir.create(target_root, recursive = TRUE, showWarnings = FALSE, mode = "0700")
  on.exit(if (dir.exists(target_root)) unlink(target_root, recursive = TRUE,
                                               force = TRUE), add = TRUE)
  for (record in snapshot$records) {
    if (!identical(record$source_kind, "single_file")) {
      stop("Multi-file imaging samples are not supported by this consumer.",
           call. = FALSE)
    }
    relative <- .snapshot_safe_relative_path(record$relative_path)
    dest <- file.path(target_root, relative)
    dir.create(dirname(dest), recursive = TRUE, showWarnings = FALSE,
               mode = "0700")
    tmp <- tempfile(pattern = ".image-", tmpdir = dirname(dest))
    tryCatch(backend_get_file(backend, record$uri, tmp, overwrite = TRUE),
             error = function(e) .snapshot_fail())
    info <- file.info(tmp)
    actual_hash <- tryCatch(digest::digest(file = tmp, algo = "sha256"),
                            error = function(e) .snapshot_fail())
    if (!isTRUE(info$isdir == FALSE) || is.na(info$size) ||
        as.numeric(info$size) != as.numeric(record$size) ||
        !identical(actual_hash, record$content_hash) ||
        !file.rename(tmp, dest)) {
      unlink(tmp, force = TRUE)
      .snapshot_fail()
    }
    tryCatch(Sys.chmod(dest, "0600", use_umask = FALSE),
             error = function(e) NULL)
  }
  tryCatch(Sys.chmod(target_root, "0700", use_umask = FALSE),
           error = function(e) NULL)
  on.exit(NULL, add = FALSE)
  list(root = normalizePath(target_root, winslash = "/", mustWork = TRUE),
       relative_paths = vapply(snapshot$records, `[[`, character(1),
                               "relative_path"))
}
