# Module: Dataset Privacy Contract
#
# Imaging rows are not assumed to be independent people. Every dataset used
# through DataSHIELD declares the metadata column that identifies its patient
# privacy unit, and admission is based on distinct values of that column.

.IMAGING_PRIVACY_UNIT <- "patient"
.IMAGING_PRIVACY_CANONICALIZATION <- "trim-utf8-v2"

#' @keywords internal
.new_imaging_handle_capability <- function() {
  token <- gsub("-", "", paste0(
    uuid::UUIDgenerate(use.time = FALSE),
    uuid::UUIDgenerate(use.time = FALSE)
  ), fixed = TRUE)
  if (!grepl("^[0-9a-f]{64}$", token)) {
    stop("Could not create an imaging handle capability.", call. = FALSE)
  }
  paste0("imgh_", token)
}

#' @keywords internal
.is_imaging_handle_reference <- function(object) {
  inherits(object, "dsimaging_handle_ref") &&
    is.list(object) && identical(names(object), "capability") &&
    is.character(object$capability) && length(object$capability) == 1L &&
    !is.na(object$capability) &&
    grepl("^imgh_[0-9a-f]{64}$", object$capability)
}

#' Fail closed when a legacy/unregistered DataSHIELD surface is allowlisted
#' by an older Opal configuration.
#' @keywords internal
.legacy_imaging_ds_disabled <- function(method) {
  stop(method, " is not available through DataSHIELD. Use a registered ",
       "handle-bound dsImaging domain method.", call. = FALSE)
}

#' @keywords internal
.register_imaging_handle <- function(handle, owner_env) {
  if (!is.list(handle) || !is.environment(owner_env)) {
    stop("Invalid imaging handle registration.", call. = FALSE)
  }
  capability <- .new_imaging_handle_capability()
  .imaging_handle_registry[[capability]] <- list(
    handle = handle, owner_env = owner_env)
  structure(list(capability = capability), class = "dsimaging_handle_ref")
}

#' @keywords internal
.new_imaging_workflow_capability <- function() {
  token <- gsub("-", "", paste0(
    uuid::UUIDgenerate(use.time = FALSE),
    uuid::UUIDgenerate(use.time = FALSE)
  ), fixed = TRUE)
  if (!grepl("^[0-9a-f]{64}$", token)) {
    stop("Could not create an imaging workflow capability.", call. = FALSE)
  }
  paste0("imgw_", token)
}

#' @keywords internal
.is_imaging_workflow_reference <- function(object) {
  inherits(object, "dsimaging_workflow_ref") &&
    is.list(object) && identical(names(object), "capability") &&
    is.character(object$capability) && length(object$capability) == 1L &&
    !is.na(object$capability) &&
    grepl("^imgw_[0-9a-f]{64}$", object$capability)
}

#' @keywords internal
.register_imaging_workflow <- function(workflow, owner_env) {
  if (!is.list(workflow) || !is.environment(owner_env)) {
    stop("Invalid imaging workflow registration.", call. = FALSE)
  }
  capability <- .new_imaging_workflow_capability()
  .imaging_workflow_registry[[capability]] <- list(
    workflow = workflow, owner_env = owner_env)
  structure(list(capability = capability), class = "dsimaging_workflow_ref")
}

#' @keywords internal
.resolve_imaging_workflow_context <- function(symbol, owner_env = NULL) {
  if (!is.character(symbol) || length(symbol) != 1L || is.na(symbol) ||
      !nzchar(symbol)) {
    stop("A valid imaging workflow reference is required.", call. = FALSE)
  }
  if (!is.null(owner_env) && !is.environment(owner_env)) {
    stop("Unknown or unavailable imaging workflow reference.", call. = FALSE)
  }
  frames <- if (is.null(owner_env)) rev(sys.frames()) else list(owner_env)
  found <- NULL
  for (env in frames) {
    if (!exists(symbol, envir = env, inherits = FALSE)) next
    object <- get(symbol, envir = env, inherits = FALSE)
    valid <- inherits(object, "dsimaging_workflow_ref") &&
      is.list(object) && identical(names(object), "capability") &&
      is.character(object$capability) && length(object$capability) == 1L &&
      grepl("^imgw_[0-9a-f]{64}$", object$capability)
    if (isTRUE(valid)) {
      found <- list(reference = object, owner_env = env)
      break
    }
  }
  if (is.null(found)) {
    stop("Unknown or unavailable imaging workflow reference.", call. = FALSE)
  }
  entry <- .imaging_workflow_registry[[found$reference$capability]]
  if (is.null(entry) || !is.list(entry$workflow) ||
      !identical(entry$owner_env, found$owner_env)) {
    stop("Unknown, stale, or cross-session imaging workflow reference.",
         call. = FALSE)
  }
  list(workflow = entry$workflow, owner_env = found$owner_env,
       capability = found$reference$capability)
}

#' @keywords internal
.resolve_imaging_workflow <- function(symbol, owner_env = NULL) {
  .resolve_imaging_workflow_context(symbol, owner_env = owner_env)$workflow
}

#' @keywords internal
.update_imaging_workflow <- function(symbol, workflow, owner_env = NULL) {
  if (!is.null(owner_env) && !is.environment(owner_env)) {
    stop("Unknown or unavailable imaging workflow reference.", call. = FALSE)
  }
  frames <- if (is.null(owner_env)) rev(sys.frames()) else list(owner_env)
  for (env in frames) {
    if (!exists(symbol, envir = env, inherits = FALSE)) next
    object <- get(symbol, envir = env, inherits = FALSE)
    if (!inherits(object, "dsimaging_workflow_ref")) next
    entry <- .imaging_workflow_registry[[object$capability]]
    if (is.null(entry) || !identical(entry$owner_env, env)) break
    entry$workflow <- workflow
    .imaging_workflow_registry[[object$capability]] <- entry
    return(invisible(TRUE))
  }
  stop("Unknown or unavailable imaging workflow reference.", call. = FALSE)
}

#' @keywords internal
.imaging_worker_context_dir <- function() {
  getOption(
    "dsimaging.worker_context_dir",
    getOption("default.dsimaging.worker_context_dir",
      file.path(getOption("dshpc.home",
        getOption("default.dshpc.home", "/srv/dshpc")),
        "staging", "dsimaging-contexts")))
}

#' @keywords internal
.imaging_worker_context_file <- function(context_id, manifest = FALSE) {
  if (!is.character(context_id) || length(context_id) != 1L ||
      is.na(context_id) || !grepl("^dsctx_[0-9a-f]{64}$", context_id)) {
    stop("Invalid imaging worker context.", call. = FALSE)
  }
  suffix <- if (isTRUE(manifest)) ".manifest.yaml" else ".context.yaml"
  file.path(.imaging_worker_context_dir(), paste0(context_id, suffix))
}

#' Build a worker manifest containing only request-authorized asset aliases
#' @keywords internal
.imaging_worker_manifest <- function(authorized, asset_names = character(0)) {
  manifest <- authorized$manifest
  if (!is.list(manifest) || is.null(manifest$dataset_id)) {
    stop("Invalid authorized imaging manifest.", call. = FALSE)
  }
  asset_names <- unique(as.character(unlist(asset_names, use.names = FALSE)))
  asset_names <- asset_names[!is.na(asset_names) & nzchar(asset_names)]
  source_assets <- manifest$assets %||% list()
  manifest$assets <- list()
  for (asset_name in asset_names) {
    asset_name <- .imaging_safe_name(asset_name, "asset name")
    if (!is.null(source_assets[[asset_name]])) {
      manifest$assets[[asset_name]] <- source_assets[[asset_name]]
      next
    }
    db <- .asset_db_connect()
    asset <- tryCatch(
      .resolve_asset_by_id_or_alias(
        db, authorized$dataset_id, asset_name, authorized = authorized),
      error = function(e) NULL,
      finally = .asset_db_close(db))
    if (is.null(asset) || is.null(asset$path_or_root) ||
        is.na(asset$path_or_root) || !nzchar(asset$path_or_root)) {
      stop("Requested imaging asset is unavailable.", call. = FALSE)
    }
    manifest$assets[[asset_name]] <- list(
      type = asset$kind %||% "derived",
      kind = asset$kind %||% "derived",
      uri = asset$path_or_root)
  }
  manifest
}

#' @keywords internal
.write_private_worker_yaml <- function(object, target) {
  tmp <- tempfile(pattern = ".worker-context-", tmpdir = dirname(target),
                  fileext = ".yaml")
  on.exit(unlink(tmp, force = TRUE))
  yaml::write_yaml(object, tmp)
  tryCatch(Sys.chmod(tmp, "0600", use_umask = FALSE),
           error = function(e) NULL)
  if (!file.rename(tmp, target)) {
    stop("Could not atomically create imaging worker context.",
         call. = FALSE)
  }
  invisible(target)
}

#' Project the admitted image routes into a worker-only exact sample map
#' @keywords internal
.imaging_worker_collection_map <- function(authorized, worker_manifest) {
  snapshot <- authorized$collection_snapshot %||%
    (authorized$handle %||% list())$collection_snapshot
  if (is.null(snapshot)) return(NULL)
  snapshot <- .validate_imaging_collection_snapshot(snapshot)

  source_uri <- ((authorized$manifest %||% list())$assets %||%
    list())$images$uri %||% NULL
  worker_assets <- worker_manifest$assets %||% list()
  mapped_assets <- names(worker_assets)[vapply(worker_assets, function(asset) {
    is.list(asset) && is.character(asset$uri) && length(asset$uri) == 1L &&
      !is.na(asset$uri) && identical(asset$uri, source_uri)
  }, logical(1))]
  if (length(mapped_assets) == 0L) return(NULL)

  records <- lapply(snapshot$records, function(record) list(
    sample_id = record$sample_id,
    source_kind = record$source_kind,
    uri = record$uri,
    relative_path = record$relative_path,
    content_hash = record$content_hash,
    size = record$size,
    n_files = record$n_files
  ))
  list(version = 1L, seal = snapshot$seal,
       asset_names = as.list(unname(mapped_assets)), records = records)
}

#' Register an opaque worker-only dataset context
#'
#' The original dataset id is never promoted from an analyst's Opal resource
#' into the shared registry. A high-entropy context name points to a read-only
#' snapshot of the admitted manifest and is embedded only in the server-side
#' job specification.
#' @keywords internal
.register_imaging_worker_context <- function(authorized,
                                             asset_names = character(0)) {
  roster <- .validate_imaging_privacy_roster(
    authorized$privacy_roster %||% authorized$handle$privacy_roster)
  backend <- authorized$backend %||% storage_backend("file")
  portable_backend <- .portable_backend_context(backend)
  registry_backend <- storage_backend(
    portable_backend$type, portable_backend$config %||% list())
  token <- gsub("-", "", paste0(
    uuid::UUIDgenerate(use.time = FALSE),
    uuid::UUIDgenerate(use.time = FALSE)
  ), fixed = TRUE)
  context_id <- paste0("dsctx_", token)
  if (!grepl("^dsctx_[0-9a-f]{64}$", context_id)) {
    stop("Could not create an imaging worker context.", call. = FALSE)
  }

  root <- .imaging_worker_context_dir()
  dir.create(root, recursive = TRUE, showWarnings = FALSE, mode = "0700")
  tryCatch(Sys.chmod(root, "0700", use_umask = FALSE),
           error = function(e) NULL)
  if (!dir.exists(root) || file.access(root, 2L) != 0L) {
    stop("Imaging worker context storage is unavailable.", call. = FALSE)
  }

  manifest_path <- .imaging_worker_context_file(context_id, manifest = TRUE)
  context_path <- .imaging_worker_context_file(context_id)
  committed <- FALSE
  on.exit({
    if (!committed) unlink(c(manifest_path, context_path), force = TRUE)
  }, add = TRUE)
  context_manifest <- .imaging_worker_manifest(authorized, asset_names)
  context_manifest$.dsimaging_privacy_roster <- roster
  context_manifest$.dsimaging_collection_seal <-
    .imaging_authorized_collection_seal(authorized)
  worker_context <- list(
    schema_version = 1L,
    context_id = context_id,
    manifest = context_manifest,
    backend = portable_backend)
  collection_map <- .imaging_worker_collection_map(
    authorized, context_manifest)
  if (!is.null(collection_map)) {
    worker_context$collection_map <- collection_map
  }
  .write_private_worker_yaml(context_manifest, manifest_path)
  .write_private_worker_yaml(worker_context, context_path)

  registered <- register_dataset(
    context_id, manifest_path, registry_backend)
  if (!isTRUE(registered)) {
    stop("Imaging worker context registration is unavailable.",
         call. = FALSE)
  }
  committed <- TRUE
  context_id
}

#' Remove one terminal workflow's opaque worker context
#'
#' Only protocol-generated context identifiers and their two exact files are
#' eligible for removal. The registry update is committed before files are
#' deleted so a concurrent worker can never resolve a half-removed entry.
#' @keywords internal
.unregister_imaging_worker_context <- function(context_id) {
  manifest_path <- .imaging_worker_context_file(context_id, manifest = TRUE)
  context_path <- .imaging_worker_context_file(context_id)
  registry_path <- getOption("dsimaging.registry_path",
    getOption("default.dsimaging.registry_path",
              "/var/lib/dsimaging/registry.yaml"))
  registry_backend <- getOption("dsimaging.registry_backend",
    getOption("default.dsimaging.registry_backend", "file"))
  if (!identical(registry_backend, "file")) {
    stop("Worker context cleanup is unavailable.", call. = FALSE)
  }

  dir.create(dirname(registry_path), recursive = TRUE, showWarnings = FALSE,
             mode = "0770")
  lock_path <- paste0(registry_path, ".lock")
  deadline <- Sys.time() + 5
  repeat {
    if (isTRUE(dir.create(lock_path, showWarnings = FALSE, mode = "0700"))) {
      break
    }
    if (Sys.time() >= deadline) {
      stop("Worker context cleanup is unavailable.", call. = FALSE)
    }
    Sys.sleep(0.02)
  }
  on.exit(unlink(lock_path, recursive = TRUE, force = TRUE), add = TRUE)

  registry <- if (file.exists(registry_path)) .load_registry() else list()
  entry <- registry[[context_id]]
  if (!is.null(entry)) {
    registered_manifest <- entry$manifest_uri %||% entry$manifest %||% NULL
    if (!is.character(registered_manifest) ||
        length(registered_manifest) != 1L || is.na(registered_manifest) ||
        !identical(path.expand(registered_manifest),
                   path.expand(manifest_path))) {
      stop("Worker context cleanup is unavailable.", call. = FALSE)
    }
    registry[[context_id]] <- NULL
    raw <- c(list(schema_version = 1L), registry)
    tmp <- tempfile(pattern = ".registry-", tmpdir = dirname(registry_path),
                    fileext = ".yaml")
    on.exit(unlink(tmp, force = TRUE), add = TRUE)
    yaml::write_yaml(raw, tmp)
    tryCatch(Sys.chmod(tmp, "0660", use_umask = FALSE),
             error = function(e) NULL)
    if (!file.rename(tmp, registry_path)) {
      stop("Worker context cleanup is unavailable.", call. = FALSE)
    }
  }
  unlink(c(manifest_path, context_path), force = TRUE)
  if (file.exists(manifest_path) || file.exists(context_path)) {
    stop("Worker context cleanup is unavailable.", call. = FALSE)
  }
  invisible(TRUE)
}

#' @keywords internal
.resolve_imaging_handle <- function(symbol, owner_env = NULL) {
  if (!is.character(symbol) || length(symbol) != 1L || is.na(symbol) ||
      !nzchar(symbol)) {
    stop("No imaging handle for the requested symbol.", call. = FALSE)
  }
  if (!is.null(owner_env) && !is.environment(owner_env)) {
    stop("No imaging handle for the requested symbol.", call. = FALSE)
  }
  frames <- if (is.null(owner_env)) rev(sys.frames()) else list(owner_env)
  found <- NULL
  for (env in frames) {
    if (!exists(symbol, envir = env, inherits = FALSE)) next
    object <- get(symbol, envir = env, inherits = FALSE)
    if (.is_imaging_handle_reference(object)) {
      found <- list(reference = object, owner_env = env)
      break
    }
  }
  if (is.null(found)) {
    stop("No imaging handle for symbol '", symbol,
         "'. Call imagingInitDS first.", call. = FALSE)
  }
  entry <- .imaging_handle_registry[[found$reference$capability]]
  if (is.null(entry) || !is.list(entry$handle) ||
      !identical(entry$owner_env, found$owner_env)) {
    stop("No imaging handle for symbol '", symbol,
         "' (unknown, stale, or cross-session reference).", call. = FALSE)
  }
  list(reference = found$reference, owner_env = found$owner_env,
       handle = entry$handle)
}

#' Resolve a handle for another trusted server package in the same session.
#' This function is deliberately not a registered DataSHIELD method.
#' @keywords internal
.resolve_imaging_handle_for_consumer <- function(symbol,
                                                 expected_capability = NULL,
                                                 owner_env = NULL) {
  # Trusted consumers must cross the same freshness boundary as dsImaging's
  # own public methods: active publish locks and any change to the admitted
  # sample-to-patient roster fail closed. Consumers should call this again
  # after staging to close their own read-time race window.
  authorized <- .authorized_imaging_dataset(symbol, owner_env = owner_env)
  if (!is.null(expected_capability)) {
    valid_capability <- is.character(expected_capability) &&
      length(expected_capability) == 1L && !is.na(expected_capability) &&
      grepl("^imgh_[0-9a-f]{64}$", expected_capability)
    if (!isTRUE(valid_capability) ||
        !identical(expected_capability, authorized$handle_capability)) {
      stop("Imaging handle capability changed during consumption.",
           call. = FALSE)
    }
  }
  authorized$handle
}

#' @keywords internal
.imaging_privacy_contract <- function(manifest) {
  meta <- manifest$metadata
  scalar <- function(value) {
    is.character(value) && length(value) == 1L && !is.na(value) &&
      nzchar(trimws(value))
  }

  if (!is.list(meta) || !scalar(meta$id_col) ||
      !identical(meta$privacy_unit, .IMAGING_PRIVACY_UNIT) ||
      !scalar(meta$privacy_unit_col) ||
      !identical(meta$privacy_unit_canonicalization,
                 .IMAGING_PRIVACY_CANONICALIZATION)) {
    stop(
      "Imaging dataset is missing the required patient privacy contract ",
      "(metadata.id_col, metadata.privacy_unit='patient', ",
      "metadata.privacy_unit_col, metadata.privacy_unit_canonicalization=",
      "'trim-utf8-v2').",
      call. = FALSE
    )
  }

  label_col <- meta$label_col %||% NULL
  if (!is.null(label_col) && !scalar(label_col)) {
    stop("metadata.label_col must be one non-empty column name.",
         call. = FALSE)
  }
  label_levels <- meta$label_levels %||% NULL
  if (!is.null(label_levels)) {
    label_levels <- as.character(unlist(label_levels, use.names = FALSE))
    safe_levels <- .safe_public_identifiers(label_levels)
    if (is.null(label_col) || length(label_levels) == 0L ||
        anyNA(label_levels) || anyDuplicated(label_levels) ||
        length(safe_levels) != length(label_levels) ||
        !setequal(safe_levels, label_levels)) {
      stop("metadata.label_levels must be a safe, unique public vocabulary.",
           call. = FALSE)
    }
  }

  id_col <- trimws(meta$id_col)
  privacy_unit_col <- trimws(meta$privacy_unit_col)
  label_col <- if (is.null(label_col)) NULL else trimws(label_col)
  if (identical(id_col, privacy_unit_col) ||
      tolower(privacy_unit_col) %in% c("source_kind", "n_files") ||
      (!is.null(label_col) &&
       label_col %in% c(id_col, privacy_unit_col))) {
    stop(paste(
      "Imaging privacy, sample, and label roles must use distinct,",
      "non-structural columns."),
         call. = FALSE)
  }

  list(
    id_col = id_col,
    privacy_unit = .IMAGING_PRIVACY_UNIT,
    privacy_unit_col = privacy_unit_col,
    privacy_unit_canonicalization = .IMAGING_PRIVACY_CANONICALIZATION,
    label_col = label_col,
    label_levels = label_levels
  )
}

#' Canonicalise patient identifiers with the shared dsImaging/dsFlower v2
#' contract: strict UTF-8 text with ASCII space, tab, CR and LF trimmed.
#' @keywords internal
.canonical_imaging_privacy_ids <- function(values) {
  values <- tryCatch(iconv(as.character(values), from = "", to = "UTF-8",
                           sub = NA_character_),
                     error = function(e) rep(NA_character_, length(values)))
  values <- sub("^[ \\t\\r\\n]+", "", values, perl = TRUE)
  values <- sub("[ \\t\\r\\n]+$", "", values, perl = TRUE)
  values[nchar(values, type = "bytes", allowNA = TRUE) > 4096L] <- NA_character_
  values
}

#' Build the immutable patient/sample roster admitted for a dataset.
#'
#' The roster is deliberately kept server-side. Sorting by canonical sample id
#' makes its hash independent of metadata row order while retaining the exact
#' sample-to-patient multiplicity.
#' @keywords internal
.new_imaging_privacy_roster <- function(sample_ids, privacy_ids) {
  sample_ids <- as.character(unlist(sample_ids, use.names = FALSE))
  privacy_ids <- as.character(unlist(privacy_ids, use.names = FALSE))
  if (length(sample_ids) == 0L || length(sample_ids) != length(privacy_ids) ||
      anyNA(sample_ids) || anyNA(privacy_ids) || any(!nzchar(sample_ids)) ||
      any(!nzchar(privacy_ids)) || anyDuplicated(sample_ids) ||
      !identical(sample_ids, .canonical_imaging_privacy_ids(sample_ids)) ||
      !identical(privacy_ids, .canonical_imaging_privacy_ids(privacy_ids))) {
    stop("Imaging privacy roster is invalid.", call. = FALSE)
  }

  ord <- order(sample_ids, method = "radix")
  sample_ids <- unname(sample_ids[ord])
  privacy_ids <- unname(privacy_ids[ord])
  privacy_units <- sort(unique(privacy_ids), method = "radix")
  multiplicity <- vapply(privacy_units, function(id) {
    sum(privacy_ids == id)
  }, integer(1))
  payload <- jsonlite::toJSON(list(
    sample_ids = sample_ids,
    privacy_ids = privacy_ids
  ), auto_unbox = FALSE, null = "null", na = "null")

  list(
    version = 1L,
    sample_ids = sample_ids,
    privacy_ids = privacy_ids,
    privacy_units = unname(privacy_units),
    unit_multiplicity = unname(as.integer(multiplicity)),
    sample_count = as.integer(length(sample_ids)),
    privacy_unit_count = as.integer(length(privacy_units)),
    roster_hash = digest::digest(payload, algo = "sha256", serialize = FALSE)
  )
}

#' @keywords internal
.validate_imaging_privacy_roster <- function(roster) {
  if (!is.list(roster) || !identical(as.integer(roster$version), 1L) ||
      !is.character(roster$roster_hash) || length(roster$roster_hash) != 1L ||
      is.na(roster$roster_hash) ||
      !grepl("^[0-9a-f]{64}$", roster$roster_hash)) {
    stop("Imaging privacy roster is unavailable.", call. = FALSE)
  }
  rebuilt <- .new_imaging_privacy_roster(
    roster$sample_ids, roster$privacy_ids)
  supplied_units <- as.character(unlist(
    roster$privacy_units, use.names = FALSE))
  supplied_multiplicity <- as.integer(unlist(
    roster$unit_multiplicity, use.names = FALSE))
  if (!identical(rebuilt$roster_hash, roster$roster_hash) ||
      !identical(rebuilt$privacy_units, supplied_units) ||
      !identical(rebuilt$unit_multiplicity, supplied_multiplicity) ||
      !identical(rebuilt$sample_count, as.integer(roster$sample_count)) ||
      !identical(rebuilt$privacy_unit_count,
                 as.integer(roster$privacy_unit_count))) {
    stop("Imaging privacy roster is unavailable.", call. = FALSE)
  }
  rebuilt
}

#' @keywords internal
.same_imaging_privacy_roster <- function(left, right) {
  left <- tryCatch(.validate_imaging_privacy_roster(left),
                   error = function(e) NULL)
  right <- tryCatch(.validate_imaging_privacy_roster(right),
                    error = function(e) NULL)
  !is.null(left) && !is.null(right) &&
    identical(left$roster_hash, right$roster_hash) &&
    identical(left$sample_ids, right$sample_ids) &&
    identical(left$privacy_ids, right$privacy_ids)
}

#' Require one and only one row for every admitted sample.
#' @keywords internal
.assert_exact_imaging_roster <- function(sample_ids, roster,
                                         privacy_ids = NULL,
                                         context = "imaging data") {
  roster <- .validate_imaging_privacy_roster(roster)
  sample_ids <- .canonical_imaging_privacy_ids(sample_ids)
  valid <- length(sample_ids) == roster$sample_count &&
    !anyNA(sample_ids) && all(nzchar(sample_ids)) &&
    !anyDuplicated(sample_ids) &&
    identical(sort(unname(sample_ids), method = "radix"),
              roster$sample_ids)
  if (isTRUE(valid) && !is.null(privacy_ids)) {
    privacy_ids <- .canonical_imaging_privacy_ids(privacy_ids)
    candidate <- tryCatch(
      .new_imaging_privacy_roster(sample_ids, privacy_ids),
      error = function(e) NULL)
    valid <- !is.null(candidate) &&
      .same_imaging_privacy_roster(candidate, roster)
  }
  if (!isTRUE(valid)) {
    stop(context, " is not the complete admitted collection.",
         call. = FALSE)
  }
  invisible(TRUE)
}

#' @keywords internal
.read_imaging_metadata_table <- function(manifest, backend = NULL) {
  meta <- manifest$metadata
  if (!is.list(meta)) {
    stop("Imaging metadata is unavailable.", call. = FALSE)
  }

  path <- meta$file %||% NULL
  cleanup <- FALSE
  if (is.null(path) || !file.exists(path)) {
    uri <- meta$uri %||% NULL
    if (!is.null(uri) && grepl("^s3://", uri) && !is.null(backend)) {
      fmt <- meta$format %||% .guess_format(uri)
      path <- tempfile(fileext = paste0(".", fmt))
      tryCatch(
        backend_get_file(backend, uri, path),
        error = function(e) stop("Imaging metadata is unavailable.",
                                 call. = FALSE))
      cleanup <- TRUE
    } else if (!is.null(uri) && file.exists(uri)) {
      path <- uri
    }
  }
  if (cleanup) on.exit(unlink(path), add = TRUE)
  if (is.null(path) || !file.exists(path)) {
    stop("Imaging metadata is unavailable.", call. = FALSE)
  }

  fmt <- meta$format %||% .guess_format(path)
  # Reader diagnostics commonly include the absolute source path. This helper
  # runs on analyst-facing handle resolution paths, so keep those details in
  # node logs and expose one constant failure instead.
  data <- tryCatch(suppressWarnings({
    if (identical(fmt, "parquet")) {
      if (!requireNamespace("arrow", quietly = TRUE)) {
        stop("Imaging metadata is unavailable.", call. = FALSE)
      }
      as.data.frame(arrow::read_parquet(path))
    } else if (identical(fmt, "csv")) {
      utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
    } else {
      stop("Imaging metadata format is unsupported.", call. = FALSE)
    }
  }), error = function(e) {
    stop("Imaging metadata is unavailable.", call. = FALSE)
  })
  if (!is.data.frame(data)) {
    stop("Imaging metadata is invalid.", call. = FALSE)
  }
  data
}

#' @keywords internal
.validate_imaging_privacy_metadata <- function(data, contract,
                                                require_label = FALSE) {
  required <- c(contract$id_col, contract$privacy_unit_col)
  if (isTRUE(require_label)) required <- c(required, contract$label_col)
  if (anyNA(required) || any(!nzchar(required)) ||
      any(!required %in% names(data))) {
    stop("Imaging metadata does not satisfy its privacy contract.",
         call. = FALSE)
  }

  raw_sample_ids <- tryCatch(as.character(data[[contract$id_col]]),
                             error = function(e) character(0))
  sample_ids <- .canonical_imaging_privacy_ids(raw_sample_ids)
  patient_ids <- .canonical_imaging_privacy_ids(
    data[[contract$privacy_unit_col]])
  missing_token <- function(values) {
    tolower(trimws(as.character(values))) %in%
      c("na", "nan", "null", "<na>", "nat")
  }
  invalid_sample <- is.na(sample_ids) | !nzchar(sample_ids) |
    missing_token(sample_ids)
  invalid_patient <- is.na(patient_ids) | !nzchar(patient_ids) |
    missing_token(patient_ids)
  if (any(invalid_sample) || any(invalid_patient) || anyDuplicated(sample_ids) ||
      !identical(raw_sample_ids, sample_ids)) {
    stop("Imaging metadata does not satisfy its privacy contract.",
         call. = FALSE)
  }

  if (isTRUE(require_label)) {
    labels <- data[[contract$label_col]]
    invalid_label <- is.na(labels) | !nzchar(trimws(as.character(labels))) |
      missing_token(labels)
    if (any(invalid_label)) {
      stop("Imaging metadata does not satisfy its privacy contract.",
           call. = FALSE)
    }
  }

  list(sample_ids = sample_ids, privacy_ids = patient_ids)
}

#' @keywords internal
.imaging_privacy_admission <- function(manifest, backend = NULL) {
  contract <- .imaging_privacy_contract(manifest)
  data <- .read_imaging_metadata_table(manifest, backend)
  ids <- .validate_imaging_privacy_metadata(
    data, contract, require_label = !is.null(contract$label_col))
  approved_levels <- contract$label_levels %||% character(0)
  if (length(approved_levels) > 0L) {
    labels <- as.character(data[[contract$label_col]])
    private_identifiers <- unique(c(ids$sample_ids, ids$privacy_ids))
    if (!all(labels %in% approved_levels) ||
        any(approved_levels %in% private_identifiers)) {
      stop("Imaging metadata does not satisfy its privacy contract.",
           call. = FALSE)
    }
  }
  roster <- .new_imaging_privacy_roster(ids$sample_ids, ids$privacy_ids)
  pinned <- manifest$.dsimaging_privacy_roster %||% NULL
  if (!is.null(pinned) && !.same_imaging_privacy_roster(pinned, roster)) {
    stop("Imaging dataset no longer matches its admitted privacy roster.",
         call. = FALSE)
  }
  n_units <- roster$privacy_unit_count
  .assert_min_privacy_units(
    n_units, context = paste0("dataset '", manifest$dataset_id, "'"))
  list(contract = contract, n_privacy_units = as.integer(n_units), data = data,
       sample_ids = roster$sample_ids, privacy_ids = roster$privacy_ids,
       roster = roster)
}

#' @keywords internal
.assert_min_privacy_units <- function(n_units, context = "dataset") {
  threshold <- .nfilter_threshold()
  if (length(n_units) != 1L || is.na(n_units)) {
    stop("Disclosive: ", context,
         " privacy-unit count is unknown. Operation blocked.", call. = FALSE)
  }
  if (n_units < threshold) {
    stop("Disclosive: ", context, " has fewer than ", threshold,
         " privacy units. Operation blocked.", call. = FALSE)
  }
  invisible(TRUE)
}

#' @keywords internal
.assert_dataset_not_publishing <- function(handle) {
  manifest_uri <- handle$manifest_uri %||% NULL
  backend <- handle$backend %||% NULL
  if (is.null(manifest_uri) || is.null(backend)) return(invisible(TRUE))

  if (grepl("^s3://", manifest_uri)) {
    lock_uri <- paste0(sub("/manifest\\.ya?ml$", "", manifest_uri),
                       "/.publish-lock")
    locked <- tryCatch({
      info <- backend_head(backend, lock_uri)
      !is.null(info) && isTRUE(info$exists)
    }, error = function(e) TRUE)
  } else {
    locked <- file.exists(file.path(dirname(manifest_uri), ".publish-lock"))
  }
  if (isTRUE(locked)) {
    stop("Imaging dataset is being updated. Operation blocked.",
         call. = FALSE)
  }
  invisible(TRUE)
}

#' Resolve a session-bound handle and optionally assert its dataset id.
#' @keywords internal
.authorized_imaging_dataset <- function(handle_symbol, dataset_id = NULL,
                                        owner_env = NULL) {
  handle_symbol <- as.character(handle_symbol)
  if (length(handle_symbol) != 1L || is.na(handle_symbol) ||
      !nzchar(handle_symbol)) {
    stop("A valid imaging handle is required.", call. = FALSE)
  }
  resolved_handle <- .resolve_imaging_handle(handle_symbol, owner_env)
  handle <- resolved_handle$handle
  .assert_dataset_not_publishing(handle)
  actual <- handle$dataset_id %||% handle$manifest$dataset_id
  if (!is.null(dataset_id) && !identical(as.character(dataset_id), actual)) {
    stop("Imaging handle does not authorize the requested dataset.",
         call. = FALSE)
  }
  roster <- tryCatch(.validate_imaging_privacy_roster(
    handle$privacy_roster), error = function(e) NULL)
  if (is.null(roster)) {
    stop("Imaging handle has no admitted privacy roster.", call. = FALSE)
  }
  pinned_snapshot <- handle$collection_snapshot %||% NULL
  current_manifest <- handle$manifest
  if (!is.null(pinned_snapshot)) {
    pinned_snapshot <- .validate_imaging_collection_snapshot(pinned_snapshot)
    current_manifest <- tryCatch(
      suppressWarnings(parse_manifest(handle$manifest_uri, handle$backend)),
      error = function(e) stop(
        "Imaging dataset no longer matches its admitted collection snapshot.",
        call. = FALSE))
    .assert_imaging_resource_dataset(current_manifest, actual)
    .assert_imaging_manifest_scope(
      current_manifest, handle$manifest_uri, handle$backend)
  }
  current_manifest$.dsimaging_privacy_roster <- roster
  current <- .imaging_privacy_admission(current_manifest, handle$backend)
  if (!.same_imaging_privacy_roster(roster, current$roster)) {
    stop("Imaging dataset no longer matches its admitted privacy roster.",
         call. = FALSE)
  }
  if (!is.null(pinned_snapshot)) {
    current_manifest$.dsimaging_privacy_roster <- NULL
    current_snapshot <- .new_imaging_collection_snapshot(
      current_manifest, handle$backend, current)
    .assert_dataset_not_publishing(handle)
    if (!identical(current_snapshot$seal, pinned_snapshot$seal)) {
      stop("Imaging dataset no longer matches its admitted collection snapshot.",
           call. = FALSE)
    }
    handle$manifest <- current_manifest
    handle$descriptor <- imaging_dataset_descriptor(current_manifest)
    handle$collection_snapshot <- current_snapshot
  }
  list(handle = handle, dataset_id = actual, manifest = handle$manifest,
       backend = handle$backend, privacy = handle$privacy,
       n_privacy_units = roster$privacy_unit_count,
       privacy_roster = roster, collection_seal = handle$collection_seal,
       owner_env = resolved_handle$owner_env,
       handle_capability = resolved_handle$reference$capability)
}

#' Record that a session-bound workflow capability exposed a private asset.
#' @keywords internal
.authorize_private_imaging_asset <- function(asset_id, owner_env) {
  if (!is.character(asset_id) || length(asset_id) != 1L || is.na(asset_id) ||
      !grepl("^asset_[0-9a-f]{32}$", asset_id) ||
      !is.environment(owner_env)) {
    stop("Invalid private imaging asset authorization.", call. = FALSE)
  }
  environments <- .imaging_private_asset_authorizations[[asset_id]] %||%
    list()
  if (!any(vapply(environments, identical, logical(1), y = owner_env))) {
    environments[[length(environments) + 1L]] <- owner_env
  }
  .imaging_private_asset_authorizations[[asset_id]] <- environments
  invisible(TRUE)
}

#' @keywords internal
.private_imaging_asset_is_authorized <- function(asset_id, owner_env) {
  if (!is.environment(owner_env)) return(FALSE)
  environments <- .imaging_private_asset_authorizations[[asset_id]] %||%
    list()
  any(vapply(environments, identical, logical(1), y = owner_env))
}
