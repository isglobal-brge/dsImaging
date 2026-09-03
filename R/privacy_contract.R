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
  state <- .imaging_session_state(owner_env, create = TRUE)
  handles <- state$handles
  handles[[capability]] <- list(handle = handle)
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
  state <- .imaging_session_state(owner_env, create = TRUE)
  workflows <- state$workflows
  workflows[[capability]] <- list(workflow = workflow)
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
  state <- .imaging_session_state(found$owner_env, create = FALSE)
  entry <- if (is.null(state)) NULL else
    state$workflows[[found$reference$capability]]
  if (is.null(entry) || !is.list(entry$workflow)) {
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
    state <- .imaging_session_state(env, create = FALSE)
    entry <- if (is.null(state)) NULL else state$workflows[[object$capability]]
    if (!is.list(entry) || !is.list(entry$workflow)) break
    entry$workflow <- workflow
    workflows <- state$workflows
    workflows[[object$capability]] <- entry
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
      source_asset <- source_assets[[asset_name]]
      source_asset$.dsimaging_asset_identity <- NULL
      manifest$assets[[asset_name]] <- source_asset
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
    derivation_hash <- asset$derivation_hash %||% ""
    if (is.na(derivation_hash)) derivation_hash <- ""
    manifest$assets[[asset_name]] <- list(
      type = asset$kind %||% "derived",
      kind = asset$kind %||% "derived",
      uri = asset$path_or_root,
      .dsimaging_asset_identity = list(
        asset_id = asset$asset_id,
        derivation_hash = as.character(derivation_hash)))
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

#' Project admitted source routes into a worker-only exact sample map
#' @keywords internal
.imaging_worker_collection_map <- function(authorized, worker_manifest) {
  snapshot <- authorized$collection_snapshot %||%
    (authorized$handle %||% list())$collection_snapshot
  if (is.null(snapshot)) return(NULL)
  snapshot <- .validate_imaging_collection_snapshot(snapshot)

  source_assets <- (authorized$manifest %||% list())$assets %||% list()
  worker_assets <- worker_manifest$assets %||% list()
  project_record <- function(record) {
    projected <- list(
      sample_id = record$sample_id,
      source_kind = record$source_kind,
      uri = record$uri,
      relative_path = record$relative_path,
      content_hash = record$content_hash,
      size = record$size,
      n_files = record$n_files
    )
    if (!is.null(record$version_id) && !is.na(record$version_id) &&
        nzchar(as.character(record$version_id))) {
      projected$version_id <- as.character(record$version_id)
    }
    projected
  }

  records_by_asset <- list()
  image_uri <- (source_assets$images %||% list())$uri %||% NULL
  image_records <- lapply(snapshot$records, project_record)
  for (asset_name in names(worker_assets)) {
    asset <- worker_assets[[asset_name]]
    kind <- if (is.list(asset)) asset$kind %||% asset$type %||% NULL else NULL
    if (is.list(asset) && is.character(asset$uri) &&
        length(asset$uri) == 1L && !is.na(asset$uri) &&
        identical(kind, "image_root") &&
        identical(asset$uri, image_uri)) {
      records_by_asset[[asset_name]] <- image_records
    }
  }

  roster <- authorized$privacy_roster %||%
    (authorized$handle %||% list())$privacy_roster
  backend <- authorized$backend %||% (authorized$handle %||% list())$backend
  for (asset_name in intersect(names(worker_assets), names(source_assets))) {
    if (!is.null(records_by_asset[[asset_name]])) next
    source_asset <- source_assets[[asset_name]]
    worker_asset <- worker_assets[[asset_name]]
    kind <- source_asset$kind %||% source_asset$type %||% NULL
    if (!identical(kind, "mask_root") || !is.list(worker_asset) ||
        !identical(worker_asset$uri %||% NULL, source_asset$uri %||% NULL)) {
      next
    }
    if (!inherits(backend, "dsimaging_backend")) {
      stop("Mask asset index is invalid.", call. = FALSE)
    }
    index <- .validated_mask_hash_index(
      backend, authorized$manifest, asset_name, required = TRUE)
    index_ids <- as.character(index$sample_id)
    canonical_ids <- .canonical_imaging_privacy_ids(index_ids)
    if (!identical(index_ids, canonical_ids)) {
      stop("Mask asset index is invalid.", call. = FALSE)
    }
    .assert_exact_imaging_roster(
      index_ids, roster, context = "Imaging mask index")
    snapshot_ids <- vapply(
      snapshot$records, `[[`, character(1), "sample_id")
    index <- index[match(snapshot_ids, index_ids), , drop = FALSE]
    if (!all(c("size", "source_kind") %in% names(index))) {
      stop("Mask asset index is invalid.", call. = FALSE)
    }
    sizes <- suppressWarnings(as.numeric(index$size))
    source_kinds <- as.character(index$source_kind)
    relative <- tryCatch(vapply(index$uri, .snapshot_relative_object,
      character(1), root = source_asset$uri, backend_type = backend$type),
      error = function(e) NULL)
    valid <- length(sizes) == nrow(index) && !anyNA(sizes) &&
      all(is.finite(sizes)) && all(sizes >= 0) && all(sizes %% 1 == 0) &&
      length(source_kinds) == nrow(index) && !anyNA(source_kinds) &&
      all(source_kinds == "mask_file") && !is.null(relative) &&
      !anyDuplicated(relative)
    if (!isTRUE(valid)) stop("Mask asset index is invalid.", call. = FALSE)

    versions <- rep(NA_character_, nrow(index))
    if ("version_id" %in% names(index)) {
      versions <- as.character(index$version_id)
      versions[versions %in% c("", "null")] <- NA_character_
      valid_versions <- is.na(versions) | vapply(versions, .snapshot_scalar,
        logical(1), max_bytes = 1024L)
      if (!all(valid_versions)) {
        stop("Mask asset index is invalid.", call. = FALSE)
      }
    }
    records_by_asset[[asset_name]] <- lapply(seq_len(nrow(index)), function(i) {
      record <- list(
        sample_id = as.character(index$sample_id[[i]]),
        source_kind = "mask_file",
        uri = as.character(index$uri[[i]]),
        relative_path = relative[[i]],
        content_hash = as.character(index$content_hash[[i]]),
        size = sizes[[i]],
        n_files = 1L
      )
      if (!is.na(versions[[i]])) record$version_id <- versions[[i]]
      record
    })
  }

  mapped_assets <- names(records_by_asset)
  if (length(mapped_assets) == 0L) return(NULL)
  list(version = 1L, seal = snapshot$seal,
       asset_names = as.list(unname(mapped_assets)),
       records = records_by_asset[[mapped_assets[[1L]]]],
       records_by_asset = records_by_asset)
}

#' Hash one exact worker asset map in stable sample order
#' @keywords internal
.imaging_worker_asset_map_hash <- function(collection_map, asset_name) {
  if (is.null(collection_map)) return(NULL)
  records_by_asset <- collection_map$records_by_asset
  records <- if (is.list(records_by_asset)) {
    records_by_asset[[asset_name]]
  } else NULL
  if (is.null(records)) return(NULL)
  if (!is.list(records) || length(records) == 0L) {
    stop("Imaging asset mapping is invalid.", call. = FALSE)
  }
  canonical <- lapply(records, function(record) {
    if (!is.list(record) || !.snapshot_scalar(record$sample_id) ||
        !.snapshot_scalar(record$uri) ||
        !.snapshot_scalar(record$content_hash, max_bytes = 64L) ||
        !grepl("^[0-9a-f]{64}$", record$content_hash) ||
        length(record$size) != 1L || is.na(record$size) ||
        !is.finite(as.numeric(record$size)) || as.numeric(record$size) < 0 ||
        as.numeric(record$size) %% 1 != 0) {
      stop("Imaging asset mapping is invalid.", call. = FALSE)
    }
    version_id <- record$version_id %||% ""
    if (!identical(version_id, "") &&
        !.snapshot_scalar(version_id, max_bytes = 1024L)) {
      stop("Imaging asset mapping is invalid.", call. = FALSE)
    }
    list(
      sample_id = record$sample_id,
      uri = record$uri,
      version_id = version_id,
      content_hash = record$content_hash,
      size = format(as.numeric(record$size), scientific = FALSE, trim = TRUE,
                    digits = 22L)
    )
  })
  ids <- vapply(canonical, `[[`, character(1), "sample_id")
  if (anyDuplicated(ids)) {
    stop("Imaging asset mapping is invalid.", call. = FALSE)
  }
  canonical <- canonical[order(ids, method = "radix")]
  compute_derivation_hash(asset_name = asset_name, records = canonical)
}

#' Resolve an immutable identity for one exact worker input
#' @keywords internal
.imaging_worker_asset_identity <- function(collection_map, worker_manifest,
                                           asset_name) {
  map_hash <- .imaging_worker_asset_map_hash(collection_map, asset_name)
  if (!is.null(map_hash)) {
    return(compute_derivation_hash(kind = "exact_map", hash = map_hash))
  }
  asset <- (worker_manifest$assets %||% list())[[asset_name]]
  identity <- if (is.list(asset)) asset$.dsimaging_asset_identity else NULL
  asset_id <- identity$asset_id %||% NULL
  derivation_hash <- identity$derivation_hash %||% NULL
  uri <- if (is.list(asset)) asset$uri %||% NULL else NULL
  valid <- is.list(identity) && .snapshot_scalar(asset_id, max_bytes = 64L) &&
    grepl("^asset_[0-9a-f]{32}$", asset_id) &&
    is.character(derivation_hash) && length(derivation_hash) == 1L &&
    !is.na(derivation_hash) && nchar(derivation_hash, type = "bytes") <= 128L &&
    !grepl("[\r\n]", derivation_hash) && .snapshot_scalar(uri)
  if (!isTRUE(valid)) {
    stop("Requested imaging asset has no immutable identity.", call. = FALSE)
  }
  compute_derivation_hash(
    kind = "catalog_asset", asset_id = asset_id,
    derivation_hash = derivation_hash, uri = uri)
}

#' Register an opaque worker-only dataset context
#'
#' The original dataset id is never promoted from an analyst's Opal resource
#' into the shared registry. A high-entropy context name points to a read-only
#' snapshot of the admitted manifest and is embedded only in the server-side
#' job specification.
#' @keywords internal
.register_imaging_worker_context <- function(authorized,
                                             asset_names = character(0),
                                             collection_map = NULL,
                                             worker_manifest = NULL) {
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
  normalized_asset_names <- unique(as.character(
    unlist(asset_names, use.names = FALSE)))
  normalized_asset_names <- normalized_asset_names[
    !is.na(normalized_asset_names) & nzchar(normalized_asset_names)]
  normalized_asset_names <- vapply(
    normalized_asset_names, .imaging_safe_name, character(1),
    name = "asset name")
  if (is.null(worker_manifest)) {
    context_manifest <- .imaging_worker_manifest(
      authorized, normalized_asset_names)
  } else {
    manifest_assets <- if (is.list(worker_manifest)) {
      worker_manifest$assets
    } else NULL
    valid_manifest <- is.list(worker_manifest) && is.list(manifest_assets) &&
      is.character(worker_manifest$dataset_id) &&
      length(worker_manifest$dataset_id) == 1L &&
      !is.na(worker_manifest$dataset_id) &&
      identical(worker_manifest$dataset_id, authorized$dataset_id) &&
      !is.null(names(manifest_assets)) &&
      identical(sort(names(manifest_assets), method = "radix"),
                sort(normalized_asset_names, method = "radix"))
    if (!isTRUE(valid_manifest)) {
      stop("Imaging worker manifest is invalid.", call. = FALSE)
    }
    context_manifest <- worker_manifest
  }
  context_manifest$.dsimaging_privacy_roster <- roster
  context_manifest$.dsimaging_collection_seal <-
    .imaging_authorized_collection_seal(authorized)
  worker_context <- list(
    schema_version = 1L,
    context_id = context_id,
    manifest = context_manifest,
    backend = portable_backend)
  if (is.null(collection_map)) {
    collection_map <- .imaging_worker_collection_map(
      authorized, context_manifest)
  } else if (!is.list(collection_map) ||
             !identical(as.integer(collection_map$version), 1L)) {
    stop("Imaging asset mapping is invalid.", call. = FALSE)
  }
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
  state <- .imaging_session_state(found$owner_env, create = FALSE)
  entry <- if (is.null(state)) NULL else
    state$handles[[found$reference$capability]]
  if (is.null(entry) || !is.list(entry$handle)) {
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
  state <- .imaging_session_state(owner_env, create = TRUE)
  authorizations <- state$private_asset_authorizations
  authorizations[[asset_id]] <- TRUE
  invisible(TRUE)
}

#' @keywords internal
.private_imaging_asset_is_authorized <- function(asset_id, owner_env) {
  if (!is.environment(owner_env)) return(FALSE)
  state <- tryCatch(
    .imaging_session_state(owner_env, create = FALSE),
    error = function(e) NULL)
  !is.null(state) && isTRUE(state$private_asset_authorizations[[asset_id]])
}
