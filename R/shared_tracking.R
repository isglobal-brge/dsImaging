# Module: Shared dsHPC Tracking
#
# dsHPC owns the public, disclosure-safe tracking catalogue. dsImaging owns
# the domain reference stored behind each reusable output and never publishes
# paths, manifests, credentials, or per-image execution identifiers.

#' @keywords internal
.imaging_tracking_id <- function(value, required = TRUE) {
  valid <- is.character(value) && length(value) == 1L && !is.na(value) &&
    grepl(paste0("^trk_[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-",
                 "[89ab][0-9a-f]{3}-[0-9a-f]{12}$"), value)
  if (isTRUE(valid)) return(value)
  if (isTRUE(required)) {
    stop("Shared imaging workflow tracking is unavailable.", call. = FALSE)
  }
  NULL
}

#' Conservative identity of the installed dsHPC runtime used by dsImaging
#' @keywords internal
.imaging_runtime_identity <- function(unit_selection = NULL) {
  identity <- dsHPC::hpcRuntimeIdentityInternal(
    unit_selection = unit_selection)
  if (!is.character(identity) || length(identity) != 1L || is.na(identity) ||
      !grepl("^[0-9a-f]{64}$", identity)) {
    stop("Imaging runtime identity is unavailable.", call. = FALSE)
  }
  identity
}

#' Verify a generation still uses the runtime under which it was claimed
#' @keywords internal
.imaging_generation_runtime_identity <- function(spec) {
  expected <- spec$runtime_identity %||% NULL
  current <- .imaging_runtime_identity(spec$dshpc_unit %||% NULL)
  if (!is.null(expected) && (!is.character(expected) || length(expected) != 1L ||
      is.na(expected) || !grepl("^[0-9a-f]{64}$", expected) ||
      !identical(expected, current))) {
    stop("Imaging generation runtime changed after submission.", call. = FALSE)
  }
  current
}

#' Create or reuse one public logical root for an imaging derivation
#' @keywords internal
.imaging_tracking_create <- function(kind, derivation_hash) {
  kind <- .imaging_safe_name(kind, "tracking kind")
  if (!is.character(derivation_hash) || length(derivation_hash) != 1L ||
      is.na(derivation_hash) || !grepl("^[0-9a-f]{64}$", derivation_hash)) {
    stop("Shared imaging workflow tracking is unavailable.", call. = FALSE)
  }
  reuse_key <- compute_derivation_hash(
    contract = "dsImaging-shared-tracking-v2",
    kind = kind,
    derivation_hash = derivation_hash,
    runtime_identity = .imaging_runtime_identity())
  created <- dsHPC::hpcTrackingCreateInternal(reuse_key = reuse_key,
    kind = "imaging")
  if (!is.list(created) || !identical(created$kind, "imaging")) {
    stop("Shared imaging workflow tracking is unavailable.", call. = FALSE)
  }
  created$tracking_id <- .imaging_tracking_id(created$tracking_id)
  created
}

#' Read the tracking root sealed into a durable collection generation
#' @keywords internal
.imaging_generation_tracking_id <- function(generation, required = TRUE) {
  if (is.character(generation) && length(generation) == 1L &&
      grepl("^gen_[0-9a-f]{32}$", generation)) {
    generation <- get_generation(generation)
  }
  spec <- if (is.list(generation)) tryCatch(
    jsonlite::fromJSON(generation$spec_json, simplifyVector = FALSE),
    error = function(e) NULL) else NULL
  column_id <- if (is.list(generation)) .imaging_tracking_id(
    generation$tracking_id %||% NULL, required = FALSE) else NULL
  spec_id <- .imaging_tracking_id(
    spec$tracking_id %||% NULL, required = FALSE)
  if (!is.null(column_id) && !is.null(spec_id) &&
      !identical(column_id, spec_id)) {
    stop("Shared imaging workflow tracking is unavailable.", call. = FALSE)
  }
  .imaging_tracking_id(column_id %||% spec_id, required = required)
}

#' Persist a root on a pre-existing generation during rolling upgrades
#' @keywords internal
.imaging_set_generation_tracking_id <- function(generation_id, tracking_id) {
  tracking_id <- .imaging_tracking_id(tracking_id)
  generation <- get_generation(generation_id)
  if (is.null(generation)) {
    stop("Shared imaging workflow tracking is unavailable.", call. = FALSE)
  }
  spec <- tryCatch(
    jsonlite::fromJSON(generation$spec_json, simplifyVector = FALSE),
    error = function(e) NULL)
  if (!is.list(spec)) {
    stop("Shared imaging workflow tracking is unavailable.", call. = FALSE)
  }
  existing <- .imaging_tracking_id(spec$tracking_id %||% NULL,
                                    required = FALSE)
  column_id <- .imaging_tracking_id(
    generation$tracking_id %||% NULL, required = FALSE)
  if (!is.null(existing) && !identical(existing, tracking_id)) {
    stop("Shared imaging workflow tracking is unavailable.", call. = FALSE)
  }
  if (!is.null(column_id) && !identical(column_id, tracking_id)) {
    stop("Shared imaging workflow tracking is unavailable.", call. = FALSE)
  }
  if (is.null(existing)) {
    spec$tracking_id <- tracking_id
  }
  update_generation(generation_id, tracking_id = tracking_id,
    spec_json = as.character(jsonlite::toJSON(spec, auto_unbox = TRUE)))
  invisible(tracking_id)
}

#' Find one durable collection generation by its public tracking root
#' @keywords internal
.imaging_generation_for_tracking <- function(tracking_id) {
  tracking_id <- .imaging_tracking_id(tracking_id)
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db), add = TRUE)
  rows <- DBI::dbGetQuery(db, paste(
    "SELECT generation_id, dataset_id, collection_seal, kind, visibility,",
    "state, published_asset_id, tracking_id, spec_json",
    "FROM asset_generations WHERE tracking_id = ?"),
    params = list(tracking_id))
  if (nrow(rows) != 1L) {
    stop("Shared imaging workflow is unavailable.", call. = FALSE)
  }
  as.list(rows[1L, , drop = FALSE])
}

#' Publish one validated global asset behind a tracking output
#' @keywords internal
.imaging_tracking_asset <- function(asset_id) {
  if (!is.character(asset_id) || length(asset_id) != 1L || is.na(asset_id) ||
      !grepl("^asset_[0-9a-f]{32}$", asset_id)) {
    stop("Shared imaging output is unavailable.", call. = FALSE)
  }
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db), add = TRUE)
  asset <- .asset_get(db, asset_id)
  valid <- is.list(asset) && identical(asset$status, "active") &&
    identical(asset$visibility, "global") &&
    !is.null(.asset_collection_seal(asset$collection_seal))
  if (!isTRUE(valid)) {
    stop("Shared imaging output is unavailable.", call. = FALSE)
  }
  asset
}

#' Publish one validated global asset behind a tracking output
#' @keywords internal
.imaging_tracking_publish_asset <- function(tracking_id, asset_id,
                                             finish = FALSE,
                                             success = TRUE) {
  tracking_id <- .imaging_tracking_id(tracking_id)
  .imaging_tracking_asset(asset_id)
  dsHPC::hpcTrackingPublishReferenceInternal(
    tracking_id = tracking_id,
    output_name = "imaging_asset",
    reference = asset_id,
    classification = "server_reusable")
  if (isTRUE(finish)) {
    dsHPC::hpcTrackingFinishInternal(tracking_id, success = isTRUE(success))
  }
  invisible(asset_id)
}

#' Reuse a validated asset without republishing a sealed tracking root
#' @keywords internal
.imaging_tracking_reuse_asset <- function(tracking, asset_id) {
  if (!is.list(tracking)) {
    stop("Shared imaging workflow tracking is unavailable.", call. = FALSE)
  }
  tracking_id <- .imaging_tracking_id(tracking$tracking_id)
  if (isTRUE(tracking$is_done)) {
    .imaging_tracking_asset(asset_id)
    resolved <- tryCatch(.imaging_tracking_resolve_asset(
      tracking_id, output_name = "imaging_asset"), error = function(e) NULL)
    queue_visibility <- tolower(as.character(
      .dshpc_option_safe("queue_visibility") %||% "shared")[[1L]])
    if (is.null(resolved) && identical(queue_visibility, "scoped")) {
      return(invisible(asset_id))
    }
    if (!identical(resolved, asset_id)) {
      stop("Shared imaging output is unavailable.", call. = FALSE)
    }
    return(invisible(asset_id))
  }
  .imaging_tracking_publish_asset(tracking_id, asset_id, finish = TRUE)
}

#' Publish the asset produced by a direct dsHPC child
#' @keywords internal
.imaging_tracking_publish_job_asset <- function(job_id, asset_id) {
  tracking_id <- dsHPC::hpcTrackingForJobInternal(job_id)
  tracking_id <- .imaging_tracking_id(tracking_id, required = FALSE)
  if (is.null(tracking_id)) return(invisible(FALSE))
  # The publisher runs while the primary execution is still RUNNING. dsHPC
  # seals the explicit root only after that primary reaches a successful
  # terminal state; sealing here would race the remaining job lifecycle.
  .imaging_tracking_publish_asset(tracking_id, asset_id, finish = FALSE)
  invisible(TRUE)
}

#' Seal a collection root after publication or an unrecoverable terminal state
#' @keywords internal
.imaging_tracking_finish_generation <- function(generation_id, success) {
  tracking_id <- .imaging_generation_tracking_id(generation_id,
                                                  required = FALSE)
  if (is.null(tracking_id)) return(invisible(FALSE))
  dsHPC::hpcTrackingFinishInternal(tracking_id, success = isTRUE(success))
  invisible(TRUE)
}

#' Publish a collection asset and seal its logical root
#' @keywords internal
.imaging_tracking_publish_generation <- function(generation_id, asset_id) {
  tracking_id <- .imaging_generation_tracking_id(generation_id)
  .imaging_tracking_publish_asset(tracking_id, asset_id, finish = TRUE)
}

#' Resolve and validate one shared dsImaging asset reference
#' @keywords internal
.imaging_tracking_resolve_asset <- function(reference_or_tracking_id,
                                            output_name = NULL,
                                            dataset_id = NULL,
                                            kind = NULL,
                                            derivation_hash = NULL,
                                            collection_seal = NULL) {
  resolved <- tryCatch(
    dsHPC::hpcTrackingResolveOutputInternal(
      reference_or_tracking_id, output_name = output_name),
    error = function(e) NULL)
  valid <- inherits(resolved, "dshpc_domain_output_reference") &&
    is.list(resolved) && identical(resolved$provider, "dsImaging") &&
    identical(resolved$classification, "server_reusable") &&
    identical(resolved$output_name, "imaging_asset") &&
    is.character(resolved$reference) && length(resolved$reference) == 1L &&
    !is.na(resolved$reference) &&
    grepl("^asset_[0-9a-f]{32}$", resolved$reference)
  if (!isTRUE(valid)) {
    stop("Shared imaging output is unavailable.", call. = FALSE)
  }

  db <- .asset_db_connect()
  on.exit(.asset_db_close(db), add = TRUE)
  asset <- .asset_get(db, resolved$reference)
  valid_asset <- is.list(asset) && identical(asset$status, "active") &&
    identical(asset$visibility, "global") &&
    !is.null(.asset_collection_seal(asset$collection_seal))
  expected <- list(dataset_id = dataset_id, kind = kind,
    derivation_hash = derivation_hash, collection_seal = collection_seal)
  for (field in names(expected)) {
    value <- expected[[field]]
    if (!is.null(value) &&
        !identical(as.character(asset[[field]]), as.character(value))) {
      valid_asset <- FALSE
    }
  }
  if (!isTRUE(valid_asset)) {
    stop("Shared imaging output is unavailable.", call. = FALSE)
  }
  resolved$reference
}

#' Resolve an asset id or a dsHPC opaque output reference
#' @keywords internal
.imaging_resolve_asset_argument <- function(value) {
  if (!inherits(value, "dshpc_output_reference")) {
    return(.imaging_safe_name(value, "asset_id_or_alias"))
  }
  .imaging_tracking_resolve_asset(value)
}
