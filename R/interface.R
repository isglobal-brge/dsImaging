# Module: DataSHIELD Exposed Methods
# Assign and aggregate methods for imaging dataset management.

#' Initialize Imaging Handle
#'
#' DataSHIELD ASSIGN method. Creates an imaging dataset handle from a
#' resource that was assigned via \code{datashield.assign.resource()}.
#' The resource must be an \code{imaging+dataset://} URL resolved by
#' \code{ImagingDatasetResourceResolver}.
#'
#' @param resource_symbol Character; symbol name of the assigned resource.
#' @return An imaging handle object (assigned server-side).
#' @export
imagingInitDS <- function(resource_symbol) {
  .dsimaging_require_literal_arguments()
  # resource_symbol is a STRING (e.g. "img_res"), not the object itself.
  # The ResourceClient is already in the session env from datashield.assign.resource().
  # Pattern matches dsOMOP: omopInitDS("res") -> get("res", parent.frame())
  owner_env <- parent.frame()
  if (!is.character(resource_symbol) || length(resource_symbol) != 1L ||
      is.na(resource_symbol) || !nzchar(resource_symbol) ||
      !exists(resource_symbol, envir = owner_env, inherits = FALSE)) {
    stop("Imaging resource could not be initialized.", call. = FALSE)
  }
  obj <- get(resource_symbol, envir = owner_env, inherits = FALSE)

  # Opal, Armadillo, and DSLite resolve the authorized descriptor before the
  # assign method runs. Requiring that client prevents an in-session raw
  # descriptor from bypassing the platform's Resource authorization.
  if (inherits(obj, "ImagingDatasetResourceClient")) {
    return(tryCatch({
      handle <- .imaging_handle_from_resource_client(obj, resource_symbol)
      .register_imaging_handle(handle, owner_env)
    }, error = function(e) {
      stop("Imaging resource could not be initialized.", call. = FALSE)
    }))
  }

  stop("Imaging resource could not be initialized.", call. = FALSE)
}

#' Destroy an Imaging Handle
#'
#' DataSHIELD ASSIGN method. Removes a session-owned imaging handle capability
#' and its symbol without returning dataset details. Dependent feature views
#' are invalidated at the same time. A retry is idempotent only when the same
#' session still contains the well-formed opaque reference after its private
#' payload has already been removed.
#'
#' @param handle_symbol Character; symbol of the imaging handle to destroy.
#' @return The opaque reference as an invisible retry tombstone. DataSHIELD
#'   assigns it back to the same symbol until the client confirms removal.
#' @export
imagingDestroyDS <- function(handle_symbol) {
  .dsimaging_require_literal_arguments()
  owner_env <- parent.frame()
  unavailable <- function() {
    stop("Unknown or unavailable imaging handle reference.", call. = FALSE)
  }
  if (!is.environment(owner_env) ||
      !is.character(handle_symbol) || length(handle_symbol) != 1L ||
      is.na(handle_symbol) || !nzchar(handle_symbol) ||
      !exists(handle_symbol, envir = owner_env, inherits = FALSE)) {
    unavailable()
  }
  reference <- get(handle_symbol, envir = owner_env, inherits = FALSE)
  if (!.is_imaging_handle_reference(reference)) unavailable()
  if (bindingIsLocked(handle_symbol, owner_env)) unavailable()
  state <- tryCatch(
    .imaging_session_state(owner_env, create = FALSE),
    error = function(e) NULL)
  if (is.null(state)) unavailable()
  entry <- state$handles[[reference$capability]]
  if (identical(entry, .dsimaging_session_tombstone)) {
    # Idempotent retry after authoritative state was removed but the session
    # symbol could not be cleared (for example, a lost destroy response).
    .invalidate_imaging_feature_views(state, reference$capability)
    rm(list = handle_symbol, envir = owner_env)
    return(invisible(reference))
  }
  if (is.null(entry) || !is.list(entry$handle)) {
    unavailable()
  }

  .invalidate_imaging_feature_views(state, reference$capability)
  handles <- state$handles
  handles[[reference$capability]] <- .dsimaging_session_tombstone
  rm(list = handle_symbol, envir = owner_env)
  invisible(reference)
}

#' Build a handle only from a ResourceClient bound to its Resource URL
#' @keywords internal
.imaging_handle_from_resource_client <- function(client, symbol) {
  resource <- client$getResource()
  .validate_imaging_resource_object(resource)
  parsed <- .validate_imaging_resource_location(
    .parse_imaging_url(resource$url))
  dataset_id <- client$getDatasetId()
  if (!is.character(dataset_id) || length(dataset_id) != 1L ||
      !identical(dataset_id, parsed$dataset_id)) {
    stop("Imaging resource does not match its dataset.", call. = FALSE)
  }
  manifest <- client$getManifest()
  .assert_imaging_resource_dataset(manifest, dataset_id)
  backend <- client$getBackend()
  manifest_uri <- client$getManifestUri()
  .assert_imaging_manifest_scope(manifest, manifest_uri, backend)
  desc <- client$asImagingDescriptor()
  if (!inherits(desc, "ImagingDatasetDescriptor") ||
      !identical(as.character(desc$dataset_id), dataset_id)) {
    stop("Imaging resource does not match its dataset.", call. = FALSE)
  }
  .assert_imaging_resource_dataset(desc$manifest, dataset_id)
  .assert_imaging_manifest_scope(desc$manifest, manifest_uri, backend)
  .make_imaging_handle(desc, symbol, backend = backend,
    manifest_uri = manifest_uri, require_snapshot = TRUE)
}

#' Create an imaging handle from a descriptor
#'
#' Also performs disclosure check: blocks creation if the dataset has
#' fewer samples than the nfilter threshold.
#'
#' @keywords internal
.make_imaging_handle <- function(desc, symbol, backend = NULL,
                                 manifest_uri = NULL,
                                 require_snapshot = FALSE) {
  backend <- .sanitize_imaging_backend(backend, desc$dataset_id)
  collection_snapshot <- NULL
  if (isTRUE(require_snapshot)) {
    if (is.null(manifest_uri)) {
      stop("Imaging collection integrity could not be verified.", call. = FALSE)
    }
    read_snapshot <- function() {
      manifest <- parse_manifest(manifest_uri, backend)
      .assert_imaging_resource_dataset(manifest, desc$dataset_id)
      .assert_imaging_manifest_scope(manifest, manifest_uri, backend)
      # Resolve the metadata location before admission so `file` and `uri`
      # cannot name two different tables with the same roster.
      .snapshot_artifact_uri(manifest$metadata, backend)
      current_admission <- .imaging_privacy_admission(manifest, backend)
      list(
        desc = imaging_dataset_descriptor(manifest),
        admission = current_admission,
        snapshot = .new_imaging_collection_snapshot(
          manifest, backend, current_admission))
    }
    .assert_dataset_not_publishing(list(
      backend = backend, manifest_uri = manifest_uri))
    first <- read_snapshot()
    .assert_dataset_not_publishing(list(
      backend = backend, manifest_uri = manifest_uri))
    second <- read_snapshot()
    .assert_dataset_not_publishing(list(
      backend = backend, manifest_uri = manifest_uri))
    if (!identical(first$snapshot$seal, second$snapshot$seal)) {
      stop("Imaging collection changed during admission.", call. = FALSE)
    }
    desc <- second$desc
    admission <- second$admission
    collection_snapshot <- second$snapshot
  } else {
    .assert_dataset_not_publishing(list(
      backend = backend, manifest_uri = manifest_uri))
    admission <- .imaging_privacy_admission(desc$manifest, backend)
  }

  handle <- list(
    source      = "imaging_resource",
    dataset_id  = desc$dataset_id,
    descriptor  = desc,
    manifest    = desc$manifest,
    backend     = backend,
    manifest_uri = manifest_uri,
    privacy     = admission$contract,
    n_privacy_units = admission$n_privacy_units,
    privacy_roster = admission$roster,
    collection_seal = if (is.null(collection_snapshot)) {
      digest::digest(list(manifest = desc$manifest,
        privacy_roster = admission$roster), algo = "sha256", serialize = TRUE)
    } else collection_snapshot$seal,
    collection_snapshot = collection_snapshot,
    created_at  = Sys.time()
  )
  handle
}

#' Get the storage backend from an imaging handle
#'
#' Used by imaging analysis runners to access S3/MinIO dataset images.
#' The backend is stored when imagingInitDS creates the handle.
#'
#' @param handle_symbol Character; the symbol name of the imaging handle.
#' @return A dsimaging_backend object, or NULL if no backend.
#' @export
imagingGetBackendDS <- function(handle_symbol) {
  .legacy_imaging_ds_disabled("imagingGetBackendDS")
}

#' Get the manifest from an imaging handle
#'
#' @param handle_symbol Character; the symbol name.
#' @return Parsed manifest list, or NULL.
#' @export
imagingGetManifestDS <- function(handle_symbol) {
  .legacy_imaging_ds_disabled("imagingGetManifestDS")
}

#' Get an imaging handle by symbol
#' @keywords internal
.get_imaging_handle <- function(symbol) {
  tryCatch(.getImagingHandle(symbol), error = function(e) NULL)
}

#' Disclosure-controlled label distribution for an imaging dataset
#'
#' DataSHIELD AGGREGATE method. Reads the dataset's sample-metadata table on the
#' node and tabulates only the manifest-declared label at patient level. The
#' complete distribution is withheld if any cell is below the DataSHIELD
#' \code{nfilter.tab} threshold; admitted counts are then hidden, bucketed, or
#' released according to the configured privacy profile. The pixels never
#' leave the node.
#'
#' @param handle_symbol Character; the imaging handle symbol.
#' @param column Character or NULL; must be the manifest's declared
#'   \code{metadata.label_col}; NULL selects that declared column.
#' @return A data.frame with columns \code{label} and \code{n}, or an empty
#'   data.frame when the complete distribution is not releasable.
#' @export
imagingLabelDistDS <- function(handle_symbol, column = NULL) {
  .dsimaging_require_literal_arguments()
  authorized <- .authorized_imaging_dataset(
    handle_symbol, owner_env = parent.frame())
  contract <- authorized$privacy
  declared <- contract$label_col
  if (is.null(declared) ||
      (!is.null(column) && !identical(as.character(column), declared))) {
    stop("Only the manifest-declared label column may be tabulated.",
         call. = FALSE)
  }

  df <- .read_imaging_metadata_table(
    authorized$manifest, authorized$backend)
  ids <- .validate_imaging_privacy_metadata(
    df, contract, require_label = TRUE)
  .assert_min_privacy_units(length(unique(ids$privacy_ids)),
    context = "imaging dataset")

  labels <- as.character(df[[declared]])
  per_patient <- split(labels, ids$privacy_ids)
  conflicting <- vapply(per_patient, function(x) length(unique(x)) != 1L,
                        logical(1))
  if (any(conflicting)) {
    stop("Imaging metadata does not satisfy its privacy contract.",
         call. = FALSE)
  }
  patient_labels <- vapply(per_patient, `[[`, character(1), 1L)
  approved_levels <- contract$label_levels %||% character(0)
  observed_levels <- unique(patient_labels)
  identifiers <- unique(c(ids$sample_ids, ids$privacy_ids))
  if (length(approved_levels) == 0L ||
      any(approved_levels %in% identifiers) ||
      !all(observed_levels %in% approved_levels)) {
    return(data.frame(label = character(0), n = integer(0),
                      stringsAsFactors = FALSE))
  }
  counts <- table(factor(patient_labels, levels = approved_levels))
  safe <- safe_metadata_distribution(counts)
  if (is.null(safe)) {
    return(data.frame(label = character(0), n = integer(0),
                      stringsAsFactors = FALSE))
  }
  safe_levels <- safe_metadata_levels(names(safe), length(patient_labels))
  if (is.null(safe_levels)) {
    return(data.frame(label = character(0), n = integer(0),
                      stringsAsFactors = FALSE))
  }
  keep <- !is.na(safe) & names(safe) %in% safe_levels
  data.frame(label = names(safe)[keep], n = as.integer(safe[keep]),
             stringsAsFactors = FALSE)
}

#' List available label sets for an imaging dataset
#'
#' DataSHIELD AGGREGATE method. Returns the label sets defined in the
#' dataset's manifest. The researcher can then choose which label set
#' to use for training.
#'
#' @param handle_symbol Character; the imaging handle symbol.
#' @return A data.frame with columns: name, type, columns, description.
#' @export
imagingLabelsDS <- function(handle_symbol) {
  .legacy_imaging_ds_disabled("imagingLabelsDS")
}

#' Get a specific label set's URI from the manifest
#' @keywords internal
.get_label_uri <- function(manifest, label_set_name) {
  labels <- manifest$labels
  if (is.null(labels)) return(NULL)
  for (lbl in labels) {
    if (identical(lbl$name, label_set_name)) return(lbl$uri)
  }
  NULL
}

#' List available mask assets for an imaging dataset
#'
#' DataSHIELD AGGREGATE method. Queries the asset catalog for segmentation
#' mask assets that are ACTIVE, complete, and linked to the connected dataset.
#'
#' @param handle_symbol Character; the imaging handle symbol.
#' @return A data.frame with columns: alias, provider, status, n_valid, created_at.
#' @export
imagingMasksDS <- function(handle_symbol) {
  .legacy_imaging_ds_disabled("imagingMasksDS")
}

#' @keywords internal
.empty_masks_df <- function() {
  data.frame(alias = character(0), provider = character(0),
             status = character(0), n_valid = integer(0),
             created_at = character(0), stringsAsFactors = FALSE)
}

#' Get the asset database connection
#' @keywords internal
.get_asset_db <- function() {
  db_path <- getOption("dsimaging.asset_db",
    file.path(getOption("dsimaging.data_dir", "/var/lib/dsimaging"),
              "imaging_assets.sqlite"))
  if (!file.exists(db_path)) return(NULL)
  tryCatch(DBI::dbConnect(RSQLite::SQLite(), db_path),
           error = function(e) NULL)
}

#' Count samples from a manifest's metadata file
#'
#' Returns the raw count for internal use (e.g. disclosure assertions).
#' Call \code{safe_metadata_count()} on the result before returning to
#' the client.
#'
#' @keywords internal
.count_samples_from_manifest <- function(manifest, backend = NULL) {
  meta <- manifest$metadata
  if (is.null(meta)) return(NA_integer_)

  # Try local file first
  local_file <- meta$file
  if (!is.null(local_file) && file.exists(local_file)) {
    fmt <- meta$format %||% .guess_format(local_file)
    if (identical(fmt, "parquet") && requireNamespace("arrow", quietly = TRUE))
      return(nrow(arrow::read_parquet(local_file, as_data_frame = FALSE)))
    if (identical(fmt, "csv"))
      return(length(readLines(local_file)) - 1L)
  }

  # Local absolute metadata.uri (the documented manifest field). Manifest
  # validation guarantees uri is an absolute path or an s3:// URI, so any
  # non-s3 uri is a local file.
  uri <- meta$uri
  if (!is.null(uri) && !grepl("^s3://", uri) && file.exists(uri)) {
    fmt <- meta$format %||% .guess_format(uri)
    if (identical(fmt, "parquet") && requireNamespace("arrow", quietly = TRUE))
      return(nrow(arrow::read_parquet(uri, as_data_frame = FALSE)))
    if (identical(fmt, "csv"))
      return(length(readLines(uri)) - 1L)
  }

  # Try S3 URI via backend
  if (!is.null(uri) && grepl("^s3://", uri) && !is.null(backend)) {
    tmp <- tempfile(fileext = if (grepl("\\.parquet$", uri)) ".parquet" else ".csv")
    on.exit(unlink(tmp), add = TRUE)
    tryCatch({
      backend_get_file(backend, uri, tmp)
      fmt <- meta$format %||% .guess_format(tmp)
      if (identical(fmt, "parquet") && requireNamespace("arrow", quietly = TRUE))
        return(nrow(arrow::read_parquet(tmp, as_data_frame = FALSE)))
      if (identical(fmt, "csv"))
        return(length(readLines(tmp)) - 1L)
    }, error = function(e) NULL)
  }

  NA_integer_
}

# --- Disclosure controls ---

#' Get the nfilter threshold for minimum sample counts
#'
#' Uses \code{dsimaging.nfilter.subset} when configured, falling back to
#' DataSHIELD's standard \code{nfilter.subset} option.
#'
#' @return Integer; the minimum number of samples to avoid disclosure.
#' @keywords internal
.nfilter_threshold <- function() {
  .get_nfilter_subset()
}

#' Bucket a count to avoid exact disclosure
#'
#' Replaces exact counts with bucketed ranges when below a threshold,
#' following DataSHIELD disclosure conventions.
#'
#' @param n Integer; the exact count.
#' @param threshold Integer; minimum count for exact reporting.
#' @return Integer; the original count if above threshold, or NA.
#' @keywords internal
.safe_count <- function(n, threshold = NULL) {
  if (is.null(threshold)) threshold <- .nfilter_threshold()
  if (is.null(n) || is.na(n)) return(NA_integer_)
  if (n < threshold) return(NA_integer_)
  as.integer(n)
}

#' Assert that a dataset has enough samples
#'
#' @param n_samples Integer; number of samples.
#' @param context Character; context for error message.
#' @keywords internal
.assert_min_samples <- function(n_samples, context = "dataset") {
  threshold <- .nfilter_threshold()
  if (is.null(n_samples) || is.na(n_samples)) {
    stop("Disclosive: ", context, " sample count is unknown. ",
         "Cannot verify disclosure control. Operation blocked.", call. = FALSE)
  }
  if (n_samples < threshold) {
    stop("Disclosive: ", context, " has fewer than ", threshold,
         " samples. Operation blocked.", call. = FALSE)
  }
  invisible(TRUE)
}

#' List Available Imaging Datasets
#'
#' DataSHIELD AGGREGATE method. Returns the dataset_ids and titles of
#' all enabled datasets in the server's registry.
#'
#' @return Data.frame with columns: dataset_id, title, modality, enabled.
#' @keywords internal
.list_imaging_datasets_admin <- function() {
  datasets <- tryCatch(list_datasets(), error = function(e) list())

  if (length(datasets) == 0) {
    return(data.frame(
      dataset_id = character(0),
      title = character(0),
      modality = character(0),
      enabled = logical(0),
      stringsAsFactors = FALSE
    ))
  }

  rows <- lapply(names(datasets), function(id) {
    entry <- datasets[[id]]
    manifest <- tryCatch(.manifest_for_registry_listing(id, entry),
      error = function(e) NULL)
    title <- if (is.null(manifest)) {
      "(error reading manifest)"
    } else {
      manifest$title %||% "(untitled)"
    }
    modality <- if (is.null(manifest)) {
      "unknown"
    } else {
      manifest$modality %||% "unknown"
    }

    data.frame(
      dataset_id = id,
      title = title,
      modality = modality,
      enabled = TRUE,
      stringsAsFactors = FALSE
    )
  })

  do.call(rbind, rows)
}

#' Legacy Dataset Registry Listing Method
#'
#' Retained so stale Opal allowlists fail closed. Dataset discovery is an
#' administrator operation; analyst APIs start from an assigned resource.
#' @export
imagingListDatasetsDS <- function() {
  .legacy_imaging_ds_disabled("imagingListDatasetsDS")
}

#' Resolve a manifest for registry listing
#'
#' @keywords internal
.manifest_for_registry_listing <- function(dataset_id, entry) {
  resolved <- tryCatch(resolve_dataset(dataset_id), error = function(e) NULL)
  if (!is.null(resolved)) {
    return(parse_manifest(resolved$manifest_uri, resolved$backend))
  }

  manifest_uri <- entry$manifest_uri %||% entry$manifest
  if (is.null(manifest_uri) || !nzchar(manifest_uri))
    stop("Registry entry for dataset '", dataset_id,
         "' has no manifest_uri.", call. = FALSE)

  backend <- storage_backend(entry$backend %||% "file", config = list(
    endpoint = entry$endpoint,
    credentials_ref = entry$credentials_ref,
    region = entry$region
  ))
  parse_manifest(manifest_uri, backend)
}

#' Get Imaging Dataset Metadata
#'
#' DataSHIELD AGGREGATE method. Returns disclosure-safe metadata about
#' the imaging dataset (column names, sample count, modality, etc.)
#' without exposing actual data values.
#'
#' @param handle_symbol Character; symbol of the imaging handle.
#' @return Named list with metadata summary.
#' @export
imagingMetadataDS <- function(handle_symbol) {
  .dsimaging_require_literal_arguments()
  authorized <- .authorized_imaging_dataset(
    handle_symbol, owner_env = parent.frame())
  handle <- authorized$handle
  manifest <- authorized$manifest
  meta <- manifest$metadata
  admission <- .imaging_privacy_admission(manifest, authorized$backend)
  safe_n <- safe_metadata_count(admission$n_privacy_units)

  list(
    dataset_id    = .safe_public_identifier(manifest$dataset_id, "unknown"),
    title         = .safe_public_identifier(manifest$title, "unknown"),
    modality      = .safe_public_identifier(manifest$modality, "unknown"),
    task_types    = .safe_public_identifiers(manifest$task_types),
    templates     = .safe_public_identifiers(manifest$compatible_templates),
    # Compatibility: n_samples now means protected privacy units, never rows.
    n_samples     = safe_n,
    n_privacy_units = safe_n,
    columns       = .safe_public_identifiers(names(admission$data)),
    id_col        = .safe_public_identifier(admission$contract$id_col),
    label_col     = .safe_public_identifier(admission$contract$label_col),
    privacy_unit  = .safe_public_identifier(
      admission$contract$privacy_unit, "unknown"),
    privacy_unit_col = .safe_public_identifier(
      admission$contract$privacy_unit_col),
    privacy_unit_canonicalization = .safe_public_identifier(
      admission$contract$privacy_unit_canonicalization, "unknown"),
    n_assets      = length(manifest$assets %||% list()),
    n_samples_sufficient = !is.na(safe_n) && safe_n > 0L
  )
}

#' Validate Imaging Dataset
#'
#' DataSHIELD AGGREGATE method. Runs security and integrity checks on
#' the imaging dataset.
#'
#' @param handle_symbol Character; symbol of the imaging handle.
#' @return Named list with validation results.
#' @export
imagingValidateDS <- function(handle_symbol) {
  .dsimaging_require_literal_arguments()
  authorized <- .authorized_imaging_dataset(
    handle_symbol, owner_env = parent.frame())
  result <- validate_imaging_dataset(
    authorized$manifest, backend = authorized$backend)
  status <- result$asset_status %||% list()
  status_names <- if (length(status) == 0L) character(0) else
    vapply(names(status), .safe_public_identifier, character(1))
  keep_status <- !is.na(status_names)
  status <- status[keep_status]
  status_names <- status_names[keep_status]
  asset_status <- if (length(status) == 0L) {
    data.frame(name = character(0), type = character(0), valid = logical(0),
               stringsAsFactors = FALSE)
  } else {
    data.frame(
      name = status_names,
      type = vapply(status, function(x) .safe_public_identifier(
        x$type, "unknown"), character(1)),
      valid = vapply(status, function(x) isTRUE(x$valid), logical(1)),
      stringsAsFactors = FALSE
    )
  }
  list(
    valid = isTRUE(result$valid),
    errors = if (isTRUE(result$valid)) character(0) else "validation_failed",
    warnings = if (length(result$warnings %||% character(0)) > 0L)
      "validation_warning" else character(0),
    asset_status = asset_status
  )
}

#' List Dataset Assets
#'
#' DataSHIELD AGGREGATE method. Returns the names and types of assets
#' available in the imaging dataset.
#'
#' @param handle_symbol Character; symbol of the imaging handle.
#' @return Data.frame with columns: name, type.
#' @export
imagingAssetsDS <- function(handle_symbol) {
  .dsimaging_require_literal_arguments()
  handle <- .authorized_imaging_dataset(
    handle_symbol, owner_env = parent.frame())$handle
  assets <- handle$manifest$assets %||% list()
  asset_names <- if (length(assets) == 0L) character(0) else
    vapply(names(assets), .safe_public_identifier, character(1))
  keep_assets <- !is.na(asset_names)
  assets <- assets[keep_assets]
  asset_names <- asset_names[keep_assets]

  if (length(assets) == 0) {
    return(data.frame(
      name = character(0), type = character(0),
      stringsAsFactors = FALSE
    ))
  }

  data.frame(
    name = asset_names,
    type = vapply(assets, function(a) .safe_public_identifier(
      a$kind %||% a$type, "unknown"), character(1)),
    stringsAsFactors = FALSE
  )
}

#' Retrieve an imaging handle from storage
#'
#' @param symbol Character; handle symbol name.
#' @return The imaging handle.
#' @keywords internal
.getImagingHandle <- function(symbol) {
  .resolve_imaging_handle(symbol)$handle
}
