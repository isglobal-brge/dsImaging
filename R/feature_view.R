# Module: Opaque Imaging Feature Views
#
# The private registry retains the complete admitted sample-to-patient roster.
# dsFlower accepts opaque views and exact snapshots of admitted legacy exports;
# patient mappings stay private in both cases.

# A naked feature table is still available to legacy DataSHIELD packages. Mark
# the whole session for its lifetime: only a complete unchanged registered
# export in its original row order can regain a patient-bound view.
#' @keywords internal
.mark_imaging_feature_table_export <- function(owner_env) {
  if (!is.environment(owner_env)) {
    stop("Invalid imaging feature-table session.", call. = FALSE)
  }
  state <- .imaging_session_state(owner_env, create = TRUE)
  flags <- state$flags
  flags$feature_table_exported <- TRUE
  invisible(TRUE)
}

# Trusted same-process consumers use this predicate; it is not a DataSHIELD
# method and the state is not represented by a workspace object or attribute.
#' @keywords internal
.imaging_session_exported_feature_table <- function(owner_env) {
  if (!is.environment(owner_env)) return(FALSE)
  state <- tryCatch(
    .imaging_session_state(owner_env, create = FALSE),
    error = function(e) NULL)
  !is.null(state) && isTRUE(state$flags$feature_table_exported)
}

# Normalize only the container and incidental attributes, preserving export row
# order. Values, types and columns stay exact. A missing/duplicate/changed sample key
# fails the same complete-roster guard used by opaque feature views.
# No analyst-controlled attribute grants authority: entries live in session state.
#' @keywords internal
.imaging_export_frame <- function(object, contract = NULL, roster = NULL) {
  if (inherits(object, c("Table", "RecordBatch"))) {
    object <- as.data.frame(object)
  }
  if (!is.data.frame(object)) return(NULL)
  object <- as.data.frame(object)
  if (!is.null(contract) || !is.null(roster)) {
    roster <- .validate_imaging_privacy_roster(roster)
    id_col <- contract$id_col
    if (!is.character(id_col) || length(id_col) != 1L || is.na(id_col) ||
        !id_col %in% names(object) || anyDuplicated(names(object))) {
      stop("Imaging export sample key is unavailable.", call. = FALSE)
    }
    sample_ids <- .canonical_imaging_privacy_ids(object[[id_col]])
    .assert_exact_imaging_roster(sample_ids, roster,
                                context = "Imaging feature table export")
  }
  attributes(object) <- list(names = names(object),
    row.names = .set_row_names(nrow(object)), class = "data.frame")
  object
}

# Retain the already-admitted roster alongside an exact legacy table export.
# Exports without a sample key or the declared label remain available to legacy
# consumers, but cannot be promoted to a patient-bound dsFlower training view.
#' @keywords internal
.register_imaging_feature_table_export <- function(
    data, authorized, handle_symbol, owner_env, syntactic_names = FALSE,
    original_names = names(data)) {
  contract <- authorized$privacy
  manifest <- authorized$manifest
  if (is.null(contract$label_col) ||
      !all(c(contract$id_col, contract$label_col) %in% original_names)) {
    return(invisible(NULL))
  }
  if (isTRUE(syntactic_names)) {
    structural <- unlist(contract[c("id_col", "privacy_unit_col", "label_col")],
                         use.names = FALSE)
    # Preserve exact structural names: repaired-name collisions must not change
    # the identity or target role. Non-structural feature names may be repaired.
    if (!identical(structural, make.names(structural))) return(invisible(NULL))
    positions <- match(structural, original_names)
    present <- !is.na(positions)
    if (!identical(unname(names(data)[positions[present]]),
                   unname(structural[present]))) return(invisible(NULL))
  }
  if (is.null(contract$label_col) ||
      !all(c(contract$id_col, contract$label_col) %in% names(data))) {
    return(invisible(NULL))
  }
  roster <- .validate_imaging_privacy_roster(authorized$privacy_roster)
  sample_ids <- .canonical_imaging_privacy_ids(data[[contract$id_col]])
  complete <- tryCatch({
    .assert_exact_imaging_roster(sample_ids, roster,
                               context = "Imaging feature table export")
    TRUE
  }, error = function(e) FALSE)
  # Legacy loadable assets (for example dose ROI rows) can have multiplicities
  # outside the feature-view contract. Do not change their loading semantics.
  if (!complete) return(invisible(NULL))
  exported_data <- .imaging_export_frame(data, contract, roster)
  sample_ids <- .canonical_imaging_privacy_ids(exported_data[[contract$id_col]])
  patient_map <- stats::setNames(roster$privacy_ids, roster$sample_ids)
  private_data <- exported_data
  private_data[[contract$privacy_unit_col]] <- unname(patient_map[sample_ids])
  state <- .imaging_session_state(owner_env, create = TRUE)
  feature_views <- state$feature_views
  export_sha256 <- digest::digest(exported_data, algo = "sha256", serialize = TRUE)
  data_sha256 <- digest::digest(private_data, algo = "sha256", serialize = TRUE)
  for (existing in ls(feature_views, all.names = TRUE)) {
    entry <- feature_views[[existing]]
    if (is.list(entry) && identical(entry$export_sha256, export_sha256) &&
        identical(entry$data_sha256, data_sha256) &&
        identical(entry$source_handle_capability, authorized$handle_capability)) {
      return(invisible(existing))
    }
  }
  capability <- .new_imaging_feature_view_capability()
  feature_views[[capability]] <- list(
    source_handle_symbol = as.character(handle_symbol),
    source_handle_capability = authorized$handle_capability,
    dataset_id = authorized$dataset_id,
    collection_seal = .imaging_authorized_collection_seal(authorized),
    source_privacy = authorized$privacy,
    manifest = manifest, privacy = contract, privacy_roster = roster,
    data = private_data,
    data_sha256 = data_sha256, exported_data = exported_data,
    export_sha256 = export_sha256)
  invisible(capability)
}

#' @keywords internal
.imaging_export_reference <- function(object, owner_env,
                                       expected_capability = NULL) {
  frame <- .imaging_export_frame(object)
  state <- .imaging_session_state(owner_env, create = FALSE)
  if (is.null(frame) || is.null(state)) return(NULL)
  candidates <- if (is.null(expected_capability)) {
    ls(state$feature_views, all.names = TRUE)
  } else expected_capability
  matched <- character(0)
  for (capability in candidates) {
    entry <- state$feature_views[[capability]]
    if (!is.list(entry) || is.null(entry$exported_data)) next
    normalized <- tryCatch(.imaging_export_frame(
      frame, entry$privacy, entry$privacy_roster), error = function(e) NULL)
    if (is.null(normalized)) next
    fingerprint <- digest::digest(normalized, algo = "sha256", serialize = TRUE)
    if (identical(entry$export_sha256, fingerprint) &&
        identical(entry$exported_data, normalized)) {
      matched <- c(matched, capability)
    }
  }
  # Equal table values alone cannot distinguish different admitted datasets or
  # patient mappings. Ambiguous provenance must never select an arbitrary unit.
  if (length(matched) != 1L) return(NULL)
  structure(list(capability = matched[[1L]]), class = "dsimaging_feature_view_ref")
}

#' @keywords internal
.new_imaging_feature_view_capability <- function() {
  token <- gsub("-", "", paste0(
    uuid::UUIDgenerate(use.time = FALSE),
    uuid::UUIDgenerate(use.time = FALSE)
  ), fixed = TRUE)
  if (!grepl("^[0-9a-f]{64}$", token)) {
    stop("Could not create an imaging feature-view capability.", call. = FALSE)
  }
  paste0("imgf_", token)
}

#' @keywords internal
.is_imaging_feature_view_reference <- function(object) {
  inherits(object, "dsimaging_feature_view_ref") &&
    is.list(object) && identical(names(object), "capability") &&
    is.character(object$capability) && length(object$capability) == 1L &&
    !is.na(object$capability) &&
    grepl("^imgf_[0-9a-f]{64}$", object$capability)
}

#' @keywords internal
.feature_view_public_columns <- function(value, name, required = FALSE) {
  value <- .decode_imaging_columns_arg(value)
  if (is.null(value)) {
    if (isTRUE(required)) stop(name, " is required.", call. = FALSE)
    return(character(0))
  }
  value <- enc2utf8(as.character(value))
  safe <- vapply(value, .safe_public_identifier, character(1))
  if (!length(value) || anyNA(value) || any(!nzchar(value)) ||
      anyDuplicated(value) || !identical(unname(safe), unname(value))) {
    stop(name, " must contain unique public column names.", call. = FALSE)
  }
  unname(value)
}

#' @keywords internal
.feature_view_public_column <- function(value, name) {
  columns <- .feature_view_public_columns(value, name, required = TRUE)
  if (length(columns) != 1L) {
    stop(name, " must be one public column name.", call. = FALSE)
  }
  columns[[1L]]
}

#' @keywords internal
.feature_view_public_levels <- function(value) {
  if (is.character(value) && length(value) == 1L &&
      startsWith(value, "B64:")) {
    value <- .dsr_decode(value)
  }
  if (is.factor(value)) value <- as.character(value)
  if (is.list(value)) {
    if (!is.null(names(value)) ||
        any(vapply(value, length, integer(1)) != 1L)) {
      stop("target_levels must be a public vector.", call. = FALSE)
    }
    kinds <- vapply(value, function(level) {
      if (is.character(level)) "character" else
        if (is.logical(level)) "logical" else
          if (is.numeric(level)) "numeric" else "unsupported"
    }, character(1))
    if (length(unique(kinds)) != 1L || identical(kinds[[1L]], "unsupported")) {
      stop("target_levels must be a public vector.", call. = FALSE)
    }
    value <- unlist(value, use.names = FALSE)
  }
  if (!is.atomic(value) ||
      !(is.character(value) || is.logical(value) || is.numeric(value)) ||
      length(value) < 2L || length(value) > 1024L || anyNA(value) ||
      (is.numeric(value) && any(!is.finite(value)))) {
    stop("target_levels must be a public classification vocabulary.",
         call. = FALSE)
  }
  value <- enc2utf8(as.character(value))
  safe <- vapply(value, .safe_public_identifier, character(1))
  if (anyDuplicated(value) ||
      !identical(unname(safe), unname(value))) {
    stop("target_levels must be a public classification vocabulary.",
         call. = FALSE)
  }
  unname(value)
}

#' @keywords internal
.external_clinical_feature_view <- function(
    authorized, asset_id_or_alias, columns, clinical_symbol, clinical_id_col,
    clinical_columns, target_col, target_levels, owner_env) {
  if (.imaging_session_exported_feature_table(owner_env)) {
    stop("External clinical table is unavailable.", call. = FALSE)
  }
  clinical_symbol <- .imaging_safe_name(clinical_symbol, "clinical_symbol")
  clinical_id_col <- .feature_view_public_column(
    clinical_id_col, "clinical_id_col")
  clinical_columns <- .feature_view_public_columns(
    clinical_columns, "clinical_columns")
  target_col <- .feature_view_public_column(target_col, "target_col")
  target_levels <- if (is.null(target_levels)) NULL else {
    .feature_view_public_levels(target_levels)
  }

  if (!exists(clinical_symbol, envir = owner_env, inherits = FALSE) ||
      bindingIsActive(clinical_symbol, owner_env)) {
    stop("External clinical table is unavailable.", call. = FALSE)
  }
  clinical <- get(clinical_symbol, envir = owner_env, inherits = FALSE)
  if (!is.data.frame(clinical) || is.null(names(clinical)) ||
      anyNA(names(clinical)) || any(!nzchar(names(clinical))) ||
      anyDuplicated(names(clinical))) {
    stop("External clinical table is unavailable.", call. = FALSE)
  }

  source_contract <- authorized$privacy
  id_col <- source_contract$id_col
  patient_col <- source_contract$privacy_unit_col
  roles <- c(clinical_id_col, id_col, patient_col, target_col)
  if (target_col %in% c(clinical_id_col, id_col, patient_col) ||
      length(intersect(clinical_columns, roles)) > 0L) {
    stop("External clinical schema is unavailable.", call. = FALSE)
  }
  required <- unique(c(clinical_id_col, clinical_columns, target_col))
  if (any(!required %in% names(clinical))) {
    stop("External clinical schema is unavailable.", call. = FALSE)
  }

  one_dimensional <- vapply(clinical[required], function(value) {
    (is.atomic(value) || is.factor(value)) && is.null(dim(value)) &&
      length(value) == nrow(clinical)
  }, logical(1))
  if (!all(one_dimensional)) {
    stop("External clinical schema is unavailable.", call. = FALSE)
  }

  roster <- .validate_imaging_privacy_roster(authorized$privacy_roster)
  raw_clinical_ids <- tryCatch(
    as.character(clinical[[clinical_id_col]]),
    error = function(e) rep(NA_character_, nrow(clinical)))
  clinical_ids <- .canonical_imaging_privacy_ids(raw_clinical_ids)
  canonical <- !is.na(raw_clinical_ids) & !is.na(clinical_ids) &
    nzchar(clinical_ids) & raw_clinical_ids == clinical_ids
  canonical[is.na(canonical)] <- FALSE
  ambiguous <- duplicated(clinical_ids) |
    duplicated(clinical_ids, fromLast = TRUE)
  clinical_ids[!canonical | ambiguous] <- NA_character_

  target <- clinical[[target_col]]
  if (!is.factor(target) &&
      !(is.character(target) || is.logical(target) || is.numeric(target))) {
    stop("External clinical target is unavailable.", call. = FALSE)
  }

  data <- .imaging_load_asset(
    authorized, asset_id_or_alias, columns = NULL,
    include_metadata = FALSE, syntactic_names = FALSE)
  structural <- unique(c(
    id_col, patient_col, source_contract$label_col %||% character(0)))
  requested <- if (is.null(columns)) {
    setdiff(names(data), structural)
  } else {
    missing <- setdiff(columns, names(data))
    if (length(missing)) stop("Feature view columns are unavailable.")
    setdiff(columns, structural)
  }
  if (!length(requested) ||
      length(intersect(c(clinical_columns, target_col), requested)) > 0L) {
    stop("Feature view schema is unavailable.", call. = FALSE)
  }

  data <- data[, unique(c(id_col, requested)), drop = FALSE]
  sample_ids <- .canonical_imaging_privacy_ids(data[[id_col]])
  .assert_exact_imaging_roster(
    sample_ids, roster, context = "Imaging feature view")
  patient_map <- stats::setNames(roster$privacy_ids, roster$sample_ids)
  image_patient_ids <- unname(patient_map[sample_ids])
  match_index <- match(image_patient_ids, clinical_ids)
  for (column in clinical_columns) {
    data[[column]] <- clinical[[column]][match_index]
  }
  data[[target_col]] <- target[match_index]

  view_privacy <- source_contract
  view_privacy$label_col <- target_col
  view_privacy["label_levels"] <- list(target_levels)
  view_manifest <- authorized$manifest
  if (!is.list(view_manifest) || !is.list(view_manifest$metadata)) {
    stop("Imaging feature view manifest is unavailable.", call. = FALSE)
  }
  view_manifest$metadata$label_col <- target_col
  view_manifest$metadata$label_levels <- target_levels
  list(data = data, privacy = view_privacy, manifest = view_manifest)
}

#' Create an opaque, patient-bound feature view for dsFlower
#'
#' DataSHIELD ASSIGN method. Loads one complete feature asset under the
#' authority of an initialized dsImaging handle and retains the admitted patient
#' mapping in a private session registry. By default it joins only the manifest
#' label. An external, same-session clinical table can instead supply an explicit
#' target and approved covariates from one row per patient. The image roster
#' always remains authoritative: unmatched or ambiguous patient identifiers are
#' totalised as missing and extra clinical rows are ignored, so linkage cannot
#' subset the admitted image cohort or expose identifier membership through
#' success/failure. The assigned result contains only a high-entropy capability;
#' it is not a table and cannot be subset into a smaller cohort.
#'
#' @param handle_symbol Character; initialized imaging handle.
#' @param asset_id_or_alias Character; opaque asset id or server-side alias.
#' @param columns Optional public feature-column selection. Sample, label, and
#'   patient identity columns are retained by the server as structural fields.
#' @param clinical_symbol Optional character name of a data frame assigned in
#'   the same DataSHIELD session. If supplied, the remaining clinical arguments
#'   describe an external clinical table.
#' @param clinical_id_col Character; external table column containing one
#'   canonical patient identifier per row. It is matched to the sealed imaging
#'   privacy-unit roster, never to image sample identifiers.
#' @param clinical_columns Optional public covariate columns copied into the
#'   private feature view. Sample and patient identity columns are forbidden.
#' @param target_col Character; explicit external target column. It becomes the
#'   view's manifest-declared label without changing the source imaging handle.
#' @param target_levels Optional ordered public classification vocabulary. It is
#'   never inferred or validated against private target values.
#' @return An opaque feature-view reference for assignment in the same session.
#' @export
imagingFeatureViewDS <- function(handle_symbol, asset_id_or_alias,
                                 columns = NULL, clinical_symbol = NULL,
                                 clinical_id_col = "patient_id",
                                 clinical_columns = NULL, target_col = NULL,
                                 target_levels = NULL) {
  .dsimaging_require_literal_arguments()
  columns <- .decode_imaging_columns_arg(columns)
  owner_env <- parent.frame()
  asset_id_or_alias <- .imaging_resolve_asset_argument(asset_id_or_alias)
  if (!is.null(columns) &&
      (!is.character(columns) || !length(columns) || anyNA(columns) ||
       any(!nzchar(columns)) || anyDuplicated(columns))) {
    stop("columns must contain unique, non-empty public column names.",
         call. = FALSE)
  }
  columns <- if (is.null(columns)) NULL else enc2utf8(columns)

  tryCatch({
    authorized <- .authorized_imaging_dataset(
      handle_symbol, owner_env = owner_env)
    source_contract <- authorized$privacy
    if (is.null(clinical_symbol)) {
      unused_clinical_id <- !is.null(clinical_id_col) &&
        !identical(clinical_id_col, "patient_id")
      if (unused_clinical_id || !all(vapply(
          list(clinical_columns, target_col, target_levels), is.null,
          logical(1)))) {
        stop("External clinical table is unavailable.")
      }
      contract <- source_contract
      view_manifest <- authorized$manifest
      if (!is.list(contract) || is.null(contract$label_col)) {
        stop("Feature view has no declared label.")
      }
      data <- .imaging_load_asset(
        authorized, asset_id_or_alias, columns = NULL,
        include_metadata = TRUE, syntactic_names = FALSE)

      id_col <- contract$id_col
      label_col <- contract$label_col
      patient_col <- contract$privacy_unit_col
      requested <- if (is.null(columns)) {
        setdiff(names(data), c(id_col, label_col, patient_col))
      } else {
        missing <- setdiff(columns, names(data))
        if (length(missing)) stop("Feature view columns are unavailable.")
        setdiff(columns, c(id_col, label_col, patient_col))
      }
      if (!length(requested)) {
        stop("Feature view has no model feature columns.")
      }
      data <- data[, unique(c(id_col, requested, label_col)), drop = FALSE]
    } else {
      linked <- .external_clinical_feature_view(
        authorized, asset_id_or_alias, columns, clinical_symbol,
        clinical_id_col, clinical_columns, target_col, target_levels,
        owner_env)
      data <- linked$data
      contract <- linked$privacy
      view_manifest <- linked$manifest
      id_col <- contract$id_col
      patient_col <- contract$privacy_unit_col
    }

    sample_ids <- .canonical_imaging_privacy_ids(data[[id_col]])
    roster <- .validate_imaging_privacy_roster(authorized$privacy_roster)
    .assert_exact_imaging_roster(
      sample_ids, roster, context = "Imaging feature view")
    patient_map <- stats::setNames(roster$privacy_ids, roster$sample_ids)
    data[[patient_col]] <- unname(patient_map[sample_ids])
    .assert_exact_imaging_roster(
      sample_ids, roster, privacy_ids = data[[patient_col]],
      context = "Imaging feature view")

    capability <- .new_imaging_feature_view_capability()
    state <- .imaging_session_state(owner_env, create = TRUE)
    feature_views <- state$feature_views
    feature_views[[capability]] <- list(
      source_handle_symbol = as.character(handle_symbol),
      source_handle_capability = authorized$handle_capability,
      dataset_id = authorized$dataset_id,
      collection_seal = .imaging_authorized_collection_seal(authorized),
      source_privacy = source_contract,
      manifest = view_manifest,
      privacy = contract,
      privacy_roster = roster,
      data = data,
      data_sha256 = digest::digest(data, algo = "sha256", serialize = TRUE))
    structure(
      list(capability = capability),
      class = "dsimaging_feature_view_ref")
  }, error = function(e) {
    stop("Imaging feature view could not be created.", call. = FALSE)
  })
}

#' Resolve an opaque feature view for a trusted same-session consumer
#'
#' This is deliberately not a registered DataSHIELD method. Consumers must
#' resolve again after staging to close their read-time race window.
#' @keywords internal
.resolve_imaging_feature_view_for_consumer <- function(
    symbol, expected_capability = NULL, owner_env = NULL) {
  unavailable <- function() {
    stop("Unknown, stale, or cross-session imaging feature-view reference.",
         call. = FALSE)
  }
  if (!is.character(symbol) || length(symbol) != 1L || is.na(symbol) ||
      !nzchar(symbol) || !is.environment(owner_env) ||
      !exists(symbol, envir = owner_env, inherits = FALSE)) {
    unavailable()
  }
  if (bindingIsActive(symbol, owner_env)) unavailable()
  reference <- get(symbol, envir = owner_env, inherits = FALSE)
  if (!.is_imaging_feature_view_reference(reference)) {
    reference <- .imaging_export_reference(
      reference, owner_env, expected_capability)
  }
  if (!.is_imaging_feature_view_reference(reference)) unavailable()
  if (!is.null(expected_capability) &&
      (!is.character(expected_capability) ||
       length(expected_capability) != 1L || is.na(expected_capability) ||
       !identical(expected_capability, reference$capability))) {
    unavailable()
  }
  state <- tryCatch(
    .imaging_session_state(owner_env, create = FALSE),
    error = function(e) NULL)
  if (is.null(state)) unavailable()
  entry <- state$feature_views[[reference$capability]]
  if (!is.list(entry)) unavailable()

  authorized <- tryCatch(.authorized_imaging_dataset(
    entry$source_handle_symbol, owner_env = owner_env), error = function(e) NULL)
  current_seal <- if (is.null(authorized)) NULL else tryCatch(
    .imaging_authorized_collection_seal(authorized), error = function(e) NULL)
  current_data_hash <- if (is.data.frame(entry$data)) tryCatch(
    digest::digest(entry$data, algo = "sha256", serialize = TRUE),
    error = function(e) NULL) else NULL
  view_contract <- if (is.list(entry$manifest)) tryCatch(
    .imaging_privacy_contract(entry$manifest),
    error = function(e) NULL) else NULL
  if (is.null(authorized) ||
      !identical(authorized$handle_capability,
                 entry$source_handle_capability) ||
      !identical(authorized$dataset_id, entry$dataset_id) ||
      !identical(current_seal, entry$collection_seal) ||
      !.same_imaging_privacy_roster(
        authorized$privacy_roster, entry$privacy_roster) ||
      !identical(authorized$privacy, entry$source_privacy) ||
      !identical(view_contract, entry$privacy) ||
      !identical(current_data_hash, entry$data_sha256)) {
    unavailable()
  }
  contract <- entry$privacy
  if (any(!c(contract$id_col, contract$privacy_unit_col,
             contract$label_col) %in% names(entry$data))) {
    unavailable()
  }
  valid <- tryCatch({
    .assert_exact_imaging_roster(
      entry$data[[contract$id_col]], entry$privacy_roster,
      privacy_ids = entry$data[[contract$privacy_unit_col]],
      context = "Imaging feature view")
    TRUE
  }, error = function(e) FALSE)
  if (!valid) unavailable()

  list(
    data = entry$data,
    dataset_id = entry$dataset_id,
    manifest = entry$manifest,
    privacy = entry$privacy,
    privacy_roster = entry$privacy_roster,
    collection_seal = entry$collection_seal,
    feature_view_capability = reference$capability,
    source_handle_capability = entry$source_handle_capability)
}

#' Destroy an opaque imaging feature view
#'
#' DataSHIELD ASSIGN method. Removes the private feature table and invalidates
#' the capability owned by the current session.
#'
#' @param feature_view_symbol Character; assigned feature-view symbol.
#' @return The opaque reference as an invisible retry tombstone.
#' @export
imagingFeatureViewDestroyDS <- function(feature_view_symbol) {
  .dsimaging_require_literal_arguments()
  owner_env <- parent.frame()
  unavailable <- function() {
    stop("Unknown or unavailable imaging feature-view reference.",
         call. = FALSE)
  }
  if (!is.character(feature_view_symbol) ||
      length(feature_view_symbol) != 1L || is.na(feature_view_symbol) ||
      !nzchar(feature_view_symbol) ||
      !exists(feature_view_symbol, envir = owner_env, inherits = FALSE)) {
    unavailable()
  }
  reference <- get(feature_view_symbol, envir = owner_env, inherits = FALSE)
  if (!.is_imaging_feature_view_reference(reference) ||
      bindingIsLocked(feature_view_symbol, owner_env)) unavailable()
  state <- tryCatch(
    .imaging_session_state(owner_env, create = FALSE),
    error = function(e) NULL)
  if (is.null(state)) unavailable()
  entry <- state$feature_views[[reference$capability]]
  if (identical(entry, .dsimaging_session_tombstone)) {
    rm(list = feature_view_symbol, envir = owner_env)
    return(invisible(reference))
  }
  if (is.null(entry) || !is.list(entry)) unavailable()
  feature_views <- state$feature_views
  feature_views[[reference$capability]] <- .dsimaging_session_tombstone
  rm(list = feature_view_symbol, envir = owner_env)
  invisible(reference)
}
