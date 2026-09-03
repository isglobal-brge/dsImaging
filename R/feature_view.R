# Module: Opaque Imaging Feature Views
#
# Feature tables intended for dsFlower remain behind a session-bound capability.
# The private registry retains the complete admitted sample-to-patient roster;
# only the opaque reference is assigned into the DataSHIELD workspace.

# A naked feature table is intentionally still available to legacy
# DataSHIELD packages. Once one has entered a session, however, no trusted
# consumer can prove that a later data.frame is not a copied or row-subsetted
# derivative. Conservatively mark the whole owner environment for its lifetime.
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

#' Create an opaque, patient-bound feature view for dsFlower
#'
#' DataSHIELD ASSIGN method. Loads one complete feature asset under the
#' authority of an initialized dsImaging handle, joins only its manifest label,
#' and retains the admitted patient mapping in a private session registry. The
#' assigned result contains only a high-entropy capability; it is not a table
#' and cannot be subset into a smaller cohort.
#'
#' @param handle_symbol Character; initialized imaging handle.
#' @param asset_id_or_alias Character; opaque asset id or server-side alias.
#' @param columns Optional public feature-column selection. Sample, label, and
#'   patient identity columns are retained by the server as structural fields.
#' @return An opaque feature-view reference for assignment in the same session.
#' @export
imagingFeatureViewDS <- function(handle_symbol, asset_id_or_alias,
                                 columns = NULL) {
  .dsimaging_require_literal_arguments()
  columns <- .decode_imaging_columns_arg(columns)
  owner_env <- parent.frame()
  asset_id_or_alias <- .imaging_safe_name(
    asset_id_or_alias, "asset_id_or_alias")
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
    contract <- authorized$privacy
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
  reference <- get(symbol, envir = owner_env, inherits = FALSE)
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
  if (is.null(authorized) ||
      !identical(authorized$handle_capability,
                 entry$source_handle_capability) ||
      !identical(authorized$dataset_id, entry$dataset_id) ||
      !identical(current_seal, entry$collection_seal) ||
      !.same_imaging_privacy_roster(
        authorized$privacy_roster, entry$privacy_roster) ||
      !identical(authorized$privacy, entry$privacy) ||
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
    manifest = authorized$manifest,
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
