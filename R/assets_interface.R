# Module: Asset Catalog DataSHIELD Methods
# New aggregate methods for querying the asset catalog.

#' List Derived Assets for a Dataset
#'
#' DataSHIELD AGGREGATE method. Returns all active assets for a dataset,
#' optionally filtered by kind. Each asset has a unique asset_id,
#' derivation_hash, and lineage.
#'
#' @param handle_symbol Character; initialized imaging handle.
#' @param kind Character or NULL; filter by kind (e.g. "feature_table",
#'   "mask_root", "embedding_table").
#' @return Data.frame with asset catalog entries.
#' @export
imagingAssetCatalogDS <- function(handle_symbol, kind = NULL) {
  .dsimaging_require_literal_arguments()
  authorized <- .authorized_imaging_dataset(
    handle_symbol, owner_env = parent.frame())
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  collection_seal <- .imaging_authorized_collection_seal(authorized)
  assets <- .asset_list(db, authorized$dataset_id, kind = kind,
                        visibility = "global",
                        collection_seal = collection_seal)
  if (nrow(assets) > 0L) {
    valid_id <- !is.na(assets$asset_id) &
      grepl("^asset_[0-9a-f]{32}$", assets$asset_id)
    assets <- assets[valid_id, , drop = FALSE]
  }
  if (nrow(assets) == 0) {
    return(data.frame(
      asset_id = character(0), kind = character(0),
      modality = character(0),
      stringsAsFactors = FALSE))
  }
  data.frame(
    asset_id = unname(assets$asset_id),
    kind = unname(vapply(
      assets$kind, .safe_public_identifier, character(1),
      default = "unknown")),
    modality = unname(vapply(
      assets$modality, .safe_public_identifier, character(1),
      default = "unknown")),
    stringsAsFactors = FALSE)
}

#' Get Asset Details
#'
#' Retired DataSHIELD method. Detailed asset metadata, provenance, storage
#' locations, and manifests are node-private and are never returned to analysts.
#'
#' @param handle_symbol Character; initialized imaging handle.
#' @param asset_id_or_alias Character; opaque asset reference or public alias.
#' @return This function always errors.
#' @export
imagingAssetDetailDS <- function(handle_symbol, asset_id_or_alias) {
  .legacy_imaging_ds_disabled("imagingAssetDetailDS")
}

#' List Asset Aliases for a Dataset
#'
#' Retired DataSHIELD method. Alias history is node-private.
#'
#' @param handle_symbol Character; initialized imaging handle.
#' @return This function always errors.
#' @export
imagingAliasesDS <- function(handle_symbol) {
  .legacy_imaging_ds_disabled("imagingAliasesDS")
}

#' Get Asset Lineage
#'
#' DataSHIELD AGGREGATE method. Returns the derivation graph for an asset.
#'
#' @param asset_id Character; the asset identifier.
#' @return This function always errors; detailed lineage remains node-private.
#' @export
imagingLineageDS <- function(asset_id) {
  .legacy_imaging_ds_disabled("imagingLineageDS")
}

#' Load a Feature Asset as a DataSHIELD Table
#'
#' DataSHIELD ASSIGN method. Loads a published feature-table-like imaging
#' asset into the server session as a data.frame, after applying the same
#' minimum-row disclosure guard used for imaging resources.
#'
#' Supported asset kinds are \code{radiomics_collection},
#' \code{feature_table}, \code{qc_table}, \code{dose_table}, and
#' \code{embedding_table}.
#'
#' @param handle_symbol Character; initialized imaging handle.
#' @param asset_id_or_alias Character; asset id or dataset-level alias.
#' @param columns Optional character vector of columns to keep.
#' @param include_metadata Logical; if TRUE, join only the manifest-declared
#'   \code{metadata.label_col} on \code{metadata.id_col}. Undeclared metadata
#'   columns are never copied into the analysis table.
#' @param syntactic_names Logical; if TRUE, repair column names with
#'   \code{make.names(..., unique = TRUE)} for formula-based DataSHIELD models.
#' @return A data.frame for assignment in the DataSHIELD session.
#' @export
imagingLoadAssetDS <- function(handle_symbol, asset_id_or_alias, columns = NULL,
                               include_metadata = FALSE,
                               syntactic_names = FALSE) {
  .dsimaging_require_literal_arguments()
  columns <- .decode_imaging_columns_arg(columns)
  owner_env <- parent.frame()
  authorized <- .authorized_imaging_dataset(
    handle_symbol, owner_env = owner_env)
  data <- .imaging_load_asset(authorized, asset_id_or_alias, columns,
                              include_metadata, syntactic_names)
  .mark_imaging_feature_table_export(owner_env)
  data
}

#' Decode a public column selection carried as one literal argument
#' @keywords internal
.decode_imaging_columns_arg <- function(columns) {
  if (is.null(columns)) return(NULL)
  if (is.character(columns) && length(columns) == 1L &&
      !is.na(columns) && startsWith(columns, "B64:")) {
    columns <- .dsr_decode(columns)
  }
  if (is.list(columns)) {
    valid <- vapply(columns, function(value) {
      is.character(value) && length(value) == 1L && !is.na(value)
    }, logical(1))
    if (!all(valid)) {
      stop("columns must contain only public column names.", call. = FALSE)
    }
    columns <- unname(vapply(columns, as.character, character(1)))
  }
  if (!is.character(columns) || anyNA(columns)) {
    stop("columns must contain only public column names.", call. = FALSE)
  }
  enc2utf8(columns)
}

#' @keywords internal
.imaging_load_asset <- function(authorized, asset_id_or_alias, columns = NULL,
                                include_metadata = FALSE,
                                syntactic_names = FALSE) {
  dataset_id <- authorized$dataset_id
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))

  asset <- .resolve_asset_by_id_or_alias(
    db, dataset_id, asset_id_or_alias, authorized = authorized)
  if (is.null(asset)) {
    stop("Asset '", asset_id_or_alias, "' not found for dataset '",
         dataset_id, "'.", call. = FALSE)
  }

  supported <- c("radiomics_collection", "feature_table", "qc_table",
                 "dose_table", "embedding_table")
  if (!asset$kind %in% supported) {
    stop("Requested imaging asset is not a loadable feature table.",
         call. = FALSE)
  }

  df <- tryCatch(
    .read_feature_asset(asset, dataset_id, resolved = authorized),
    error = function(e) stop("Imaging feature asset is unavailable.",
                             call. = FALSE))
  .assert_feature_asset_privacy(df, authorized,
    context = "imaging feature asset")
  if (isTRUE(include_metadata)) {
    df <- tryCatch(
      .join_feature_asset_metadata(df, dataset_id, resolved = authorized),
      error = function(e) stop("Imaging dataset metadata is unavailable.",
                               call. = FALSE))
  }
  if (!is.null(columns)) {
    columns <- as.character(columns)
    missing <- setdiff(columns, names(df))
    if (length(missing) > 0) {
      stop("Requested columns not found: ", paste(missing, collapse = ", "),
           call. = FALSE)
    }
    df <- df[, columns, drop = FALSE]
  }

  if (isTRUE(syntactic_names)) {
    names(df) <- make.names(names(df), unique = TRUE)
  }
  df
}

#' Check if a Derivation Already Exists (Deduplication)
#'
#' DataSHIELD AGGREGATE method. Given a derivation_hash, checks if an
#' identical asset already exists. Domain publishers use
#' this to avoid recomputation.
#'
#' @param dataset_id Character; the dataset identifier.
#' @param derivation_hash Character; the content-address hash.
#' @return Named list with exists (logical) and asset_id (if found).
#' @export
imagingDeduplicateDS <- function(dataset_id, derivation_hash) {
  .legacy_imaging_ds_disabled("imagingDeduplicateDS")
}

#' Register a Derived Asset (called by dsHPC publisher plugin)
#'
#' NOT a DataSHIELD method -- called server-side by the dsHPC publisher
#' plugin for imaging assets. Direct analyst calls are rejected: the supported
#' path is dsHPC invoking dsImaging's `.radiomics_publisher()` or
#' `.imaging_asset_publisher()`, which validates the complete output before
#' registering it from the dsImaging namespace.
#'
#' @param dataset_id Character.
#' @param kind Character.
#' @param path_or_root Character.
#' @param derivation_hash Character or NULL.
#' @param parent_asset_ids Character vector.
#' @param provenance Named list.
#' @param created_by Character.
#' @param created_by_job Character or NULL.
#' @param description Character or NULL; human-readable asset description.
#' @param storage_backend Character; storage backend label.
#' @param alias Character or NULL; optional alias to set.
#' @param visibility Character; either `"private"` or `"global"`.
#' @param collection_seal Character; immutable collection-snapshot seal.
#' @return Character; the asset_id.
#' @export
register_derived_asset <- function(dataset_id, kind, path_or_root,
                                    derivation_hash = NULL,
                                    parent_asset_ids = character(0),
                                    provenance = NULL,
                                    created_by = NULL,
                                    created_by_job = NULL,
                                    description = NULL,
                                    storage_backend = "file",
                                    alias = NULL,
                                    visibility = "private",
                                    collection_seal = NULL) {
  .dsimaging_require_publisher_caller()
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))

  asset_id <- .asset_register(db, dataset_id, kind, path_or_root,
    derivation_hash = derivation_hash,
    parent_asset_ids = parent_asset_ids,
    provenance = provenance,
    created_by = created_by,
    created_by_job = created_by_job,
    collection_seal = collection_seal,
    storage_backend = storage_backend,
    description = description,
    visibility = visibility)

  if (!is.null(alias) && identical(visibility, "global")) {
    .asset_set_alias(db, dataset_id, alias, asset_id)
  }

  asset_id
}

#' Resolve a feature_table asset by alias or ID
#'
#' Returns the asset metadata needed by downstream consumers to use the feature
#' table directly without materializing it in R.
#'
#' @param dataset_id Character.
#' @param alias_or_id Character; alias name or asset_id.
#' @param collection_seal Character; immutable collection-snapshot seal.
#' @return Named list: asset_id, uri (path_or_root), storage_backend,
#'   derivation_hash, dataset_id.
#' @export
resolve_feature_table_asset <- function(dataset_id, alias_or_id,
                                        collection_seal) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))

  asset <- .resolve_asset_by_id_or_alias(db, dataset_id, alias_or_id,
    collection_seal = .asset_collection_seal(
      collection_seal, required = TRUE))
  if (is.null(asset))
    stop("Asset not found: ", alias_or_id, call. = FALSE)
  if (!asset$kind %in% c("feature_table", "radiomics_collection"))
    stop("Requested imaging asset is not a feature table.", call. = FALSE)

  list(
    asset_id = asset$asset_id,
    uri = asset$path_or_root,
    storage_backend = asset$storage_backend %||% "file",
    derivation_hash = asset$derivation_hash,
    dataset_id = dataset_id
  )
}

#' Promote an Alias
#'
#' NOT a DataSHIELD method -- admin/server-side utility.
#'
#' @param dataset_id Character.
#' @param alias Character; the alias name.
#' @param asset_id Character; the asset to point to.
#' @export
promote_asset_alias <- function(dataset_id, alias, asset_id) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  .asset_set_alias(db, dataset_id, alias, asset_id)
}

#' @keywords internal
.resolve_asset_by_id_or_alias <- function(db, dataset_id, asset_id_or_alias,
                                          authorized = NULL,
                                          collection_seal = NULL) {
  if (!is.null(authorized)) {
    collection_seal <- .imaging_authorized_collection_seal(authorized)
  } else {
    collection_seal <- .asset_collection_seal(collection_seal)
  }
  if (is.null(collection_seal)) return(NULL)
  asset <- .asset_get(db, asset_id_or_alias)
  if (!is.null(asset)) {
    if (!identical(asset$dataset_id, dataset_id) ||
        !identical(asset$collection_seal, collection_seal) ||
        !identical(asset$status, "active")) return(NULL)
    if (!identical(asset$visibility, "global")) {
      if (!identical(asset$visibility, "private") || is.null(authorized) ||
          !grepl("^asset_[0-9a-f]{32}$", asset_id_or_alias)) return(NULL)
      capability_authorized <- .private_imaging_asset_is_authorized(
        asset$asset_id, authorized$owner_env)
      if (!capability_authorized) return(NULL)
    }
    return(asset)
  }

  resolved_id <- .asset_resolve_alias(
    db, dataset_id, asset_id_or_alias, collection_seal = collection_seal)
  if (is.null(resolved_id)) return(NULL)
  .asset_get(db, resolved_id)
}

#' @keywords internal
.read_feature_asset <- function(asset, dataset_id, resolved = NULL) {
  root <- asset$path_or_root
  if (is.null(root) || is.na(root) || !nzchar(root))
    stop("Asset has no path_or_root.", call. = FALSE)

  path <- .resolve_feature_asset_file(root, dataset_id, resolved = resolved)
  if (grepl("^s3://", path)) {
    if (is.null(resolved)) resolved <- .resolve_ds(dataset_id)
    if (is.null(resolved) || is.null(resolved$backend)) {
      stop("Cannot resolve storage backend for S3 asset: ", path,
           call. = FALSE)
    }
    ext <- if (grepl("\\.parquet$", path, ignore.case = TRUE)) ".parquet" else ".csv"
    tmp <- tempfile(fileext = ext)
    on.exit(unlink(tmp), add = TRUE)
    backend_get_file(resolved$backend, path, tmp)
    path <- tmp
  }

  .read_table_file(path)
}

#' @keywords internal
.join_feature_asset_metadata <- function(df, dataset_id, resolved = NULL) {
  meta <- .read_dataset_metadata(dataset_id, resolved = resolved)
  if (is.null(meta)) {
    stop("Dataset metadata could not be resolved for dataset '", dataset_id,
         "'. Call imagingInitDS() first or register the dataset.",
         call. = FALSE)
  }
  manifest <- resolved$manifest %||% NULL
  contract <- if (!is.null(resolved$privacy)) {
    resolved$privacy
  } else {
    .imaging_privacy_contract(manifest)
  }
  id_col <- contract$id_col
  label_col <- contract$label_col %||% NULL
  if (is.null(label_col)) {
    stop("Dataset manifest does not declare a clinical label column.",
         call. = FALSE)
  }
  if (!id_col %in% names(df)) {
    stop("Feature asset does not contain its declared identifier column.",
         call. = FALSE)
  }
  if (any(!c(id_col, label_col) %in% names(meta))) {
    stop("Dataset metadata does not contain its declared clinical columns.",
         call. = FALSE)
  }
  ids <- .validate_imaging_privacy_metadata(
    meta, contract, require_label = TRUE)
  current_roster <- .new_imaging_privacy_roster(
    ids$sample_ids, ids$privacy_ids)
  if (is.null(resolved$privacy_roster) ||
      !.same_imaging_privacy_roster(
        resolved$privacy_roster, current_roster)) {
    stop("Dataset metadata no longer matches its admitted privacy roster.",
         call. = FALSE)
  }

  feature_ids <- .canonical_imaging_privacy_ids(df[[id_col]])
  .assert_exact_imaging_roster(feature_ids, resolved$privacy_roster,
    context = "Imaging feature asset")
  match_index <- match(feature_ids, ids$sample_ids)
  if (anyNA(match_index)) {
    stop("Dataset metadata does not match the admitted feature roster.",
         call. = FALSE)
  }
  # A generated table must never override the manifest-authoritative label.
  if (label_col %in% names(df)) df[[label_col]] <- NULL
  df[[label_col]] <- meta[[label_col]][match_index]
  df
}

#' @keywords internal
.read_dataset_metadata <- function(dataset_id, resolved = NULL) {
  context <- if (is.null(resolved)) {
    .resolve_dataset_metadata_context(dataset_id)
  } else {
    list(manifest = resolved$manifest, backend = resolved$backend)
  }
  if (is.null(context) || is.null(context$manifest)) return(NULL)

  meta <- context$manifest$metadata
  if (is.null(meta)) return(NULL)

  local_file <- meta$file
  if (!is.null(local_file) && file.exists(local_file)) {
    return(.read_table_file(local_file))
  }

  uri <- meta$uri
  if (!is.null(uri) && grepl("^s3://", uri) && !is.null(context$backend)) {
    fmt <- meta$format %||% .guess_format(uri)
    tmp <- tempfile(fileext = paste0(".", fmt))
    on.exit(unlink(tmp), add = TRUE)
    backend_get_file(context$backend, uri, tmp)
    return(.read_table_file(tmp))
  }

  if (!is.null(uri) && file.exists(uri)) {
    return(.read_table_file(uri))
  }

  NULL
}

#' Require a feature asset to represent the complete admitted collection and
#' at least the configured number of distinct patient privacy units.
#' @keywords internal
.assert_feature_asset_privacy <- function(df, authorized, context) {
  contract <- authorized$privacy
  roster <- .validate_imaging_privacy_roster(authorized$privacy_roster)
  feature_names <- names(df)
  safe_names <- if (is.null(feature_names)) character(0) else vapply(
    feature_names, .safe_public_identifier, character(1),
    default = NA_character_)
  identifiers <- unique(c(roster$sample_ids, roster$privacy_units))
  if (length(feature_names) == 0L || anyNA(safe_names) ||
      anyDuplicated(feature_names) ||
      !identical(unname(safe_names), unname(feature_names)) ||
      any(feature_names %in% identifiers)) {
    stop("Imaging feature asset schema is unavailable.", call. = FALSE)
  }
  current <- .imaging_privacy_admission(
    authorized$manifest, authorized$backend)
  if (!.same_imaging_privacy_roster(roster, current$roster)) {
    stop("Imaging dataset no longer matches its admitted privacy roster.",
         call. = FALSE)
  }
  sample_map <- stats::setNames(roster$privacy_ids, roster$sample_ids)

  if (!contract$id_col %in% names(df)) {
    stop("Imaging feature asset cannot be mapped to patient privacy units.",
         call. = FALSE)
  }
  asset_ids <- .canonical_imaging_privacy_ids(df[[contract$id_col]])
  .assert_exact_imaging_roster(asset_ids, roster,
    context = "Imaging feature asset")
  allowed_metadata <- c(contract$id_col, contract$label_col %||% character(0))
  undeclared_clinical <- setdiff(names(current$data), allowed_metadata)
  if (length(intersect(names(df), undeclared_clinical)) > 0L) {
    stop("Imaging feature asset contains undeclared clinical columns.",
         call. = FALSE)
  }
  if (!is.null(contract$label_col) && contract$label_col %in% names(df)) {
    metadata_ids <- .canonical_imaging_privacy_ids(
      current$data[[contract$id_col]])
    match_index <- match(asset_ids, metadata_ids)
    asset_labels <- as.character(df[[contract$label_col]])
    metadata_labels <- as.character(
      current$data[[contract$label_col]][match_index])
    same_labels <- length(asset_labels) == length(metadata_labels) &&
      all((is.na(asset_labels) & is.na(metadata_labels)) |
          (!is.na(asset_labels) & !is.na(metadata_labels) &
           asset_labels == metadata_labels))
    if (!isTRUE(same_labels)) {
      stop("Imaging feature asset label does not match dataset metadata.",
           call. = FALSE)
    }
  }
  privacy_ids <- unname(sample_map[asset_ids])
  if (anyNA(privacy_ids) || any(!nzchar(privacy_ids))) {
    stop("Imaging feature asset cannot be mapped to patient privacy units.",
         call. = FALSE)
  }
  multiplicity <- vapply(roster$privacy_units, function(id) {
    sum(privacy_ids == id)
  }, integer(1))
  if (!identical(unname(as.integer(multiplicity)),
                 roster$unit_multiplicity)) {
    stop("Imaging feature asset is not the complete admitted collection.",
         call. = FALSE)
  }
  .assert_min_privacy_units(length(unique(privacy_ids)), context = context)
}

#' @keywords internal
.resolve_dataset_metadata_context <- function(dataset_id) {
  resolved <- tryCatch(.resolve_ds(dataset_id), error = function(e) NULL)
  if (!is.null(resolved) && !is.null(resolved$manifest)) {
    return(list(manifest = resolved$manifest, backend = resolved$backend))
  }
  NULL
}

#' @keywords internal
.resolve_feature_asset_file <- function(root, dataset_id, resolved = NULL) {
  if (!is.character(root) || length(root) != 1L || is.na(root) ||
      !nzchar(root) || grepl("[\r\n]", root)) {
    stop("Feature asset location is invalid.", call. = FALSE)
  }
  if (grepl("\\.(parquet|csv)$", root, ignore.case = TRUE)) {
    if (!grepl("^s3://", root) &&
        (!file.exists(root) || dir.exists(root) || nzchar(Sys.readlink(root)))) {
      stop("Feature asset file is unavailable.", call. = FALSE)
    }
    return(root)
  }

  if (grepl("^s3://", root)) {
    if (is.null(resolved)) resolved <- .resolve_ds(dataset_id)
    if (is.null(resolved) || is.null(resolved$backend)) {
      stop("Cannot resolve storage backend for S3 asset: ", root,
           call. = FALSE)
    }
    prefix <- paste0(sub("/+$", "", root), "/")
    listed <- backend_list(resolved$backend, prefix)
    hits <- unique(listed[
      startsWith(listed, prefix) &
      grepl("\\.(parquet|csv)$", listed, ignore.case = TRUE)
    ])
    if (length(hits) != 1L) {
      stop("Feature asset must contain exactly one table.", call. = FALSE)
    }
    return(hits[[1L]])
  }

  if (!dir.exists(root) || nzchar(Sys.readlink(root))) {
    stop("Feature asset directory is unavailable.", call. = FALSE)
  }
  canonical_root <- normalizePath(root, winslash = "/", mustWork = TRUE)
  entries <- list.files(root, all.files = TRUE, no.. = TRUE,
    recursive = TRUE, full.names = TRUE, include.dirs = TRUE)
  links <- Sys.readlink(c(root, entries))
  if (any(!is.na(links) & nzchar(links))) {
    stop("Feature asset contains a symbolic link.", call. = FALSE)
  }
  hits <- entries[
    !dir.exists(entries) &
    grepl("\\.(parquet|csv)$", entries, ignore.case = TRUE)
  ]
  if (length(hits) != 1L) {
    stop("Feature asset must contain exactly one table.", call. = FALSE)
  }
  candidate <- normalizePath(hits[[1L]], winslash = "/", mustWork = TRUE)
  if (!startsWith(candidate, paste0(sub("/+$", "", canonical_root), "/"))) {
    stop("Feature asset table is outside its asset root.", call. = FALSE)
  }
  candidate
}

#' @keywords internal
.read_table_file <- function(path) {
  if (grepl("\\.parquet$", path, ignore.case = TRUE)) {
    if (!requireNamespace("arrow", quietly = TRUE))
      stop("arrow package required to read parquet feature assets.",
           call. = FALSE)
    return(as.data.frame(arrow::read_parquet(path)))
  }
  if (grepl("\\.csv$", path, ignore.case = TRUE)) {
    return(utils::read.csv(path, stringsAsFactors = FALSE,
                           check.names = FALSE))
  }
  stop("Unsupported feature table format: ", path, call. = FALSE)
}
