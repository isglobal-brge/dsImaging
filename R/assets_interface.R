# Module: Asset Catalog DataSHIELD Methods
# New aggregate methods for querying the asset catalog.

#' List Derived Assets for a Dataset
#'
#' DataSHIELD AGGREGATE method. Returns all active assets for a dataset,
#' optionally filtered by kind. Each asset has a unique asset_id,
#' derivation_hash, and lineage.
#'
#' @param dataset_id Character; the dataset identifier.
#' @param kind Character or NULL; filter by kind (e.g. "feature_table",
#'   "mask_root", "embedding_table").
#' @return Data.frame with asset catalog entries.
#' @export
imagingAssetCatalogDS <- function(dataset_id, kind = NULL) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  assets <- .asset_list(db, dataset_id, kind = kind)
  if (nrow(assets) == 0) {
    return(data.frame(
      asset_id = character(0), kind = character(0),
      derivation_hash = character(0), created_at = character(0),
      created_by = character(0), tags = character(0),
      stringsAsFactors = FALSE))
  }
  assets[, c("asset_id", "kind", "modality", "visibility", "derivation_hash",
             "created_at", "created_by", "created_by_job", "tags"), drop = FALSE]
}

#' Get Asset Details
#'
#' DataSHIELD AGGREGATE method. Returns full metadata for a specific asset,
#' including its provenance and manifest.
#'
#' @param asset_id_or_alias Character; asset_id or alias name.
#' @param dataset_id Character; required if using an alias.
#' @return Named list with asset details.
#' @export
imagingAssetDetailDS <- function(asset_id_or_alias, dataset_id = NULL) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))

  # Try direct asset_id first
  asset <- .asset_get(db, asset_id_or_alias)

  # Try as alias
  if (is.null(asset) && !is.null(dataset_id)) {
    resolved_id <- .asset_resolve_alias(db, dataset_id, asset_id_or_alias)
    if (!is.null(resolved_id)) asset <- .asset_get(db, resolved_id)
  }

  if (is.null(asset)) {
    stop("Asset '", asset_id_or_alias, "' not found.", call. = FALSE)
  }

  # Parse JSON fields
  provenance <- if (!is.na(asset$provenance_json))
    jsonlite::fromJSON(asset$provenance_json, simplifyVector = FALSE) else list()
  manifest <- if (!is.na(asset$manifest_json))
    jsonlite::fromJSON(asset$manifest_json, simplifyVector = FALSE) else list()

  # Get lineage
  lineage <- .asset_get_lineage(db, asset$asset_id)

  list(
    asset_id = asset$asset_id,
    dataset_id = asset$dataset_id,
    kind = asset$kind,
    modality = asset$modality,
    status = asset$status,
    visibility = asset$visibility,
    derivation_hash = asset$derivation_hash,
    created_at = asset$created_at,
    created_by = asset$created_by,
    created_by_job = asset$created_by_job,
    path_or_root = asset$path_or_root,
    tags = asset$tags,
    provenance = provenance,
    manifest = manifest,
    parents = if (nrow(lineage) > 0) lineage else NULL
  )
}

#' List Asset Aliases for a Dataset
#'
#' DataSHIELD AGGREGATE method.
#'
#' @param dataset_id Character; the dataset identifier.
#' @return Data.frame with alias, asset_id, updated_at.
#' @export
imagingAliasesDS <- function(dataset_id) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  .asset_list_aliases(db, dataset_id)
}

#' Get Asset Lineage
#'
#' DataSHIELD AGGREGATE method. Returns the derivation graph for an asset.
#'
#' @param asset_id Character; the asset identifier.
#' @return Data.frame with parent_asset_id, relationship, kind.
#' @export
imagingLineageDS <- function(asset_id) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  .asset_get_lineage(db, asset_id)
}

#' Load a Feature Asset as a DataSHIELD Table
#'
#' DataSHIELD ASSIGN method. Loads a published feature-table-like imaging
#' asset into the server session as a data.frame, after applying the same
#' minimum-row disclosure guard used for imaging resources.
#'
#' Supported asset kinds are \code{radiomics_collection},
#' \code{feature_table}, \code{qc_table}, and \code{embedding_table}.
#'
#' @param dataset_id Character; dataset identifier used for alias resolution
#'   and backend lookup.
#' @param asset_id_or_alias Character; asset id or dataset-level alias.
#' @param columns Optional character vector of columns to keep.
#' @param include_metadata Logical; if TRUE, left-join the dataset metadata
#'   table on `sample_id` so clinical/outcome columns travel with the features.
#' @param syntactic_names Logical; if TRUE, repair column names with
#'   \code{make.names(..., unique = TRUE)} for formula-based DataSHIELD models.
#' @return A data.frame for assignment in the DataSHIELD session.
#' @export
imagingLoadAssetDS <- function(dataset_id, asset_id_or_alias, columns = NULL,
                               include_metadata = FALSE,
                               syntactic_names = FALSE) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))

  asset <- .resolve_asset_by_id_or_alias(db, dataset_id, asset_id_or_alias)
  if (is.null(asset)) {
    stop("Asset '", asset_id_or_alias, "' not found for dataset '",
         dataset_id, "'.", call. = FALSE)
  }

  supported <- c("radiomics_collection", "feature_table", "qc_table",
                 "embedding_table")
  if (!asset$kind %in% supported) {
    stop("Asset '", asset_id_or_alias, "' is kind '", asset$kind,
         "', not a loadable feature table.", call. = FALSE)
  }

  df <- .read_feature_asset(asset, dataset_id)
  if (isTRUE(include_metadata)) {
    df <- .join_feature_asset_metadata(df, dataset_id)
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

  .assert_min_samples(nrow(df),
    context = paste0("asset '", asset_id_or_alias, "'"))
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
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  existing <- .asset_find_by_hash(db, dataset_id, derivation_hash)
  list(
    exists = !is.null(existing),
    asset_id = existing
  )
}

#' Register a Derived Asset (called by dsJobs publisher plugin)
#'
#' NOT a DataSHIELD method -- called server-side by the dsJobs publisher
#' plugin for imaging assets.
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
                                    alias = NULL) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))

  asset_id <- .asset_register(db, dataset_id, kind, path_or_root,
    derivation_hash = derivation_hash,
    parent_asset_ids = parent_asset_ids,
    provenance = provenance,
    created_by = created_by,
    created_by_job = created_by_job,
    storage_backend = storage_backend,
    description = description)

  if (!is.null(alias)) {
    .asset_set_alias(db, dataset_id, alias, asset_id)
  }

  asset_id
}

#' Resolve a feature_table asset by alias or ID
#'
#' Returns the asset metadata needed by dsFlower to consume
#' the feature table directly without materializing it in R.
#'
#' @param dataset_id Character.
#' @param alias_or_id Character; alias name or asset_id.
#' @return Named list: asset_id, uri (path_or_root), storage_backend,
#'   derivation_hash, dataset_id.
#' @export
resolve_feature_table_asset <- function(dataset_id, alias_or_id) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))

  asset <- .resolve_asset_by_id_or_alias(db, dataset_id, alias_or_id)
  if (is.null(asset))
    stop("Asset not found: ", alias_or_id, call. = FALSE)
  if (!asset$kind %in% c("feature_table", "radiomics_collection"))
    stop("Asset '", alias_or_id, "' is kind '", asset$kind,
         "', not a feature table.", call. = FALSE)

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
.resolve_asset_by_id_or_alias <- function(db, dataset_id, asset_id_or_alias) {
  asset <- .asset_get(db, asset_id_or_alias)
  if (!is.null(asset)) return(asset)

  resolved_id <- .asset_resolve_alias(db, dataset_id, asset_id_or_alias)
  if (is.null(resolved_id)) return(NULL)
  .asset_get(db, resolved_id)
}

#' @keywords internal
.read_feature_asset <- function(asset, dataset_id) {
  root <- asset$path_or_root
  if (is.null(root) || is.na(root) || !nzchar(root))
    stop("Asset has no path_or_root.", call. = FALSE)

  path <- .resolve_feature_asset_file(root, dataset_id)
  if (grepl("^s3://", path)) {
    resolved <- .resolve_ds(dataset_id)
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
.join_feature_asset_metadata <- function(df, dataset_id) {
  meta <- .read_dataset_metadata(dataset_id)
  if (is.null(meta)) {
    stop("Dataset metadata could not be resolved for dataset '", dataset_id,
         "'. Call imagingInitDS() first or register the dataset.",
         call. = FALSE)
  }
  if (!"sample_id" %in% names(df)) {
    stop("Feature asset does not contain a sample_id column.", call. = FALSE)
  }
  if (!"sample_id" %in% names(meta)) {
    stop("Dataset metadata does not contain a sample_id column.",
         call. = FALSE)
  }

  meta_cols <- c("sample_id", setdiff(names(meta), names(df)))
  meta <- meta[, meta_cols, drop = FALSE]
  merged <- merge(df, meta, by = "sample_id", all.x = TRUE, sort = FALSE)
  merged[match(df$sample_id, merged$sample_id), , drop = FALSE]
}

#' @keywords internal
.read_dataset_metadata <- function(dataset_id) {
  context <- .resolve_dataset_metadata_context(dataset_id)
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

#' @keywords internal
.resolve_dataset_metadata_context <- function(dataset_id) {
  resolved <- tryCatch(.resolve_ds(dataset_id), error = function(e) NULL)
  if (!is.null(resolved) && !is.null(resolved$manifest)) {
    return(list(manifest = resolved$manifest, backend = resolved$backend))
  }

  manifest <- tryCatch(imagingGetManifestDS(dataset_id), error = function(e) NULL)
  if (is.null(manifest)) {
    manifest <- tryCatch(imagingGetManifestDS("img"), error = function(e) NULL)
  }
  if (is.null(manifest)) return(NULL)

  backend <- tryCatch(imagingGetBackendDS(dataset_id), error = function(e) NULL)
  if (is.null(backend)) {
    backend <- tryCatch(imagingGetBackendDS("img"), error = function(e) NULL)
  }
  list(manifest = manifest, backend = backend)
}

#' @keywords internal
.resolve_feature_asset_file <- function(root, dataset_id) {
  if (grepl("\\.(parquet|csv)$", root, ignore.case = TRUE)) return(root)

  candidates <- c("radiomics_features.parquet", "features.parquet",
                  "feature_table.parquet", "radiomics_features.csv",
                  "features.csv", "feature_table.csv")

  if (grepl("^s3://", root)) {
    resolved <- .resolve_ds(dataset_id)
    if (is.null(resolved) || is.null(resolved$backend)) {
      stop("Cannot resolve storage backend for S3 asset: ", root,
           call. = FALSE)
    }
    prefix <- sub("/$", "", root)
    for (candidate in candidates) {
      uri <- paste0(prefix, "/", candidate)
      head <- backend_head(resolved$backend, uri)
      if (!is.null(head) && isTRUE(head$exists)) return(uri)
    }
    listed <- backend_list(resolved$backend, paste0(prefix, "/"))
    hits <- listed[grepl("\\.(parquet|csv)$", listed, ignore.case = TRUE)]
    if (length(hits) > 0) return(hits[1])
    stop("No parquet/csv feature table found under asset root: ", root,
         call. = FALSE)
  }

  if (!dir.exists(root))
    stop("Asset path does not exist: ", root, call. = FALSE)
  for (candidate in candidates) {
    path <- file.path(root, candidate)
    if (file.exists(path)) return(path)
  }
  hits <- list.files(root, pattern = "\\.(parquet|csv)$", full.names = TRUE,
                     recursive = TRUE, ignore.case = TRUE)
  if (length(hits) > 0) return(hits[1])
  stop("No parquet/csv feature table found under asset root: ", root,
       call. = FALSE)
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
