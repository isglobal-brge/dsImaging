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

#' Check if a Derivation Already Exists (Deduplication)
#'
#' DataSHIELD AGGREGATE method. Given a derivation_hash, checks if an
#' identical asset already exists. Upstream packages (dsRadiomics) use
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
                                    alias = NULL) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))

  asset_id <- .asset_register(db, dataset_id, kind, path_or_root,
    derivation_hash = derivation_hash,
    parent_asset_ids = parent_asset_ids,
    provenance = provenance,
    created_by = created_by,
    created_by_job = created_by_job,
    description = description)

  if (!is.null(alias)) {
    .asset_set_alias(db, dataset_id, alias, asset_id)
  }

  asset_id
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
