# Module: Asset Catalog (SQLite)
# Manages derived assets: masks, radiomics tables, embeddings, etc.
# Each dataset can have multiple assets of the same kind with different
# parameterizations. Assets are immutable and content-addressed via
# derivation_hash. Aliases provide human-friendly names.

#' Open the asset catalog database
#'
#' @return A DBI connection.
#' @keywords internal
.asset_db_connect <- function() {
  db_path <- .asset_db_path()
  first_time <- !file.exists(db_path)

  db <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  DBI::dbExecute(db, "PRAGMA journal_mode=WAL")
  DBI::dbExecute(db, "PRAGMA busy_timeout=5000")
  DBI::dbExecute(db, "PRAGMA foreign_keys=ON")

  if (first_time) .asset_db_create_schema(db)
  db
}

#' Get path to the asset catalog database
#' @keywords internal
.asset_db_path <- function() {
  path <- getOption("dsimaging.asset_db",
            getOption("default.dsimaging.asset_db", NULL))
  if (!is.null(path)) return(path)

  # Default: alongside the registry
  reg_path <- getOption("dsimaging.registry_path",
                getOption("default.dsimaging.registry_path", NULL))
  if (!is.null(reg_path))
    return(file.path(dirname(reg_path), "imaging_assets.sqlite"))

  # Final fallback
  "/var/lib/dsimaging/imaging_assets.sqlite"
}

#' Create the asset catalog schema
#' @keywords internal
.asset_db_create_schema <- function(db) {
  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS assets (
      asset_id         TEXT PRIMARY KEY,
      dataset_id       TEXT NOT NULL,
      kind             TEXT NOT NULL,
      modality         TEXT,
      status           TEXT NOT NULL DEFAULT 'active',
      visibility       TEXT NOT NULL DEFAULT 'global',
      derivation_hash  TEXT,
      created_at       TEXT NOT NULL,
      created_by       TEXT,
      created_by_job   TEXT,
      path_or_root     TEXT,
      manifest_json    TEXT,
      provenance_json  TEXT,
      tags             TEXT
    )")

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS asset_parents (
      asset_id        TEXT NOT NULL,
      parent_asset_id TEXT NOT NULL,
      relationship    TEXT DEFAULT 'derived_from',
      PRIMARY KEY (asset_id, parent_asset_id),
      FOREIGN KEY (asset_id) REFERENCES assets(asset_id),
      FOREIGN KEY (parent_asset_id) REFERENCES assets(asset_id)
    )")

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS asset_aliases (
      dataset_id TEXT NOT NULL,
      alias      TEXT NOT NULL,
      asset_id   TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (dataset_id, alias),
      FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
    )")

  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_assets_dataset ON assets(dataset_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_assets_hash ON assets(derivation_hash)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_assets_kind ON assets(dataset_id, kind)")
}

#' @keywords internal
.asset_db_close <- function(db) {
  tryCatch(DBI::dbDisconnect(db), error = function(e) NULL)
}

# --- CRUD operations ---

#' Register a new asset in the catalog
#'
#' Assets are immutable. If an asset with the same derivation_hash already
#' exists for the dataset, returns the existing asset_id instead of creating
#' a duplicate.
#'
#' @param dataset_id Character; parent dataset.
#' @param kind Character; asset kind (image_root, mask_root, feature_table, etc.).
#' @param path_or_root Character; filesystem path to the asset.
#' @param derivation_hash Character or NULL; content-address hash.
#' @param parent_asset_ids Character vector; asset_ids this was derived from.
#' @param provenance Named list; how this asset was produced.
#' @param created_by Character; who created it.
#' @param created_by_job Character or NULL; job_id that produced it.
#' @param modality Character or NULL.
#' @param visibility Character; "global" or "private".
#' @param tags Character or NULL; comma-separated tags.
#' @param manifest Named list or NULL; asset-specific manifest.
#' @return Character; the asset_id (new or existing).
#' @keywords internal
.asset_register <- function(db, dataset_id, kind, path_or_root,
                             derivation_hash = NULL,
                             parent_asset_ids = character(0),
                             provenance = NULL,
                             created_by = NULL,
                             created_by_job = NULL,
                             modality = NULL,
                             visibility = "global",
                             tags = NULL,
                             manifest = NULL) {
  # Check for duplicate by derivation_hash
  if (!is.null(derivation_hash)) {
    existing <- DBI::dbGetQuery(db,
      "SELECT asset_id FROM assets
       WHERE dataset_id = ? AND derivation_hash = ? AND status = 'active'",
      params = list(dataset_id, derivation_hash))
    if (nrow(existing) > 0) return(existing$asset_id[1])
  }

  # Generate asset_id
  hex <- paste(sample(c(0:9, letters[1:6]), 8, replace = TRUE), collapse = "")
  asset_id <- paste0("asset_", format(Sys.time(), "%Y%m%d_%H%M%S"), "_", hex)
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")

  prov_json <- if (!is.null(provenance))
    as.character(jsonlite::toJSON(provenance, auto_unbox = TRUE)) else NA_character_
  mani_json <- if (!is.null(manifest))
    as.character(jsonlite::toJSON(manifest, auto_unbox = TRUE)) else NA_character_

  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    DBI::dbExecute(db,
      "INSERT INTO assets (asset_id, dataset_id, kind, modality, status,
                           visibility, derivation_hash, created_at, created_by,
                           created_by_job, path_or_root, manifest_json,
                           provenance_json, tags)
       VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      params = list(asset_id, dataset_id, kind, modality %||% NA_character_,
        visibility, derivation_hash %||% NA_character_, now,
        created_by %||% NA_character_, created_by_job %||% NA_character_,
        path_or_root, mani_json, prov_json, tags %||% NA_character_))

    # Record lineage
    for (parent_id in parent_asset_ids) {
      DBI::dbExecute(db,
        "INSERT OR IGNORE INTO asset_parents (asset_id, parent_asset_id, relationship)
         VALUES (?, ?, 'derived_from')",
        params = list(asset_id, parent_id))
    }

    DBI::dbExecute(db, "COMMIT")
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    stop(e)
  })

  asset_id
}

#' List assets for a dataset
#'
#' @param db DBI connection.
#' @param dataset_id Character.
#' @param kind Character or NULL; filter by kind.
#' @param include_inactive Logical; include deprecated/deleted assets.
#' @return Data.frame.
#' @keywords internal
.asset_list <- function(db, dataset_id, kind = NULL,
                         include_inactive = FALSE) {
  where <- "dataset_id = ?"
  params <- list(dataset_id)

  if (!include_inactive) {
    where <- paste(where, "AND status = 'active'")
  }
  if (!is.null(kind)) {
    where <- paste(where, "AND kind = ?")
    params <- c(params, list(kind))
  }

  sql <- paste("SELECT asset_id, kind, modality, visibility, derivation_hash,",
               "created_at, created_by, created_by_job, path_or_root, tags",
               "FROM assets WHERE", where, "ORDER BY created_at DESC")
  DBI::dbGetQuery(db, sql, params = params)
}

#' Get a single asset by ID
#' @keywords internal
.asset_get <- function(db, asset_id) {
  row <- DBI::dbGetQuery(db, "SELECT * FROM assets WHERE asset_id = ?",
    params = list(asset_id))
  if (nrow(row) == 0) return(NULL)
  as.list(row[1, ])
}

#' Resolve an alias to an asset_id
#' @keywords internal
.asset_resolve_alias <- function(db, dataset_id, alias) {
  row <- DBI::dbGetQuery(db,
    "SELECT asset_id FROM asset_aliases WHERE dataset_id = ? AND alias = ?",
    params = list(dataset_id, alias))
  if (nrow(row) == 0) return(NULL)
  row$asset_id[1]
}

#' Set or update an alias
#' @keywords internal
.asset_set_alias <- function(db, dataset_id, alias, asset_id) {
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  DBI::dbExecute(db,
    "INSERT OR REPLACE INTO asset_aliases (dataset_id, alias, asset_id, updated_at)
     VALUES (?, ?, ?, ?)",
    params = list(dataset_id, alias, asset_id, now))
}

#' List aliases for a dataset
#' @keywords internal
.asset_list_aliases <- function(db, dataset_id) {
  DBI::dbGetQuery(db,
    "SELECT alias, asset_id, updated_at FROM asset_aliases
     WHERE dataset_id = ? ORDER BY alias",
    params = list(dataset_id))
}

#' Get lineage (parents) of an asset
#' @keywords internal
.asset_get_lineage <- function(db, asset_id) {
  DBI::dbGetQuery(db,
    "SELECT p.parent_asset_id, p.relationship, a.kind, a.dataset_id
     FROM asset_parents p
     JOIN assets a ON a.asset_id = p.parent_asset_id
     WHERE p.asset_id = ?",
    params = list(asset_id))
}

#' Find asset by derivation_hash (deduplication)
#' @keywords internal
.asset_find_by_hash <- function(db, dataset_id, derivation_hash) {
  row <- DBI::dbGetQuery(db,
    "SELECT asset_id FROM assets
     WHERE dataset_id = ? AND derivation_hash = ? AND status = 'active'
     LIMIT 1",
    params = list(dataset_id, derivation_hash))
  if (nrow(row) == 0) return(NULL)
  row$asset_id[1]
}

#' Compute a derivation hash from parameters
#'
#' Used by dsRadiomics, dsImaging clients to generate a content address
#' for deduplication.
#'
#' @param ... Named values to include in the hash.
#' @return Character; SHA-256 hex digest.
#' @export
compute_derivation_hash <- function(...) {
  params <- list(...)
  # Sort by name for determinism
  params <- params[order(names(params))]
  blob <- jsonlite::toJSON(params, auto_unbox = TRUE, digits = 10)
  digest::digest(blob, algo = "sha256", serialize = FALSE)
}
