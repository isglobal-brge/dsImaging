# Module: Asset Catalog (SQLite)
# Manages derived assets: masks, radiomics tables, embeddings, etc.
# Each dataset can have multiple assets of the same kind with different
# parameterizations. Assets are immutable and content-addressed via
# derivation_hash. Aliases provide human-friendly names.

#' Validate an immutable collection-snapshot seal
#' @keywords internal
.asset_collection_seal <- function(collection_seal, required = FALSE) {
  valid <- is.character(collection_seal) && length(collection_seal) == 1L &&
    !is.na(collection_seal) && grepl("^[0-9a-f]{64}$", collection_seal)
  if (isTRUE(valid)) return(collection_seal)
  if (isTRUE(required)) {
    stop("A valid imaging collection snapshot is required.", call. = FALSE)
  }
  NULL
}

#' Validate a catalog asset kind
#' @keywords internal
.asset_kind <- function(kind) {
  allowed <- c(
    "image_root", "mask_root", "feature_table", "radiomics_collection",
    "per_image_result", "wsi_root", "dicom_series_root", "rt_struct_root",
    "rt_dose_file", "rt_plan_file", "rt_seg_root", "dose_table",
    "qc_table", "qc_visual_asset", "embedding_table", "wsi_tile_root",
    "registration_root", "multimodal_ref"
  )
  if (!is.character(kind) || length(kind) != 1L || is.na(kind) ||
      !kind %in% allowed) {
    stop("Asset kind is not supported.", call. = FALSE)
  }
  kind
}

#' Open the asset catalog database
#'
#' @return A DBI connection.
#' @keywords internal
.asset_db_connect <- function() {
  db_path <- .asset_db_path()
  .asset_db_prepare_path(db_path)

  db <- DBI::dbConnect(RSQLite::SQLite(), db_path, synchronous = NULL)
  ready <- FALSE
  on.exit(if (!ready && DBI::dbIsValid(db)) DBI::dbDisconnect(db), add = TRUE)
  DBI::dbExecute(db, "PRAGMA busy_timeout=5000")
  DBI::dbExecute(db, "PRAGMA journal_mode=WAL")
  DBI::dbExecute(db, "PRAGMA foreign_keys=ON")

  .asset_db_create_schema(db)
  .asset_db_repair_permissions(db_path)
  ready <- TRUE
  db
}

#' Get path to the asset catalog database
#' @keywords internal
.asset_db_path <- function() {
  path <- .imaging_option("asset_db", NULL)
  if (!is.null(path)) return(path)

  # Default: alongside the registry
  reg_path <- .imaging_option("registry_path", NULL)
  if (!is.null(reg_path) && nzchar(reg_path))
    return(file.path(dirname(reg_path), "imaging_assets.sqlite"))

  # Final fallback
  file.path(.imaging_option("data_dir", "/var/lib/dsimaging"),
            "imaging_assets.sqlite")
}

#' Prepare asset DB directory and repair common permission drift.
#' @keywords internal
.asset_db_prepare_path <- function(db_path = .asset_db_path()) {
  db_dir <- dirname(db_path)
  if (!dir.exists(db_dir)) {
    tryCatch(
      dir.create(db_dir, recursive = TRUE, showWarnings = FALSE, mode = "0770"),
      error = function(e) NULL)
  }
  if (dir.exists(db_dir)) {
    tryCatch(Sys.chmod(db_dir, "0770", use_umask = FALSE),
             error = function(e) NULL)
  }

  .asset_db_repair_permissions(db_path)

  if (!dir.exists(db_dir) || file.access(db_dir, mode = 2) != 0)
    stop("dsImaging asset DB storage is unavailable.", call. = FALSE)
  if (file.exists(db_path) && file.access(db_path, mode = 2) != 0)
    stop("dsImaging asset DB storage is unavailable.", call. = FALSE)

  invisible(db_path)
}

#' Repair permissions on the SQLite database sidecar files.
#' @keywords internal
.asset_db_repair_permissions <- function(db_path = .asset_db_path()) {
  db_files <- c(db_path, paste0(db_path, "-wal"), paste0(db_path, "-shm"))
  db_files <- db_files[file.exists(db_files)]
  if (length(db_files) > 0)
    tryCatch(Sys.chmod(db_files, "0660", use_umask = FALSE),
             error = function(e) NULL)
  invisible(db_files)
}

#' Create the asset catalog schema
#' @keywords internal
.asset_db_create_schema <- function(db) {
  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS assets (
      asset_id          TEXT PRIMARY KEY,
      dataset_id        TEXT NOT NULL,
      collection_seal   TEXT,
      kind              TEXT NOT NULL,
      modality          TEXT,
      status            TEXT NOT NULL DEFAULT 'active',
      visibility        TEXT NOT NULL DEFAULT 'private',
      visibility_explicit INTEGER NOT NULL DEFAULT 0,
      storage_backend   TEXT NOT NULL DEFAULT 'file',
      derivation_hash   TEXT,
      created_at        TEXT NOT NULL,
      created_by        TEXT,
      created_by_job    TEXT,
      path_or_root      TEXT,
      description       TEXT,
      manifest_json     TEXT,
      provenance_json   TEXT,
      tags              TEXT
    )")

  # Fail-closed migration for existing catalogs. Rows created before explicit
  # visibility provenance existed are private, even if an older schema's
  # permissive default labelled them global. Only visibility_explicit=1 is a
  # durable administrator/publisher signal that global publication was chosen.
  cols <- DBI::dbListFields(db, "assets")
  if (!"collection_seal" %in% cols) {
    DBI::dbExecute(db,
      "ALTER TABLE assets ADD COLUMN collection_seal TEXT")
  }
  if (!"visibility" %in% cols) {
    DBI::dbExecute(db, paste(
      "ALTER TABLE assets ADD COLUMN visibility TEXT NOT NULL",
      "DEFAULT 'private'"))
  }
  if (!"visibility_explicit" %in% cols) {
    DBI::dbExecute(db, paste(
      "ALTER TABLE assets ADD COLUMN visibility_explicit INTEGER NOT NULL",
      "DEFAULT 0"))
  }
  if (!"storage_backend" %in% cols) {
    DBI::dbExecute(db, paste(
      "ALTER TABLE assets ADD COLUMN storage_backend TEXT NOT NULL",
      "DEFAULT 'file'"))
  }
  DBI::dbExecute(db, paste(
    "UPDATE assets SET visibility = 'private'",
    "WHERE visibility_explicit IS NULL OR visibility_explicit != 1",
    "OR visibility IS NULL OR visibility NOT IN ('private','global')"))
  # Catalogs created before collection binding cannot prove which immutable
  # resource snapshot authorized a row. Keep those rows for operators and
  # provenance, but never expose them as globally reusable assets.
  DBI::dbExecute(db, paste(
    "UPDATE assets SET visibility = 'private'",
    "WHERE collection_seal IS NULL OR length(collection_seal) != 64"))

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
      dataset_id      TEXT NOT NULL,
      collection_seal TEXT,
      alias           TEXT NOT NULL,
      asset_id        TEXT NOT NULL,
      updated_at      TEXT NOT NULL,
      PRIMARY KEY (dataset_id, collection_seal, alias),
      FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
    )")

  alias_cols <- DBI::dbListFields(db, "asset_aliases")
  if (!"collection_seal" %in% alias_cols) {
    # SQLite cannot replace a primary key with ALTER TABLE. Rebuild once so
    # two independently sealed collections may safely use the same alias.
    DBI::dbExecute(db, "ALTER TABLE asset_aliases RENAME TO asset_aliases_legacy")
    DBI::dbExecute(db, "
      CREATE TABLE asset_aliases (
        dataset_id      TEXT NOT NULL,
        collection_seal TEXT,
        alias           TEXT NOT NULL,
        asset_id        TEXT NOT NULL,
        updated_at      TEXT NOT NULL,
        PRIMARY KEY (dataset_id, collection_seal, alias),
        FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
      )")
    DBI::dbExecute(db, paste(
      "INSERT INTO asset_aliases",
      "(dataset_id, collection_seal, alias, asset_id, updated_at)",
      "SELECT aa.dataset_id, a.collection_seal, aa.alias, aa.asset_id,",
      "aa.updated_at FROM asset_aliases_legacy aa",
      "JOIN assets a ON a.asset_id = aa.asset_id"))
    DBI::dbExecute(db, "DROP TABLE asset_aliases_legacy")
  }

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS asset_generations (
      generation_id     TEXT PRIMARY KEY,
      dataset_id        TEXT NOT NULL,
      collection_seal   TEXT,
      kind              TEXT NOT NULL,
      derivation_hash   TEXT NOT NULL,
      visibility        TEXT NOT NULL DEFAULT 'private',
      state             TEXT NOT NULL DEFAULT 'PENDING',
      owner_id          TEXT NOT NULL,
      created_by_job    TEXT,
      expected_n        INTEGER,
      completed_n       INTEGER DEFAULT 0,
      failed_n          INTEGER DEFAULT 0,
      published_asset_id TEXT,
      tracking_id       TEXT,
      spec_json         TEXT,
      error             TEXT,
      created_at        TEXT NOT NULL,
      updated_at        TEXT NOT NULL
    )")

  generation_cols <- DBI::dbListFields(db, "asset_generations")
  if (!"collection_seal" %in% generation_cols) {
    DBI::dbExecute(db,
      "ALTER TABLE asset_generations ADD COLUMN collection_seal TEXT")
  }
  if (!"visibility" %in% generation_cols) {
    DBI::dbExecute(db, paste(
      "ALTER TABLE asset_generations ADD COLUMN visibility TEXT NOT NULL",
      "DEFAULT 'private'"))
  }
  if (!"tracking_id" %in% generation_cols) {
    DBI::dbExecute(db,
      "ALTER TABLE asset_generations ADD COLUMN tracking_id TEXT")
  }
  DBI::dbExecute(db, paste(
    "UPDATE asset_generations SET visibility = 'private'",
    "WHERE visibility IS NULL OR visibility NOT IN ('private','global')"))
  DBI::dbExecute(db, paste(
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_asset_generations_tracking",
    "ON asset_generations(tracking_id) WHERE tracking_id IS NOT NULL"))

  # Status state machine: pending -> claimed -> running -> completed | failed | skipped
  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS asset_items (
      generation_id     TEXT NOT NULL,
      sample_id         TEXT NOT NULL,
      status            TEXT NOT NULL DEFAULT 'pending',
      claimed_by        TEXT,
      claimed_at        TEXT,
      job_token         TEXT,
      artifact_relpath  TEXT,
      checksum          TEXT,
      error             TEXT,
      PRIMARY KEY (generation_id, sample_id),
      FOREIGN KEY (generation_id) REFERENCES asset_generations(generation_id)
    )")

  # Schema migration for existing DBs
  tryCatch({
    cols <- DBI::dbListFields(db, "asset_items")
    if (!"claimed_by" %in% cols)
      DBI::dbExecute(db, "ALTER TABLE asset_items ADD COLUMN claimed_by TEXT")
    if (!"claimed_at" %in% cols)
      DBI::dbExecute(db, "ALTER TABLE asset_items ADD COLUMN claimed_at TEXT")
    if (!"job_token" %in% cols)
      DBI::dbExecute(db, "ALTER TABLE asset_items ADD COLUMN job_token TEXT")
  }, error = function(e) NULL)
  DBI::dbExecute(db, paste(
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_asset_items_job_token",
    "ON asset_items(job_token) WHERE job_token IS NOT NULL"))

  # Sample manifests: abstracts multi-file samples (DICOM series, bundles)
  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS sample_manifests (
      dataset_id      TEXT NOT NULL,
      collection_seal TEXT,
      sample_id       TEXT NOT NULL,
      source_kind     TEXT NOT NULL DEFAULT 'single_file',
      manifest_json   TEXT NOT NULL,
      content_hash    TEXT,
      created_at      TEXT NOT NULL,
      updated_at      TEXT NOT NULL,
      PRIMARY KEY (dataset_id, collection_seal, sample_id)
    )")

  sample_manifest_cols <- DBI::dbListFields(db, "sample_manifests")
  if (!"collection_seal" %in% sample_manifest_cols) {
    DBI::dbExecute(db,
      "ALTER TABLE sample_manifests RENAME TO sample_manifests_legacy")
    DBI::dbExecute(db, "
      CREATE TABLE sample_manifests (
        dataset_id TEXT NOT NULL, collection_seal TEXT, sample_id TEXT NOT NULL,
        source_kind TEXT NOT NULL DEFAULT 'single_file', manifest_json TEXT NOT NULL,
        content_hash TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        PRIMARY KEY (dataset_id, collection_seal, sample_id)
      )")
    DBI::dbExecute(db, paste(
      "INSERT INTO sample_manifests",
      "(dataset_id, collection_seal, sample_id, source_kind, manifest_json,",
      "content_hash, created_at, updated_at)",
      "SELECT dataset_id, NULL, sample_id, source_kind, manifest_json,",
      "content_hash, created_at, updated_at FROM sample_manifests_legacy"))
    DBI::dbExecute(db, "DROP TABLE sample_manifests_legacy")
  }

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS content_fingerprints (
      dataset_id      TEXT NOT NULL,
      collection_seal TEXT,
      sample_id       TEXT NOT NULL,
      file_path       TEXT NOT NULL,
      fingerprint     TEXT NOT NULL,
      content_hash    TEXT,
      file_size       INTEGER NOT NULL,
      file_mtime      REAL NOT NULL,
      computed_at     TEXT NOT NULL,
      PRIMARY KEY (dataset_id, collection_seal, sample_id)
    )")

  # Schema migration for existing DBs
  fingerprint_cols <- DBI::dbListFields(db, "content_fingerprints")
  if (!"content_hash" %in% fingerprint_cols) {
    DBI::dbExecute(db,
      "ALTER TABLE content_fingerprints ADD COLUMN content_hash TEXT")
    fingerprint_cols <- c(fingerprint_cols, "content_hash")
  }
  if (!"collection_seal" %in% fingerprint_cols) {
    DBI::dbExecute(db,
      "ALTER TABLE content_fingerprints RENAME TO content_fingerprints_legacy")
    DBI::dbExecute(db, "
      CREATE TABLE content_fingerprints (
        dataset_id TEXT NOT NULL, collection_seal TEXT, sample_id TEXT NOT NULL,
        file_path TEXT NOT NULL, fingerprint TEXT NOT NULL, content_hash TEXT,
        file_size INTEGER NOT NULL, file_mtime REAL NOT NULL,
        computed_at TEXT NOT NULL,
        PRIMARY KEY (dataset_id, collection_seal, sample_id)
      )")
    DBI::dbExecute(db, paste(
      "INSERT INTO content_fingerprints",
      "(dataset_id, collection_seal, sample_id, file_path, fingerprint,",
      "content_hash, file_size, file_mtime, computed_at)",
      "SELECT dataset_id, NULL, sample_id, file_path, fingerprint,",
      "content_hash, file_size, file_mtime, computed_at",
      "FROM content_fingerprints_legacy"))
    DBI::dbExecute(db, "DROP TABLE content_fingerprints_legacy")
  }

  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_assets_dataset ON assets(dataset_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_assets_collection ON assets(dataset_id, collection_seal)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_assets_hash ON assets(derivation_hash)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_assets_kind ON assets(dataset_id, kind)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_gen_hash ON asset_generations(dataset_id, derivation_hash)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_gen_collection ON asset_generations(dataset_id, collection_seal)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_gen_state ON asset_generations(state)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_items_gen ON asset_items(generation_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_fp_dataset ON content_fingerprints(dataset_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_fp_collection ON content_fingerprints(dataset_id, collection_seal)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_sample_manifest_collection ON sample_manifests(dataset_id, collection_seal)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_fp_fingerprint ON content_fingerprints(fingerprint)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_fp_content_hash ON content_fingerprints(content_hash)")
  .content_hash_ensure_schema(db)
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
                             visibility = "private",
                             collection_seal = NULL,
                             storage_backend = "file",
                             tags = NULL,
                             manifest = NULL,
                             description = NULL,
                             .in_transaction = FALSE) {
  if (!visibility %in% c("private", "global")) {
    stop("visibility must be 'private' or 'global'.", call. = FALSE)
  }
  kind <- .asset_kind(kind)
  collection_seal <- .asset_collection_seal(collection_seal,
    required = identical(visibility, "global"))

  # Only global assets are reusable across sessions. A private derivation hash
  # is not an authorization mechanism and must never discover another run.
  if (!is.null(derivation_hash) && identical(visibility, "global")) {
    existing <- DBI::dbGetQuery(db,
      "SELECT asset_id FROM assets
       WHERE dataset_id = ? AND derivation_hash = ? AND status = 'active'
         AND visibility = 'global' AND collection_seal = ?",
      params = list(dataset_id, derivation_hash, collection_seal))
    if (nrow(existing) > 0) return(existing$asset_id[1])
  }

  # Generate asset_id
  asset_id <- paste0("asset_", gsub("-", "",
    uuid::UUIDgenerate(use.time = FALSE), fixed = TRUE))
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")

  prov_json <- if (!is.null(provenance))
    as.character(jsonlite::toJSON(provenance, auto_unbox = TRUE)) else NA_character_
  mani_json <- if (!is.null(manifest))
    as.character(jsonlite::toJSON(manifest, auto_unbox = TRUE)) else NA_character_

  if (!isTRUE(.in_transaction))
    DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    parent_asset_ids <- unique(as.character(parent_asset_ids))
    if (length(parent_asset_ids) > 0L) {
      placeholders <- paste(rep("?", length(parent_asset_ids)), collapse = ",")
      parents <- DBI::dbGetQuery(db, paste0(
        "SELECT asset_id, dataset_id, collection_seal FROM assets ",
        "WHERE asset_id IN (", placeholders, ")"),
        params = as.list(parent_asset_ids))
      expected_seal <- collection_seal %||% NA_character_
      if (nrow(parents) != length(parent_asset_ids) ||
          any(parents$dataset_id != dataset_id) ||
          any(is.na(parents$collection_seal) != is.na(expected_seal)) ||
          any(!is.na(parents$collection_seal) &
              parents$collection_seal != expected_seal)) {
        stop("Asset lineage crosses an imaging collection boundary.",
             call. = FALSE)
      }
    }
    DBI::dbExecute(db,
      "INSERT INTO assets (asset_id, dataset_id, collection_seal, kind, modality, status,
                           visibility, visibility_explicit, storage_backend,
                           derivation_hash,
                           created_at, created_by, created_by_job,
                           path_or_root, description,
                           manifest_json, provenance_json, tags)
       VALUES (?, ?, ?, ?, ?, 'active', ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      params = list(asset_id, dataset_id, collection_seal %||% NA_character_,
        kind, modality %||% NA_character_,
        visibility, storage_backend,
        derivation_hash %||% NA_character_, now,
        created_by %||% NA_character_, created_by_job %||% NA_character_,
        path_or_root, description %||% NA_character_,
        mani_json, prov_json, tags %||% NA_character_))

    # Record lineage
    for (parent_id in parent_asset_ids) {
      DBI::dbExecute(db,
        "INSERT OR IGNORE INTO asset_parents (asset_id, parent_asset_id, relationship)
         VALUES (?, ?, 'derived_from')",
        params = list(asset_id, parent_id))
    }

    if (!isTRUE(.in_transaction))
      DBI::dbExecute(db, "COMMIT")
  }, error = function(e) {
    if (!isTRUE(.in_transaction))
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
                         include_inactive = FALSE,
                         visibility = "global", collection_seal = NULL) {
  where <- "dataset_id = ? AND visibility = ?"
  params <- list(dataset_id, visibility)

  collection_seal <- .asset_collection_seal(collection_seal)
  if (is.null(collection_seal)) {
    where <- paste(where, "AND collection_seal IS NULL")
  } else {
    where <- paste(where, "AND collection_seal = ?")
    params <- c(params, list(collection_seal))
  }

  if (!include_inactive) {
    where <- paste(where, "AND status = 'active'")
  }
  if (!is.null(kind)) {
    where <- paste(where, "AND kind = ?")
    params <- c(params, list(kind))
  }

  sql <- paste("SELECT asset_id, kind, modality, visibility, derivation_hash,",
               "created_at, created_by, created_by_job, path_or_root,",
               "description, tags",
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
.asset_resolve_alias <- function(db, dataset_id, alias,
                                 collection_seal = NULL) {
  collection_seal <- .asset_collection_seal(collection_seal)
  if (is.null(collection_seal)) return(NULL)
  row <- DBI::dbGetQuery(db,
    "SELECT aa.asset_id FROM asset_aliases aa
     JOIN assets a ON a.asset_id = aa.asset_id
     WHERE aa.dataset_id = ? AND aa.collection_seal = ? AND aa.alias = ?
       AND a.dataset_id = ? AND a.collection_seal = ?
       AND a.visibility = 'global' AND a.status = 'active'",
    params = list(dataset_id, collection_seal, alias, dataset_id,
      collection_seal))
  if (nrow(row) == 0) return(NULL)
  row$asset_id[1]
}

#' Set or update an alias
#' @keywords internal
.asset_set_alias <- function(db, dataset_id, alias, asset_id) {
  asset <- DBI::dbGetQuery(db,
    "SELECT dataset_id, collection_seal, visibility, status
     FROM assets WHERE asset_id = ?",
    params = list(asset_id))
  collection_seal <- if (nrow(asset) == 1L) {
    .asset_collection_seal(as.character(asset$collection_seal[1]))
  } else NULL
  if (nrow(asset) != 1L ||
      !identical(as.character(asset$dataset_id[1]), dataset_id) ||
      is.null(collection_seal) ||
      !identical(as.character(asset$visibility[1]), "global") ||
      !identical(as.character(asset$status[1]), "active")) {
    stop("Only an active global asset in the same dataset may be aliased.",
         call. = FALSE)
  }
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  DBI::dbExecute(db,
    "INSERT OR REPLACE INTO asset_aliases
     (dataset_id, collection_seal, alias, asset_id, updated_at)
     VALUES (?, ?, ?, ?, ?)",
    params = list(dataset_id, collection_seal, alias, asset_id, now))
}

#' List aliases for a dataset
#' @keywords internal
.asset_list_aliases <- function(db, dataset_id, collection_seal = NULL) {
  collection_seal <- .asset_collection_seal(collection_seal)
  if (is.null(collection_seal)) return(data.frame())
  DBI::dbGetQuery(db,
    "SELECT aa.alias, aa.asset_id, aa.updated_at FROM asset_aliases aa
     JOIN assets a ON a.asset_id = aa.asset_id
     WHERE aa.dataset_id = ? AND aa.collection_seal = ?
       AND a.dataset_id = ? AND a.collection_seal = ?
       AND a.visibility = 'global' AND a.status = 'active'
     ORDER BY aa.alias",
    params = list(dataset_id, collection_seal, dataset_id, collection_seal))
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
.asset_find_by_hash <- function(db, dataset_id, derivation_hash,
                                visibility = "global",
                                collection_seal = NULL) {
  collection_seal <- .asset_collection_seal(collection_seal)
  if (is.null(collection_seal)) return(NULL)
  row <- DBI::dbGetQuery(db,
    "SELECT asset_id FROM assets
     WHERE dataset_id = ? AND derivation_hash = ? AND status = 'active'
       AND visibility = ? AND collection_seal = ?
     LIMIT 1",
    params = list(dataset_id, derivation_hash, visibility, collection_seal))
  if (nrow(row) == 0) return(NULL)
  row$asset_id[1]
}

#' Find an active asset by derivation hash
#'
#' Public wrapper for deduplication lookups.
#'
#' @param dataset_id Character; the dataset.
#' @param derivation_hash Character; the hash to look up.
#' @param collection_seal Character; immutable authorized collection seal.
#' @return Character asset_id if found, NULL otherwise.
#' @export
find_asset_by_hash <- function(dataset_id, derivation_hash, collection_seal) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  .asset_find_by_hash(db, dataset_id, derivation_hash,
    collection_seal = .asset_collection_seal(collection_seal, required = TRUE))
}

#' Compute a derivation hash from parameters
#'
#' Used by dsImaging clients and domain extensions to generate a content address
#' for deduplication.
#'
#' @param ... Named values to include in the hash.
#' @return Character; SHA-256 hex digest.
#' @export
compute_derivation_hash <- function(...) {
  params <- list(...)
  params <- params[order(names(params))]
  implementation_version <- tryCatch(
    as.character(utils::packageVersion("dsImaging")),
    error = function(e) "development")
  payload <- list(contract = "dsImaging-derivation-v2",
    implementation_version = implementation_version, params = params)
  blob <- jsonlite::toJSON(payload, auto_unbox = TRUE, digits = 10,
    null = "null")
  digest::digest(blob, algo = "sha256", serialize = FALSE)
}

# =============================================================================
# Generation management (for partial runs, resume, dedup of in-flight work)
# =============================================================================

#' Claim or reuse a generation
#'
#' Three outcomes:
#' - ACTIVE asset exists -> reuse_asset
#' - RUNNING/PENDING generation exists -> reuse_generation
#' - Nothing -> create new generation (run_new)
#'
#' @param dataset_id Character; dataset identifier.
#' @param kind Character; derived asset/generation kind.
#' @param derivation_hash Character; content-addressed derivation hash.
#' @param collection_seal Character or NULL; immutable collection seal. When
#'   omitted it is read from `spec$dataset_context$collection_seal`.
#' @param visibility Character; visibility label for the generation.
#' @param owner_id Character or NULL; owner creating the generation.
#' @param job_id Character or NULL; optional parent job id.
#' @param expected_n Integer or NULL; expected number of items.
#' @param spec Named list or NULL; processing specification to persist.
#' @return Named list with action, asset_id or generation_id.
#' @export
claim_or_reuse_generation <- function(dataset_id, kind, derivation_hash,
                                       collection_seal = NULL,
                                       visibility = "private", owner_id = NULL,
                                       job_id = NULL, expected_n = NULL,
                                       spec = NULL) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))

  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    if (!visibility %in% c("private", "global")) {
      stop("visibility must be 'private' or 'global'.", call. = FALSE)
    }
    collection_seal <- collection_seal %||%
      (spec$dataset_context %||% list())$collection_seal
    collection_seal <- .asset_collection_seal(collection_seal,
      required = identical(visibility, "global"))
    # 1. Active asset with this hash?
    existing_asset <- if (identical(visibility, "global")) {
      .asset_find_by_hash(db, dataset_id, derivation_hash,
                          visibility = "global",
                          collection_seal = collection_seal)
    } else NULL
    if (!is.null(existing_asset)) {
      DBI::dbExecute(db, "COMMIT")
      return(list(action = "reuse_asset", asset_id = existing_asset))
    }

    # 2. Running/pending generation. Age alone cannot establish abandonment:
    # long-running dsHPC jobs may legitimately leave this row unchanged.
    gen <- if (identical(visibility, "global")) DBI::dbGetQuery(db,
      "SELECT generation_id, state, published_asset_id, updated_at
       FROM asset_generations
       WHERE dataset_id = ? AND derivation_hash = ?
         AND collection_seal = ? AND visibility = 'global'
         AND state IN ('PENDING','RUNNING')
       LIMIT 1",
      params = list(dataset_id, derivation_hash, collection_seal)) else data.frame()
    if (nrow(gen) > 0) {
      DBI::dbExecute(db, "COMMIT")
      return(list(action = "reuse_generation", generation_id = gen$generation_id[1],
                   state = gen$state[1]))
    }

    # 3. Create new generation
    gen_id <- paste0("gen_", gsub("-", "",
      uuid::UUIDgenerate(use.time = FALSE), fixed = TRUE))
    now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
    spec_json <- if (!is.null(spec))
      as.character(jsonlite::toJSON(spec, auto_unbox = TRUE)) else NA_character_
    tracking_id <- if (!is.null(spec$tracking_id)) {
      .imaging_tracking_id(spec$tracking_id)
    } else NULL

    DBI::dbExecute(db,
      "INSERT INTO asset_generations (generation_id, dataset_id, collection_seal,
        kind, derivation_hash,
        visibility, state, owner_id, created_by_job, expected_n, tracking_id,
        spec_json, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?, ?, ?, ?, ?)",
      params = list(gen_id, dataset_id, collection_seal %||% NA_character_,
        kind, derivation_hash,
        visibility, owner_id %||% NA_character_, job_id %||% NA_character_,
        as.integer(expected_n %||% NA_integer_), tracking_id %||% NA_character_,
        spec_json, now, now))

    DBI::dbExecute(db, "COMMIT")
    list(action = "run_new", generation_id = gen_id)
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    stop(e)
  })
}

#' Update generation state
#'
#' @param generation_id Character; generation identifier.
#' @param ... Named fields to update in `asset_generations`.
#' @export
update_generation <- function(generation_id, ...) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  updates <- list(...)
  updates$updated_at <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  set_clauses <- paste0(names(updates), " = ?")
  sql <- paste0("UPDATE asset_generations SET ",
                paste(set_clauses, collapse = ", "),
                " WHERE generation_id = ?")
  DBI::dbExecute(db, sql, params = c(unname(updates), list(generation_id)))
}

#' Atomically increment a generation counter
#'
#' Uses SQL `SET field = field + 1` to avoid read-modify-write races
#' when multiple per-image jobs complete concurrently.
#'
#' @param generation_id Character.
#' @param field Character; "completed_n" or "failed_n".
#' @param n Integer; amount to increment (default 1).
#' @return The new value of the field.
#' @export
increment_generation_counter <- function(generation_id, field, n = 1L) {
  if (!field %in% c("completed_n", "failed_n"))
    stop("Only completed_n and failed_n can be incremented.", call. = FALSE)
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  sql <- paste0("UPDATE asset_generations SET ", field, " = ", field, " + ?,
                  updated_at = ? WHERE generation_id = ?")
  DBI::dbExecute(db, sql, params = list(as.integer(n), now, generation_id))
  row <- DBI::dbGetQuery(db,
    paste0("SELECT ", field, " FROM asset_generations WHERE generation_id = ?"),
    params = list(generation_id))
  if (nrow(row) == 0) return(0L)
  as.integer(row[[1]][1])
}

#' Record per-sample item status
#'
#' @param generation_id Character; generation identifier.
#' @param sample_id Character; sample identifier.
#' @param status Character; item status.
#' @param artifact_relpath Character or NULL; output artifact reference.
#' @param checksum Character or NULL; artifact checksum.
#' @param error Character or NULL; failure message.
#' @param job_token Character or NULL; private opaque dsHPC correlation token.
#' @export
record_item_status <- function(generation_id, sample_id, status,
                                artifact_relpath = NULL, checksum = NULL,
                                error = NULL, job_token = NULL) {
  if (!is.null(job_token) &&
      (!is.character(job_token) || length(job_token) != 1L ||
       is.na(job_token) || !grepl("^[0-9a-f]{32}$", job_token))) {
    stop("Invalid private job correlation token.", call. = FALSE)
  }
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  DBI::dbExecute(db,
    "INSERT INTO asset_items (generation_id, sample_id, status, job_token,
      artifact_relpath, checksum, error)
     VALUES (?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(generation_id, sample_id) DO UPDATE SET
       status = excluded.status,
       claimed_by = NULL,
       claimed_at = NULL,
       job_token = COALESCE(excluded.job_token, asset_items.job_token),
       artifact_relpath = excluded.artifact_relpath,
       checksum = excluded.checksum,
       error = excluded.error",
    params = list(generation_id, sample_id, status,
      job_token %||% NA_character_,
      artifact_relpath %||% NA_character_,
      checksum %||% NA_character_,
      error %||% NA_character_))
}

#' Return one durable opaque dsHPC correlation token for a generation item
#' @keywords internal
.item_job_token <- function(generation_id, sample_id) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    row <- DBI::dbGetQuery(db,
      "SELECT job_token FROM asset_items
       WHERE generation_id = ? AND sample_id = ?",
      params = list(generation_id, sample_id))
    if (nrow(row) != 1L) {
      stop("Generation item is unavailable for job submission.",
           call. = FALSE)
    }
    token <- as.character(row$job_token[[1L]])
    if (length(token) == 1L && !is.na(token) && nzchar(token)) {
      if (!grepl("^[0-9a-f]{32}$", token)) {
        stop("Generation item has an invalid job correlation token.",
             call. = FALSE)
      }
      DBI::dbExecute(db, "COMMIT")
      return(token)
    }

    token <- tolower(gsub(
      "-", "", uuid::UUIDgenerate(use.time = FALSE), fixed = TRUE))
    if (!grepl("^[0-9a-f]{32}$", token)) {
      stop("Could not create a private job correlation token.",
           call. = FALSE)
    }
    updated <- DBI::dbExecute(db,
      "UPDATE asset_items SET job_token = ?
       WHERE generation_id = ? AND sample_id = ? AND job_token IS NULL",
      params = list(token, generation_id, sample_id))
    if (updated != 1L) {
      stop("Could not persist the private job correlation token.",
           call. = FALSE)
    }
    DBI::dbExecute(db, "COMMIT")
    token
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    stop(e)
  })
}

#' Complete a per-image item atomically
#'
#' Performs ALL of these in a single SQLite transaction:
#'   1. Update asset_items row (status, artifact, error)
#'   2. Recompute generation counters from asset_items
#'   3. Optionally register a per-image asset (for cross-user dedup)
#'
#' This eliminates the race condition where a crash or duplicate job completion
#' leaves counters out of sync. A completed item is never downgraded by a later
#' duplicate failure; a retry success can replace a previous failure.
#'
#' @param generation_id Character.
#' @param sample_id Character.
#' @param status Character; "completed" or "failed".
#' @param artifact_relpath Character or NULL.
#' @param checksum Character or NULL.
#' @param error Character or NULL.
#' @param register_asset Named list or NULL. If provided, must contain:
#'   dataset_id, kind, path_or_root, derivation_hash, provenance, created_by_job.
#' @return Invisible TRUE on success.
#' @export
complete_item_atomic <- function(generation_id, sample_id, status,
                                  artifact_relpath = NULL, checksum = NULL,
                                  error = NULL, register_asset = NULL) {
  if (!status %in% c("completed", "failed", "skipped"))
    stop("status must be 'completed', 'failed', or 'skipped'", call. = FALSE)

  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")

  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))

  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    current <- DBI::dbGetQuery(db,
      "SELECT status, artifact_relpath, checksum, error
       FROM asset_items
       WHERE generation_id = ? AND sample_id = ?",
      params = list(generation_id, sample_id))

    if (nrow(current) > 0 && identical(current$status[1], "completed") &&
        !identical(status, "completed")) {
      status <- "completed"
      artifact_relpath <- current$artifact_relpath[1]
      checksum <- current$checksum[1]
      error <- current$error[1]
      register_asset <- NULL
    }

    # 1. Update item status
    DBI::dbExecute(db,
      "INSERT INTO asset_items (generation_id, sample_id, status,
        artifact_relpath, checksum, error)
       VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT(generation_id, sample_id) DO UPDATE SET
         status = excluded.status,
         claimed_by = NULL,
         claimed_at = NULL,
         artifact_relpath = excluded.artifact_relpath,
         checksum = excluded.checksum,
         error = excluded.error",
      params = list(generation_id, sample_id, status,
        artifact_relpath %||% NA_character_,
        checksum %||% NA_character_,
        error %||% NA_character_))

    # 2. Recompute counters from durable item state. This is slightly more
    # work than an increment but makes retries/crash recovery idempotent.
    .recompute_generation_counters(db, generation_id, now = now)

    # 3. Optionally register per-image asset (cross-user dedup)
    if (!is.null(register_asset)) {
      generation <- DBI::dbGetQuery(db,
        "SELECT dataset_id, collection_seal FROM asset_generations
         WHERE generation_id = ?",
        params = list(generation_id))
      if (nrow(generation) != 1L ||
          !identical(as.character(generation$dataset_id[1]),
                     as.character(register_asset$dataset_id))) {
        stop("Per-image asset has no matching collection generation.",
             call. = FALSE)
      }
      .asset_register(db,
        dataset_id = register_asset$dataset_id,
        collection_seal = as.character(generation$collection_seal[1]),
        kind = register_asset$kind %||% "per_image_result",
        path_or_root = register_asset$path_or_root,
        derivation_hash = register_asset$derivation_hash,
        provenance = register_asset$provenance,
        created_by_job = register_asset$created_by_job,
        visibility = register_asset$visibility %||% "private",
        description = register_asset$description %||%
          paste0("Per-image result: ", sample_id),
        .in_transaction = TRUE)
    }

    DBI::dbExecute(db, "COMMIT")
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    stop(e)
  })

  invisible(TRUE)
}

#' Reconcile generation counters from item rows
#'
#' @param generation_id Character; generation identifier.
#' @return Invisible TRUE.
#' @export
reconcile_generation_counters <- function(generation_id) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  .recompute_generation_counters(db, generation_id)
  invisible(TRUE)
}

#' @keywords internal
.recompute_generation_counters <- function(db, generation_id,
                                           now = format(Sys.time(),
                                             "%Y-%m-%dT%H:%M:%OS3Z",
                                             tz = "UTC")) {
  DBI::dbExecute(db,
    "UPDATE asset_generations
     SET completed_n = (
           SELECT COUNT(*) FROM asset_items
           WHERE generation_id = ? AND status = 'completed'
         ),
         failed_n = (
           SELECT COUNT(*) FROM asset_items
           WHERE generation_id = ? AND status = 'failed'
         ),
         updated_at = ?
     WHERE generation_id = ?",
    params = list(generation_id, generation_id, now, generation_id))
  invisible(TRUE)
}

#' Atomically claim N pending items for processing
#'
#' Selects up to \code{n} items with status "pending", marks them
#' "claimed", and returns their sample_ids. Uses BEGIN IMMEDIATE
#' so concurrent callers never claim the same items.
#'
#' @param generation_id Character.
#' @param n Integer; max items to claim.
#' @param claimer_id Character or NULL; identifier for the claimer.
#' @return Character vector of claimed sample_ids (may be empty).
#' @export
claim_pending_items <- function(generation_id, n, claimer_id = NULL) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")

  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    pending <- DBI::dbGetQuery(db,
      "SELECT sample_id FROM asset_items
       WHERE generation_id = ? AND status = 'pending'
       ORDER BY sample_id LIMIT ?",
      params = list(generation_id, as.integer(n)))

    if (nrow(pending) == 0) {
      DBI::dbExecute(db, "COMMIT")
      return(character(0))
    }

    ids <- pending$sample_id
    placeholders <- paste(rep("?", length(ids)), collapse = ",")
    DBI::dbExecute(db,
      paste0("UPDATE asset_items SET status = 'claimed', claimed_by = ?,
              claimed_at = ? WHERE generation_id = ? AND sample_id IN (",
             placeholders, ")"),
      params = c(list(claimer_id %||% NA_character_, now, generation_id),
                  as.list(ids)))

    DBI::dbExecute(db, "COMMIT")
    ids
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    character(0)
  })
}

#' Requeue stale claimed items
#'
#' Items are marked `claimed` while a server process is preparing their dsHPC
#' submissions. If that process dies before it records `running` or a terminal
#' status, the generation must not stall forever.
#'
#' @param generation_id Character; generation identifier.
#' @param timeout_secs Numeric; claim age threshold in seconds.
#' @return Integer count of items requeued.
#' @keywords internal
requeue_stale_claimed_items <- function(generation_id, timeout_secs = NULL) {
  timeout_secs <- as.numeric(timeout_secs %||%
    getOption("dsimaging.analysis.claim_timeout_secs",
      getOption("default.dsimaging.analysis.claim_timeout_secs",
        getOption("dsimaging.claim_timeout_secs",
          getOption("default.dsimaging.claim_timeout_secs", 3600)))))
  if (is.na(timeout_secs) || timeout_secs <= 0) return(0L)

  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  cutoff <- format(Sys.time() - timeout_secs, "%Y-%m-%dT%H:%M:%OS3Z",
    tz = "UTC")

  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    n <- DBI::dbExecute(db,
      "UPDATE asset_items
       SET status = 'pending',
           claimed_by = NULL,
           claimed_at = NULL,
           error = 'Stale claim requeued after submit interruption'
       WHERE generation_id = ?
         AND status = 'claimed'
         AND (claimed_at IS NULL OR claimed_at = '' OR claimed_at < ?)",
      params = list(generation_id, cutoff))
    if (n > 0)
      .recompute_generation_counters(db, generation_id, now = now)
    DBI::dbExecute(db, "COMMIT")
    as.integer(n)
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    stop(e)
  })
}

#' Mark unfinished generation items as skipped
#'
#' @param generation_id Character; generation identifier.
#' @param reason Character; reason stored on skipped items.
#' @return Integer count of items skipped.
#' @keywords internal
skip_unfinished_generation_items <- function(generation_id,
                                             reason = "Cancelled") {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    n <- DBI::dbExecute(db,
      "UPDATE asset_items
       SET status = 'skipped',
           artifact_relpath = NULL,
           checksum = NULL,
           error = ?,
           claimed_by = NULL,
           claimed_at = NULL
       WHERE generation_id = ?
         AND status IN ('pending', 'claimed', 'running')",
      params = list(reason %||% "Cancelled", generation_id))
    if (n > 0)
      .recompute_generation_counters(db, generation_id, now = now)
    DBI::dbExecute(db, "COMMIT")
    as.integer(n)
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    stop(e)
  })
}

#' Get generation summary
#'
#' @param generation_id Character; generation identifier.
#' @return Named list or NULL.
#' @export
get_generation <- function(generation_id) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  row <- DBI::dbGetQuery(db,
    "SELECT * FROM asset_generations WHERE generation_id = ?",
    params = list(generation_id))
  if (nrow(row) == 0) return(NULL)
  as.list(row[1, ])
}

#' Get items for a generation (for resume)
#'
#' @param generation_id Character; generation identifier.
#' @param status Character or NULL; optional status filter.
#' @return Data frame of generation items.
#' @export
get_generation_items <- function(generation_id, status = NULL) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  if (is.null(status))
    DBI::dbGetQuery(db, "SELECT * FROM asset_items WHERE generation_id = ?",
      params = list(generation_id))
  else
    DBI::dbGetQuery(db,
      "SELECT * FROM asset_items WHERE generation_id = ? AND status = ?",
      params = list(generation_id, status))
}

#' Mark a generation as FAILED (called when the job fails)
#'
#' @param generation_id Character; generation identifier.
#' @param error Character or NULL; failure message.
#' @export
fail_generation <- function(generation_id, error = NULL) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  DBI::dbExecute(db,
    "UPDATE asset_generations SET state = 'FAILED', error = ?, updated_at = ?
     WHERE generation_id = ? AND state IN ('PENDING', 'RUNNING')",
    params = list(error %||% "Job failed", now, generation_id))
}

#' Compatibility hook for generation cleanup
#'
#' Generation age alone does not establish that work has been abandoned. A
#' pending or running generation may belong to a long-running dsHPC job, so
#' lifecycle transitions require explicit recovery, cancellation, or failure.
#' This compatibility hook does not mutate generation state.
#'
#' @param max_age_hours Retained for API compatibility; ignored.
#' @return Integer zero.
#' @export
cleanup_stale_generations <- function(max_age_hours = 2) {
  0L
}

#' Publish a completed generation as an active asset
#'
#' Only if completed_n == expected_n (no partial publishes).
#'
#' @param generation_id Character; generation identifier.
#' @param path_or_root Character; output path or root to register.
#' @param description Character or NULL; asset description.
#' @param parent_asset_ids Character vector of parent asset ids.
#' @param provenance Named list or NULL; provenance metadata.
#' @param alias Character or NULL; optional alias to promote.
#' @param publish_backend Backend object or NULL; optional publish target.
#' @return Character; the published asset_id, or NULL if validation fails.
#' @export
publish_generation <- function(generation_id, path_or_root, description = NULL,
                                parent_asset_ids = character(0),
                                provenance = NULL, alias = NULL,
                                publish_backend = NULL) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))

  gen <- DBI::dbGetQuery(db,
    "SELECT * FROM asset_generations WHERE generation_id = ?",
    params = list(generation_id))
  if (nrow(gen) == 0) stop("Generation not found.", call. = FALSE)
  gen <- as.list(gen[1, ])

  # Validate completeness
  if (!is.na(gen$expected_n) && gen$completed_n < gen$expected_n) {
    update_generation(generation_id, state = "PARTIAL")
    return(NULL)
  }

  # Commit-by-catalog: upload to target backend if publish_backend provided,
  # then register in SQLite (the INSERT is the atomic commit point)
  final_uri <- path_or_root
  backend_type <- "file"
  if (!is.null(publish_backend)) {
    prefix <- publish_backend$config$uri_prefix
    if (is.null(prefix))
      prefix <- if (publish_backend$type == "s3")
        .build_s3_uri("assets", gen$dataset_id) else path_or_root
    dest_uri <- paste0(sub("/$", "", prefix), "/", generation_id)
    backend_put_directory(publish_backend, path_or_root, dest_uri)
    final_uri <- dest_uri
    backend_type <- publish_backend$type
  }

  # Register as active asset (this INSERT is the commit point)
  asset_id <- .asset_register(db, gen$dataset_id, gen$kind, final_uri,
    collection_seal = gen$collection_seal,
    derivation_hash = gen$derivation_hash,
    parent_asset_ids = parent_asset_ids,
    provenance = provenance,
    created_by = gen$owner_id,
    created_by_job = gen$created_by_job,
    visibility = gen$visibility,
    storage_backend = backend_type,
    description = description)

  # Update generation
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  DBI::dbExecute(db,
    "UPDATE asset_generations SET state = 'ACTIVE', published_asset_id = ?,
     updated_at = ? WHERE generation_id = ?",
    params = list(asset_id, now, generation_id))

  if (!is.null(alias) && identical(gen$visibility, "global")) {
    .asset_set_alias(db, gen$dataset_id, alias, asset_id)
  }

  asset_id
}

# =============================================================================
# Content fingerprinting (per-image deduplication)
# =============================================================================

#' Compute collection fingerprints and diff against stored state
#'
#' Runs the Python fingerprinting helper on the image root, compares
#' against stored fingerprints in the database, and returns a diff
#' with new/changed/unchanged samples.
#'
#' @param dataset_id Character; the dataset.
#' @param collection_seal Character; immutable collection-snapshot seal.
#' @param image_root Character; path to the image directory.
#' @param compute_content_hash Logical; compute full-file content hashes.
#' @return Named list: new (character), changed (character),
#'   unchanged (character), fingerprints (named list of fp strings).
#' @export
compute_collection_fingerprints <- function(dataset_id, collection_seal,
                                              image_root,
                                              compute_content_hash = TRUE) {
  collection_seal <- .asset_collection_seal(collection_seal, required = TRUE)
  if (!dir.exists(image_root))
    stop("Image root does not exist: ", image_root, call. = FALSE)

  script <- system.file("python", "fingerprint_images.py", package = "dsImaging")
  if (!nzchar(script))
    stop("fingerprint_images.py not found in dsImaging", call. = FALSE)

  tmp <- tempfile(fileext = ".json")
  on.exit(unlink(tmp), add = TRUE)

  python <- .python3()
  py_args <- c(shQuote(script), "--image-root", shQuote(image_root),
               "--output", shQuote(tmp))
  if (isTRUE(compute_content_hash))
    py_args <- c(py_args, "--compute-content-hash")

  res <- system2(python, args = py_args, stdout = TRUE, stderr = TRUE)

  if (!file.exists(tmp))
    stop("Fingerprinting failed:\n", paste(res, collapse = "\n"), call. = FALSE)

  new_fps <- jsonlite::fromJSON(tmp, simplifyVector = FALSE)

  db <- .asset_db_connect()
  on.exit(.asset_db_close(db), add = TRUE)

  stored <- DBI::dbGetQuery(db,
    "SELECT sample_id, fingerprint FROM content_fingerprints
     WHERE dataset_id = ? AND collection_seal = ?",
    params = list(dataset_id, collection_seal))
  stored_map <- stats::setNames(stored$fingerprint, stored$sample_id)

  new_ids <- character(0)
  changed_ids <- character(0)
  unchanged_ids <- character(0)
  fp_map <- list()
  ch_map <- list()  # content_hash map

  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")

  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    for (entry in new_fps) {
      sid <- entry$sample_id
      fp <- entry$fingerprint
      fp_map[[sid]] <- fp
      if (!is.null(entry$content_hash))
        ch_map[[sid]] <- entry$content_hash

      if (is.null(stored_map[sid]) || is.na(stored_map[sid])) {
        new_ids <- c(new_ids, sid)
      } else if (stored_map[[sid]] != fp) {
        changed_ids <- c(changed_ids, sid)
      } else {
        unchanged_ids <- c(unchanged_ids, sid)
        next
      }

      DBI::dbExecute(db,
        "INSERT OR REPLACE INTO content_fingerprints
         (dataset_id, collection_seal, sample_id, file_path, fingerprint, content_hash,
          file_size, file_mtime, computed_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params = list(dataset_id, collection_seal, sid, entry$file_path, fp,
                       entry$content_hash %||% NA_character_,
                       as.integer(entry$file_size),
                       as.numeric(entry$file_mtime), now))
    }
    DBI::dbExecute(db, "COMMIT")
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    stop(e)
  })

  list(
    new = new_ids,
    changed = changed_ids,
    unchanged = unchanged_ids,
    fingerprints = fp_map,
    content_hashes = ch_map,
    total = length(new_fps)
  )
}

#' Compute a per-image derivation hash
#'
#' Combines the image identity with processor and parameters to produce
#' a content-addressable hash. Prefers content_hash (strong, full-file)
#' over fingerprint (fast, scan-level) for true content addressing.
#'
#' @param content_hash Character or NULL; strong full-file SHA-256.
#' @param fingerprint Character or NULL; fast scan fingerprint (fallback).
#' @param processor Character; processor identifier.
#' @param params Named list; processing parameters.
#' @return Character; SHA-256 hex digest.
#' @export
compute_image_derivation_hash <- function(content_hash = NULL, fingerprint = NULL,
                                           processor, params = list()) {
  identity <- content_hash %||% fingerprint
  if (is.null(identity))
    stop("Either content_hash or fingerprint required.", call. = FALSE)
  compute_derivation_hash(
    image_identity = identity,
    processor = processor,
    params = params
  )
}

#' Get stored content hashes for specific samples
#'
#' @param dataset_id Character.
#' @param collection_seal Character; immutable collection-snapshot seal.
#' @param sample_ids Character vector of sample IDs.
#' @return Named list mapping sample_id -> content_hash.
#' @export
get_content_hashes <- function(dataset_id, collection_seal, sample_ids) {
  collection_seal <- .asset_collection_seal(collection_seal, required = TRUE)
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  placeholders <- paste(rep("?", length(sample_ids)), collapse = ",")
  fps <- DBI::dbGetQuery(db,
    paste0("SELECT sample_id, content_hash FROM content_fingerprints
            WHERE dataset_id = ? AND collection_seal = ?
              AND sample_id IN (", placeholders, ")"),
    params = c(list(dataset_id, collection_seal), as.list(sample_ids)))
  stats::setNames(as.list(fps$content_hash), fps$sample_id)
}

#' Store content hashes from a hash index into the local SQLite
#'
#' @param dataset_id Character.
#' @param collection_seal Character; immutable collection-snapshot seal.
#' @param sample_ids Character vector.
#' @param content_hashes Character vector (same length as sample_ids).
#' @export
store_content_hashes <- function(dataset_id, collection_seal, sample_ids,
                                 content_hashes) {
  collection_seal <- .asset_collection_seal(collection_seal, required = TRUE)
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    for (i in seq_along(sample_ids)) {
      DBI::dbExecute(db,
        "INSERT OR REPLACE INTO content_fingerprints
         (dataset_id, collection_seal, sample_id, file_path, fingerprint, content_hash,
          file_size, file_mtime, computed_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params = list(dataset_id, collection_seal, sample_ids[i], "",
                       content_hashes[i],
                       content_hashes[i], 0L, 0, now))
    }
    DBI::dbExecute(db, "COMMIT")
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    stop(e)
  })
}

# =============================================================================
# Sample manifests (multi-file sample support)
# =============================================================================

#' Upsert a sample manifest
#'
#' Describes the structure of a sample (single file, DICOM series, bundle).
#'
#' @param dataset_id Character.
#' @param collection_seal Character; immutable collection-snapshot seal.
#' @param sample_id Character.
#' @param source_kind Character; "single_file", "dicom_series", "multi_file_bundle".
#' @param manifest Named list with file references.
#' @param content_hash Character or NULL; overall content hash.
#' @export
upsert_sample_manifest <- function(dataset_id, collection_seal, sample_id, source_kind,
                                    manifest, content_hash = NULL) {
  collection_seal <- .asset_collection_seal(collection_seal, required = TRUE)
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  mj <- as.character(jsonlite::toJSON(manifest, auto_unbox = TRUE))
  DBI::dbExecute(db,
    "INSERT OR REPLACE INTO sample_manifests
     (dataset_id, collection_seal, sample_id, source_kind, manifest_json, content_hash,
      created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    params = list(dataset_id, collection_seal, sample_id, source_kind, mj,
                   content_hash %||% NA_character_, now, now))
}

#' Get a sample manifest
#'
#' @param dataset_id Character; dataset identifier.
#' @param collection_seal Character; immutable collection-snapshot seal.
#' @param sample_id Character; sample identifier.
#' @return Named list or NULL.
#' @export
get_sample_manifest <- function(dataset_id, collection_seal, sample_id) {
  collection_seal <- .asset_collection_seal(collection_seal, required = TRUE)
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  row <- DBI::dbGetQuery(db,
    "SELECT * FROM sample_manifests
     WHERE dataset_id = ? AND collection_seal = ? AND sample_id = ?",
    params = list(dataset_id, collection_seal, sample_id))
  if (nrow(row) == 0) return(NULL)
  out <- as.list(row[1, ])
  out$manifest <- jsonlite::fromJSON(out$manifest_json, simplifyVector = FALSE)
  out$manifest_json <- NULL
  out
}

#' Get the primary file path for a sample
#'
#' Resolves from manifest if available, otherwise returns NULL.
#' Backward-compatible: works with single_file samples.
#'
#' @param dataset_id Character; dataset identifier.
#' @param collection_seal Character; immutable collection-snapshot seal.
#' @param sample_id Character; sample identifier.
#' @return Character or NULL.
#' @export
get_sample_primary_path <- function(dataset_id, collection_seal, sample_id) {
  sm <- get_sample_manifest(dataset_id, collection_seal, sample_id)
  if (is.null(sm)) return(NULL)
  sm$manifest$primary_path %||% sm$manifest$files[[1]]$path
}

#' List sample manifests for a dataset
#'
#' @param dataset_id Character; dataset identifier.
#' @param collection_seal Character; immutable collection-snapshot seal.
#' @return Data.frame.
#' @export
list_sample_manifests <- function(dataset_id, collection_seal) {
  collection_seal <- .asset_collection_seal(collection_seal, required = TRUE)
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  DBI::dbGetQuery(db,
    "SELECT sample_id, source_kind, content_hash, updated_at
     FROM sample_manifests WHERE dataset_id = ? AND collection_seal = ?
     ORDER BY sample_id",
    params = list(dataset_id, collection_seal))
}
