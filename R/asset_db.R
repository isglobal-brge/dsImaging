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
  if (!is.null(reg_path) && nzchar(reg_path))
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
      description      TEXT,
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

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS asset_generations (
      generation_id     TEXT PRIMARY KEY,
      dataset_id        TEXT NOT NULL,
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
      spec_json         TEXT,
      error             TEXT,
      created_at        TEXT NOT NULL,
      updated_at        TEXT NOT NULL
    )")

  # Status state machine: pending -> claimed -> running -> completed | failed | skipped
  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS asset_items (
      generation_id     TEXT NOT NULL,
      sample_id         TEXT NOT NULL,
      status            TEXT NOT NULL DEFAULT 'pending',
      claimed_by        TEXT,
      claimed_at        TEXT,
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
  }, error = function(e) NULL)

  # Sample manifests: abstracts multi-file samples (DICOM series, bundles)
  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS sample_manifests (
      dataset_id     TEXT NOT NULL,
      sample_id      TEXT NOT NULL,
      source_kind    TEXT NOT NULL DEFAULT 'single_file',
      manifest_json  TEXT NOT NULL,
      content_hash   TEXT,
      created_at     TEXT NOT NULL,
      updated_at     TEXT NOT NULL,
      PRIMARY KEY (dataset_id, sample_id)
    )")

  DBI::dbExecute(db, "
    CREATE TABLE IF NOT EXISTS content_fingerprints (
      dataset_id    TEXT NOT NULL,
      sample_id     TEXT NOT NULL,
      file_path     TEXT NOT NULL,
      fingerprint   TEXT NOT NULL,
      content_hash  TEXT,
      file_size     INTEGER NOT NULL,
      file_mtime    REAL NOT NULL,
      computed_at   TEXT NOT NULL,
      PRIMARY KEY (dataset_id, sample_id)
    )")

  # Schema migration for existing DBs
  tryCatch({
    cols <- DBI::dbListFields(db, "content_fingerprints")
    if (!"content_hash" %in% cols)
      DBI::dbExecute(db, "ALTER TABLE content_fingerprints ADD COLUMN content_hash TEXT")
  }, error = function(e) NULL)

  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_assets_dataset ON assets(dataset_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_assets_hash ON assets(derivation_hash)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_assets_kind ON assets(dataset_id, kind)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_gen_hash ON asset_generations(dataset_id, derivation_hash)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_gen_state ON asset_generations(state)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_items_gen ON asset_items(generation_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_fp_dataset ON content_fingerprints(dataset_id)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_fp_fingerprint ON content_fingerprints(fingerprint)")
  DBI::dbExecute(db, "CREATE INDEX IF NOT EXISTS idx_fp_content_hash ON content_fingerprints(content_hash)")
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
                             manifest = NULL,
                             description = NULL) {
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
                           created_by_job, path_or_root, description,
                           manifest_json, provenance_json, tags)
       VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      params = list(asset_id, dataset_id, kind, modality %||% NA_character_,
        visibility, derivation_hash %||% NA_character_, now,
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

#' Find an active asset by derivation hash
#'
#' Public wrapper for deduplication lookups.
#'
#' @param dataset_id Character; the dataset.
#' @param derivation_hash Character; the hash to look up.
#' @return Character asset_id if found, NULL otherwise.
#' @export
find_asset_by_hash <- function(dataset_id, derivation_hash) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  .asset_find_by_hash(db, dataset_id, derivation_hash)
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
  params <- params[order(names(params))]
  blob <- jsonlite::toJSON(params, auto_unbox = TRUE, digits = 10)
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
#' @return Named list with action, asset_id or generation_id.
#' @export
claim_or_reuse_generation <- function(dataset_id, kind, derivation_hash,
                                       visibility = "global", owner_id = NULL,
                                       job_id = NULL, expected_n = NULL,
                                       spec = NULL) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))

  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    # 1. Active asset with this hash?
    existing_asset <- .asset_find_by_hash(db, dataset_id, derivation_hash)
    if (!is.null(existing_asset)) {
      DBI::dbExecute(db, "COMMIT")
      return(list(action = "reuse_asset", asset_id = existing_asset))
    }

    # 2. Running/pending generation (not stale)?
    gen <- DBI::dbGetQuery(db,
      "SELECT generation_id, state, published_asset_id, updated_at
       FROM asset_generations
       WHERE dataset_id = ? AND derivation_hash = ? AND state IN ('PENDING','RUNNING')
       LIMIT 1",
      params = list(dataset_id, derivation_hash))
    if (nrow(gen) > 0) {
      # Check if stale (>2 hours old with no update)
      updated <- tryCatch(
        as.POSIXct(gen$updated_at[1], format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC"),
        error = function(e) Sys.time())
      age_hours <- as.numeric(difftime(Sys.time(), updated, units = "hours"))
      if (age_hours < 2) {
        DBI::dbExecute(db, "COMMIT")
        return(list(action = "reuse_generation", generation_id = gen$generation_id[1],
                     state = gen$state[1]))
      }
      # Stale generation -- mark as FAILED and continue to create new
      DBI::dbExecute(db,
        "UPDATE asset_generations SET state = 'FAILED',
         error = 'Stale generation (>2h without update)', updated_at = ?
         WHERE generation_id = ?",
        params = list(format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"),
                       gen$generation_id[1]))
    }

    # 3. Create new generation
    hex <- paste(sample(c(0:9, letters[1:6]), 8, replace = TRUE), collapse = "")
    gen_id <- paste0("gen_", format(Sys.time(), "%Y%m%d_%H%M%S"), "_", hex)
    now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
    spec_json <- if (!is.null(spec))
      as.character(jsonlite::toJSON(spec, auto_unbox = TRUE)) else NA_character_

    DBI::dbExecute(db,
      "INSERT INTO asset_generations (generation_id, dataset_id, kind, derivation_hash,
        visibility, state, owner_id, created_by_job, expected_n, spec_json,
        created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, 'PENDING', ?, ?, ?, ?, ?, ?)",
      params = list(gen_id, dataset_id, kind, derivation_hash,
        visibility, owner_id %||% NA_character_, job_id %||% NA_character_,
        as.integer(expected_n %||% NA_integer_), spec_json, now, now))

    DBI::dbExecute(db, "COMMIT")
    list(action = "run_new", generation_id = gen_id)
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    stop(e)
  })
}

#' Update generation state
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
#' @export
record_item_status <- function(generation_id, sample_id, status,
                                artifact_relpath = NULL, checksum = NULL,
                                error = NULL) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  DBI::dbExecute(db,
    "INSERT OR REPLACE INTO asset_items (generation_id, sample_id, status,
      artifact_relpath, checksum, error)
     VALUES (?, ?, ?, ?, ?, ?)",
    params = list(generation_id, sample_id, status,
      artifact_relpath %||% NA_character_,
      checksum %||% NA_character_,
      error %||% NA_character_))
}

#' Complete a per-image item atomically
#'
#' Performs ALL of these in a single SQLite transaction:
#'   1. Update asset_items row (status, artifact, error)
#'   2. Increment the appropriate generation counter (completed_n or failed_n)
#'   3. Optionally register a per-image asset (for cross-user dedup)
#'
#' This eliminates the race condition where a crash between separate
#' record_item_status + increment_generation_counter calls leaves
#' counters out of sync.
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

  counter_field <- if (status == "completed") "completed_n" else "failed_n"
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")

  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))

  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    # 1. Update item status
    DBI::dbExecute(db,
      "INSERT OR REPLACE INTO asset_items (generation_id, sample_id, status,
        artifact_relpath, checksum, error)
       VALUES (?, ?, ?, ?, ?, ?)",
      params = list(generation_id, sample_id, status,
        artifact_relpath %||% NA_character_,
        checksum %||% NA_character_,
        error %||% NA_character_))

    # 2. Atomically increment counter
    if (status != "skipped") {
      DBI::dbExecute(db,
        paste0("UPDATE asset_generations SET ", counter_field, " = ", counter_field,
               " + 1, updated_at = ? WHERE generation_id = ?"),
        params = list(now, generation_id))
    }

    # 3. Optionally register per-image asset (cross-user dedup)
    if (!is.null(register_asset)) {
      .asset_register(db,
        dataset_id = register_asset$dataset_id,
        kind = register_asset$kind %||% "per_image_result",
        path_or_root = register_asset$path_or_root,
        derivation_hash = register_asset$derivation_hash,
        provenance = register_asset$provenance,
        created_by_job = register_asset$created_by_job,
        description = register_asset$description %||%
          paste0("Per-image result: ", sample_id))
    }

    DBI::dbExecute(db, "COMMIT")
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    stop(e)
  })

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

#' Get generation summary
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

#' Publish a completed generation as an active asset
#'
#' Only if completed_n == expected_n (no partial publishes).
#'
#' @return Character; the published asset_id, or NULL if validation fails.
#' @export
#' Mark a generation as FAILED (called when the job fails)
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

#' Clean up stale generations (called periodically)
#' @export
cleanup_stale_generations <- function(max_age_hours = 2) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  cutoff <- format(Sys.time() - max_age_hours * 3600,
    "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  n <- DBI::dbExecute(db,
    "UPDATE asset_generations SET state = 'FAILED',
     error = 'Stale generation cleaned up', updated_at = ?
     WHERE state IN ('PENDING', 'RUNNING') AND updated_at < ?",
    params = list(format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC"), cutoff))
  n
}

#' Publish a completed generation as an active asset
publish_generation <- function(generation_id, path_or_root, description = NULL,
                                parent_asset_ids = character(0),
                                provenance = NULL, alias = NULL) {
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

  # Register as active asset
  asset_id <- .asset_register(db, gen$dataset_id, gen$kind, path_or_root,
    derivation_hash = gen$derivation_hash,
    parent_asset_ids = parent_asset_ids,
    provenance = provenance,
    created_by = gen$owner_id,
    created_by_job = gen$created_by_job,
    visibility = gen$visibility,
    description = description)

  # Update generation
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  DBI::dbExecute(db,
    "UPDATE asset_generations SET state = 'ACTIVE', published_asset_id = ?,
     updated_at = ? WHERE generation_id = ?",
    params = list(asset_id, now, generation_id))

  if (!is.null(alias)) {
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
#' @param image_root Character; path to the image directory.
#' @return Named list: new (character), changed (character),
#'   unchanged (character), fingerprints (named list of fp strings).
#' @export
compute_collection_fingerprints <- function(dataset_id, image_root,
                                              compute_content_hash = TRUE) {
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
    "SELECT sample_id, fingerprint FROM content_fingerprints WHERE dataset_id = ?",
    params = list(dataset_id))
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
         (dataset_id, sample_id, file_path, fingerprint, content_hash,
          file_size, file_mtime, computed_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        params = list(dataset_id, sid, entry$file_path, fp,
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

# =============================================================================
# Sample manifests (multi-file sample support)
# =============================================================================

#' Upsert a sample manifest
#'
#' Describes the structure of a sample (single file, DICOM series, bundle).
#'
#' @param dataset_id Character.
#' @param sample_id Character.
#' @param source_kind Character; "single_file", "dicom_series", "multi_file_bundle".
#' @param manifest Named list with file references.
#' @param content_hash Character or NULL; overall content hash.
#' @export
upsert_sample_manifest <- function(dataset_id, sample_id, source_kind,
                                    manifest, content_hash = NULL) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  mj <- as.character(jsonlite::toJSON(manifest, auto_unbox = TRUE))
  DBI::dbExecute(db,
    "INSERT OR REPLACE INTO sample_manifests
     (dataset_id, sample_id, source_kind, manifest_json, content_hash,
      created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)",
    params = list(dataset_id, sample_id, source_kind, mj,
                   content_hash %||% NA_character_, now, now))
}

#' Get a sample manifest
#' @return Named list or NULL.
#' @export
get_sample_manifest <- function(dataset_id, sample_id) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  row <- DBI::dbGetQuery(db,
    "SELECT * FROM sample_manifests WHERE dataset_id = ? AND sample_id = ?",
    params = list(dataset_id, sample_id))
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
#' @return Character or NULL.
#' @export
get_sample_primary_path <- function(dataset_id, sample_id) {
  sm <- get_sample_manifest(dataset_id, sample_id)
  if (is.null(sm)) return(NULL)
  sm$manifest$primary_path %||% sm$manifest$files[[1]]$path
}

#' List sample manifests for a dataset
#' @return Data.frame.
#' @export
list_sample_manifests <- function(dataset_id) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  DBI::dbGetQuery(db,
    "SELECT sample_id, source_kind, content_hash, updated_at
     FROM sample_manifests WHERE dataset_id = ? ORDER BY sample_id",
    params = list(dataset_id))
}
