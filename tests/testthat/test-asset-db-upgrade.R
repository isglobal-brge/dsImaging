legacy_assets_sql <- function(include_visibility = TRUE,
                              include_explicit = FALSE) {
  columns <- c(
    "asset_id TEXT PRIMARY KEY",
    "dataset_id TEXT NOT NULL",
    "kind TEXT NOT NULL",
    "modality TEXT",
    "status TEXT NOT NULL DEFAULT 'active'"
  )
  if (isTRUE(include_visibility)) {
    columns <- c(columns,
      "visibility TEXT NOT NULL DEFAULT 'global'")
  }
  if (isTRUE(include_explicit)) {
    columns <- c(columns,
      "visibility_explicit INTEGER NOT NULL DEFAULT 0")
  }
  columns <- c(columns,
    "storage_backend TEXT NOT NULL DEFAULT 'file'",
    "derivation_hash TEXT",
    "created_at TEXT NOT NULL",
    "created_by TEXT",
    "created_by_job TEXT",
    "path_or_root TEXT",
    "description TEXT",
    "manifest_json TEXT",
    "provenance_json TEXT",
    "tags TEXT")
  paste0("CREATE TABLE assets (", paste(columns, collapse = ","), ")")
}

test_that("legacy asset catalogs default every unproven asset to private", {
  path <- tempfile(fileext = ".sqlite")
  db <- DBI::dbConnect(RSQLite::SQLite(), path)
  DBI::dbExecute(db, legacy_assets_sql())
  DBI::dbExecute(db, paste(
    "INSERT INTO assets",
    "(asset_id,dataset_id,kind,status,visibility,storage_backend,created_at)",
    "VALUES ('legacy','lung','feature_table','active','global','file','now')"))
  DBI::dbDisconnect(db)

  withr::local_options(list(dsimaging.asset_db = path))
  upgraded <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(upgraded), add = TRUE)
  row <- DBI::dbGetQuery(upgraded,
    "SELECT visibility, visibility_explicit FROM assets WHERE asset_id='legacy'")
  expect_equal(row$visibility, "private")
  expect_equal(row$visibility_explicit, 0L)
})

test_that("catalogs predating the visibility column upgrade as private", {
  path <- tempfile(fileext = ".sqlite")
  db <- DBI::dbConnect(RSQLite::SQLite(), path)
  DBI::dbExecute(db, legacy_assets_sql(include_visibility = FALSE))
  DBI::dbExecute(db, paste(
    "INSERT INTO assets",
    "(asset_id,dataset_id,kind,status,storage_backend,created_at)",
    "VALUES ('ancient','lung','feature_table','active','file','now')"))
  DBI::dbDisconnect(db)

  withr::local_options(list(dsimaging.asset_db = path))
  upgraded <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(upgraded), add = TRUE)
  row <- DBI::dbGetQuery(upgraded, paste(
    "SELECT visibility, visibility_explicit FROM assets",
    "WHERE asset_id='ancient'"))
  expect_equal(row$visibility, "private")
  expect_equal(row$visibility_explicit, 0L)
})

test_that("an unsealed visibility signal becomes private on upgrade", {
  path <- tempfile(fileext = ".sqlite")
  db <- DBI::dbConnect(RSQLite::SQLite(), path)
  DBI::dbExecute(db, legacy_assets_sql(include_explicit = TRUE))
  DBI::dbExecute(db, paste(
    "INSERT INTO assets",
    "(asset_id,dataset_id,kind,status,visibility,visibility_explicit,",
    "storage_backend,created_at)",
    "VALUES ('explicit','lung','feature_table','active','global',1,'file','now')"))
  DBI::dbDisconnect(db)

  withr::local_options(list(dsimaging.asset_db = path))
  upgraded <- dsImaging:::.asset_db_connect()
  dsImaging:::.asset_db_close(upgraded)
  reopened <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(reopened), add = TRUE)
  row <- DBI::dbGetQuery(reopened,
    "SELECT visibility, visibility_explicit FROM assets WHERE asset_id='explicit'")
  expect_equal(row$visibility, "private")
  expect_equal(row$visibility_explicit, 1L)
})

test_that("a legacy unsealed alias remains private", {
  path <- tempfile(fileext = ".sqlite")
  db <- DBI::dbConnect(RSQLite::SQLite(), path)
  DBI::dbExecute(db, legacy_assets_sql())
  DBI::dbExecute(db, paste(
    "INSERT INTO assets",
    "(asset_id,dataset_id,kind,status,visibility,storage_backend,created_at)",
    "VALUES ('aliased','lung','feature_table','active','global','file','now')"))
  DBI::dbExecute(db, paste0(
    "CREATE TABLE asset_aliases (dataset_id TEXT NOT NULL, alias TEXT NOT NULL, ",
    "asset_id TEXT NOT NULL, updated_at TEXT NOT NULL, ",
    "PRIMARY KEY (dataset_id, alias))"))
  DBI::dbExecute(db, paste(
    "INSERT INTO asset_aliases (dataset_id,alias,asset_id,updated_at)",
    "VALUES ('lung','published','aliased','now')"))
  DBI::dbDisconnect(db)

  withr::local_options(list(dsimaging.asset_db = path))
  upgraded <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(upgraded), add = TRUE)
  row <- DBI::dbGetQuery(upgraded, paste(
    "SELECT visibility, visibility_explicit FROM assets",
    "WHERE asset_id='aliased'"))
  expect_equal(row$visibility, "private")
  expect_equal(row$visibility_explicit, 0L)
  expect_equal(nrow(dsImaging:::.asset_list(
    upgraded, "lung", visibility = "global",
    collection_seal = strrep("a", 64))), 0L)
  expect_null(dsImaging:::.asset_resolve_alias(
    upgraded, "lung", "published", collection_seal = strrep("a", 64)))
})

test_that("reopening a sealed private aliased asset preserves privacy", {
  path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(dsimaging.asset_db = path))
  db <- dsImaging:::.asset_db_connect()
  seal <- strrep("a", 64)
  asset_id <- dsImaging:::.asset_register(
    db, "lung", "feature_table", "/features", visibility = "private",
    collection_seal = seal)
  DBI::dbExecute(db, paste(
    "INSERT INTO asset_aliases",
    "(dataset_id,collection_seal,alias,asset_id,updated_at)",
    "VALUES (?,?,?,?,?)"),
    params = list("lung", seal, "private_features", asset_id, "now"))
  dsImaging:::.asset_db_close(db)

  reopened <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(reopened), add = TRUE)
  row <- dsImaging:::.asset_get(reopened, asset_id)
  expect_equal(row$visibility, "private")
  expect_equal(nrow(dsImaging:::.asset_list(
    reopened, "lung", visibility = "global", collection_seal = seal)), 0L)
  expect_null(dsImaging:::.asset_resolve_alias(
    reopened, "lung", "private_features", collection_seal = seal))
})

test_that("new global assets persist only with explicit provenance", {
  path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(dsimaging.asset_db = path))
  db <- dsImaging:::.asset_db_connect()
  asset_id <- dsImaging:::.asset_register(
    db, "lung", "feature_table", "/features", visibility = "global",
    collection_seal = strrep("a", 64))
  dsImaging:::.asset_db_close(db)

  reopened <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(reopened), add = TRUE)
  row <- dsImaging:::.asset_get(reopened, asset_id)
  expect_equal(row$visibility, "global")
  expect_equal(row$visibility_explicit, 1L)
  expect_equal(row$collection_seal, strrep("a", 64))
})

test_that("legacy generation items gain durable private job correlation", {
  path <- tempfile(fileext = ".sqlite")
  db <- DBI::dbConnect(RSQLite::SQLite(), path)
  DBI::dbExecute(db, paste(
    "CREATE TABLE asset_items (",
    "generation_id TEXT NOT NULL, sample_id TEXT NOT NULL,",
    "status TEXT NOT NULL DEFAULT 'pending', artifact_relpath TEXT,",
    "checksum TEXT, error TEXT, PRIMARY KEY (generation_id, sample_id))"))
  DBI::dbExecute(db, paste(
    "INSERT INTO asset_items (generation_id, sample_id, status)",
    "VALUES ('gen_legacy', 'PHI_PATIENT_701_SCAN_A', 'pending')"))
  DBI::dbDisconnect(db)

  withr::local_options(list(dsimaging.asset_db = path))
  upgraded <- dsImaging:::.asset_db_connect()
  fields <- DBI::dbListFields(upgraded, "asset_items")
  dsImaging:::.asset_db_close(upgraded)
  expect_true(all(c("claimed_by", "claimed_at", "job_token") %in% fields))

  token <- dsImaging:::.item_job_token(
    "gen_legacy", "PHI_PATIENT_701_SCAN_A")
  reopened <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(reopened), add = TRUE)
  stored <- DBI::dbGetQuery(reopened,
    "SELECT job_token FROM asset_items WHERE generation_id = 'gen_legacy'")
  expect_identical(stored$job_token[[1L]], token)
  expect_match(token, "^[0-9a-f]{32}$")
})
