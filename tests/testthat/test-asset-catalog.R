test_that("asset catalog database is created with correct schema", {
  tmp <- tempfile(fileext = ".sqlite")
  on.exit(unlink(tmp))
  withr::local_options(list(dsimaging.asset_db = tmp))

  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)

  tables <- DBI::dbListTables(db)
  expect_true("assets" %in% tables)
  expect_true("asset_parents" %in% tables)
  expect_true("asset_aliases" %in% tables)
})

test_that("asset registration and retrieval works", {
  tmp <- tempfile(fileext = ".sqlite")
  on.exit(unlink(tmp))
  withr::local_options(list(dsimaging.asset_db = tmp))

  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)

  aid <- dsImaging:::.asset_register(db, "chest_xray.v1", "feature_table",
    "/data/radiomics.parquet",
    derivation_hash = "abc123",
    provenance = list(runner = "pyradiomics", version = "3.0.1"),
    created_by = "researcher1")

  expect_true(startsWith(aid, "asset_"))

  # Retrieve
  asset <- dsImaging:::.asset_get(db, aid)
  expect_equal(asset$dataset_id, "chest_xray.v1")
  expect_equal(asset$kind, "feature_table")
  expect_equal(asset$derivation_hash, "abc123")
})

test_that("deduplication by derivation_hash works", {
  tmp <- tempfile(fileext = ".sqlite")
  on.exit(unlink(tmp))
  withr::local_options(list(dsimaging.asset_db = tmp))

  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)

  aid1 <- dsImaging:::.asset_register(db, "ds.v1", "feature_table",
    "/data/radio1.parquet", derivation_hash = "samehash",
    visibility = "global", collection_seal = strrep("a", 64))

  aid2 <- dsImaging:::.asset_register(db, "ds.v1", "feature_table",
    "/data/radio2.parquet", derivation_hash = "samehash",
    visibility = "global", collection_seal = strrep("a", 64))

  # Should return the same asset_id (deduplicated)
  expect_equal(aid1, aid2)

  # Different hash = different asset
  aid3 <- dsImaging:::.asset_register(db, "ds.v1", "feature_table",
    "/data/radio3.parquet", derivation_hash = "differenthash",
    visibility = "global", collection_seal = strrep("a", 64))

  expect_false(aid1 == aid3)

  # The same logical dataset and derivation belong to distinct immutable
  # collections when their seals differ.
  aid4 <- dsImaging:::.asset_register(db, "ds.v1", "feature_table",
    "/data/radio4.parquet", derivation_hash = "samehash",
    visibility = "global", collection_seal = strrep("b", 64))

  expect_false(aid1 == aid4)
})

test_that("private assets are isolated from deduplication and aliases", {
  tmp <- tempfile(fileext = ".sqlite")
  on.exit(unlink(tmp))
  withr::local_options(list(dsimaging.asset_db = tmp))
  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)

  first <- dsImaging:::.asset_register(
    db, "ds.v1", "feature_table", "/data/private-a.csv",
    derivation_hash = "private-hash")
  second <- dsImaging:::.asset_register(
    db, "ds.v1", "feature_table", "/data/private-b.csv",
    derivation_hash = "private-hash")

  expect_false(identical(first, second))
  expect_equal(dsImaging:::.asset_get(db, first)$visibility, "private")
  expect_null(dsImaging:::.asset_find_by_hash(db, "ds.v1", "private-hash"))
  expect_error(
    dsImaging:::.asset_set_alias(db, "ds.v1", "private", first),
    "active global asset")
})

test_that("multiple assets of same kind coexist", {
  tmp <- tempfile(fileext = ".sqlite")
  on.exit(unlink(tmp))
  withr::local_options(list(dsimaging.asset_db = tmp))

  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)

  # Three different radiomics extractions
  dsImaging:::.asset_register(db, "ds.v1", "feature_table",
    "/data/radio_unet.parquet", derivation_hash = "hash_unet",
    provenance = list(mask = "unet_v1"), visibility = "global",
    collection_seal = strrep("a", 64))
  dsImaging:::.asset_register(db, "ds.v1", "feature_table",
    "/data/radio_manual.parquet", derivation_hash = "hash_manual",
    provenance = list(mask = "manual"), visibility = "global",
    collection_seal = strrep("a", 64))
  dsImaging:::.asset_register(db, "ds.v1", "feature_table",
    "/data/radio_nnunet.parquet", derivation_hash = "hash_nnunet",
    provenance = list(mask = "nnunet_v2"), visibility = "global",
    collection_seal = strrep("a", 64))

  assets <- dsImaging:::.asset_list(db, "ds.v1", kind = "feature_table",
    collection_seal = strrep("a", 64))
  expect_equal(nrow(assets), 3L)
})

test_that("aliases work", {
  tmp <- tempfile(fileext = ".sqlite")
  on.exit(unlink(tmp))
  withr::local_options(list(dsimaging.asset_db = tmp))

  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)

  aid1 <- dsImaging:::.asset_register(db, "ds.v1", "mask_root",
    "/data/masks_unet", derivation_hash = "h1", visibility = "global",
    collection_seal = strrep("a", 64))
  aid2 <- dsImaging:::.asset_register(db, "ds.v1", "mask_root",
    "/data/masks_nnunet", derivation_hash = "h2", visibility = "global",
    collection_seal = strrep("a", 64))

  # Set alias
  dsImaging:::.asset_set_alias(db, "ds.v1", "default_lung_mask", aid1)

  # Resolve
  resolved <- dsImaging:::.asset_resolve_alias(db, "ds.v1",
    "default_lung_mask", collection_seal = strrep("a", 64))
  expect_equal(resolved, aid1)

  # Update alias to point to different asset
  dsImaging:::.asset_set_alias(db, "ds.v1", "default_lung_mask", aid2)
  resolved2 <- dsImaging:::.asset_resolve_alias(db, "ds.v1",
    "default_lung_mask", collection_seal = strrep("a", 64))
  expect_equal(resolved2, aid2)
})

test_that("the same alias is isolated across collection seals", {
  tmp <- tempfile(fileext = ".sqlite")
  withr::local_options(list(dsimaging.asset_db = tmp))
  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  seal_a <- strrep("a", 64)
  seal_b <- strrep("b", 64)
  asset_a <- dsImaging:::.asset_register(
    db, "shared", "feature_table", "/a.csv", visibility = "global",
    collection_seal = seal_a)
  asset_b <- dsImaging:::.asset_register(
    db, "shared", "feature_table", "/b.csv", visibility = "global",
    collection_seal = seal_b)
  dsImaging:::.asset_set_alias(db, "shared", "published", asset_a)
  dsImaging:::.asset_set_alias(db, "shared", "published", asset_b)

  expect_identical(
    dsImaging:::.asset_resolve_alias(db, "shared", "published", seal_a),
    asset_a)
  expect_identical(
    dsImaging:::.asset_resolve_alias(db, "shared", "published", seal_b),
    asset_b)
})

test_that("lineage tracking works", {
  tmp <- tempfile(fileext = ".sqlite")
  on.exit(unlink(tmp))
  withr::local_options(list(dsimaging.asset_db = tmp))

  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)

  # Base assets
  img_id <- dsImaging:::.asset_register(db, "ds.v1", "image_root",
    "/data/images", derivation_hash = "img_h")
  mask_id <- dsImaging:::.asset_register(db, "ds.v1", "mask_root",
    "/data/masks", derivation_hash = "mask_h")

  # Derived: radiomics from image + mask
  radio_id <- dsImaging:::.asset_register(db, "ds.v1", "feature_table",
    "/data/radiomics.parquet", derivation_hash = "radio_h",
    parent_asset_ids = c(img_id, mask_id))

  # Check lineage
  lineage <- dsImaging:::.asset_get_lineage(db, radio_id)
  expect_equal(nrow(lineage), 2L)
  expect_true(img_id %in% lineage$parent_asset_id)
  expect_true(mask_id %in% lineage$parent_asset_id)
})

test_that("lineage cannot cross immutable collection seals", {
  tmp <- tempfile(fileext = ".sqlite")
  withr::local_options(list(dsimaging.asset_db = tmp))
  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)

  parent <- dsImaging:::.asset_register(
    db, "shared", "image_root", "/a", visibility = "global",
    collection_seal = strrep("a", 64))

  expect_error(
    dsImaging:::.asset_register(
      db, "shared", "feature_table", "/b", visibility = "global",
      collection_seal = strrep("b", 64), parent_asset_ids = parent),
    "collection boundary")
})

test_that("compute_derivation_hash is deterministic", {
  h1 <- compute_derivation_hash(
    dataset_id = "ds.v1",
    mask_asset = "masks.unet.v1",
    pyradiomics_version = "3.0.1",
    bin_width = 25,
    feature_classes = c("firstorder", "glcm", "glrlm")
  )
  h2 <- compute_derivation_hash(
    dataset_id = "ds.v1",
    mask_asset = "masks.unet.v1",
    pyradiomics_version = "3.0.1",
    bin_width = 25,
    feature_classes = c("firstorder", "glcm", "glrlm")
  )
  expect_equal(h1, h2)

  # Different params = different hash
  h3 <- compute_derivation_hash(
    dataset_id = "ds.v1",
    mask_asset = "masks.unet.v1",
    pyradiomics_version = "3.0.1",
    bin_width = 50,
    feature_classes = c("firstorder", "glcm", "glrlm")
  )
  expect_false(h1 == h3)
})

test_that("old pending and running global generations are reused", {
  tmp <- tempfile(fileext = ".sqlite")
  on.exit(unlink(tmp))
  withr::local_options(list(dsimaging.asset_db = tmp))

  for (state in c("PENDING", "RUNNING")) {
    derivation_hash <- paste0("hash_old_", tolower(state))
    first <- claim_or_reuse_generation(
      "ds.v1", "feature_table", derivation_hash,
      collection_seal = strrep("a", 64), visibility = "global",
      owner_id = "tester", expected_n = 1L)
    if (identical(state, "RUNNING")) {
      update_generation(first$generation_id, state = state)
    }

    db <- dsImaging:::.asset_db_connect()
    DBI::dbExecute(db,
      "UPDATE asset_generations SET updated_at = '2000-01-01T00:00:00.000Z'
       WHERE generation_id = ?",
      params = list(first$generation_id))
    dsImaging:::.asset_db_close(db)

    second <- claim_or_reuse_generation(
      "ds.v1", "feature_table", derivation_hash,
      collection_seal = strrep("a", 64), visibility = "global",
      owner_id = "other", expected_n = 1L)

    expect_identical(second$action, "reuse_generation")
    expect_identical(second$generation_id, first$generation_id)
    expect_identical(second$state, state)
    expect_identical(get_generation(first$generation_id)$state, state)

    db <- dsImaging:::.asset_db_connect()
    count <- DBI::dbGetQuery(db,
      "SELECT COUNT(*) AS n FROM asset_generations
       WHERE dataset_id = ? AND derivation_hash = ?",
      params = list("ds.v1", derivation_hash))$n[[1]]
    dsImaging:::.asset_db_close(db)
    expect_equal(count, 1L)
  }
})

test_that("global generations are isolated by immutable collection seal", {
  tmp <- tempfile(fileext = ".sqlite")
  withr::local_options(list(dsimaging.asset_db = tmp))

  first <- claim_or_reuse_generation(
    "shared", "feature_table", "same-hash",
    collection_seal = strrep("a", 64), visibility = "global",
    owner_id = "tester", expected_n = 1L)
  second <- claim_or_reuse_generation(
    "shared", "feature_table", "same-hash",
    collection_seal = strrep("b", 64), visibility = "global",
    owner_id = "tester", expected_n = 1L)

  expect_identical(first$action, "run_new")
  expect_identical(second$action, "run_new")
  expect_false(identical(first$generation_id, second$generation_id))
})

test_that("content hashes are isolated by immutable collection seal", {
  tmp <- tempfile(fileext = ".sqlite")
  withr::local_options(list(dsimaging.asset_db = tmp))
  seal_a <- strrep("a", 64)
  seal_b <- strrep("b", 64)

  store_content_hashes("shared", seal_a, "sample", "hash-a")
  store_content_hashes("shared", seal_b, "sample", "hash-b")

  expect_identical(get_content_hashes("shared", seal_a, "sample")$sample,
                   "hash-a")
  expect_identical(get_content_hashes("shared", seal_b, "sample")$sample,
                   "hash-b")
})

test_that("legacy stale-generation cleanup does not change active work", {
  tmp <- tempfile(fileext = ".sqlite")
  on.exit(unlink(tmp))
  withr::local_options(list(dsimaging.asset_db = tmp))

  generation_id <- claim_or_reuse_generation(
    "ds.v1", "feature_table", "hash_cleanup_noop",
    collection_seal = strrep("a", 64), visibility = "global",
    owner_id = "tester", expected_n = 1L
  )$generation_id
  update_generation(generation_id, state = "RUNNING")

  db <- dsImaging:::.asset_db_connect()
  DBI::dbExecute(db,
    "UPDATE asset_generations SET updated_at = '2000-01-01T00:00:00.000Z'
     WHERE generation_id = ?",
    params = list(generation_id))
  dsImaging:::.asset_db_close(db)

  expect_identical(cleanup_stale_generations(max_age_hours = 0), 0L)
  expect_identical(get_generation(generation_id)$state, "RUNNING")
})

test_that("item completion is idempotent and completion wins late failures", {
  tmp <- tempfile(fileext = ".sqlite")
  on.exit(unlink(tmp))
  withr::local_options(list(dsimaging.asset_db = tmp))

  gen <- claim_or_reuse_generation("ds.v1", "feature_table", "hash_items",
    owner_id = "tester", expected_n = 2L)$generation_id
  record_item_status(gen, "sample_a", "running")
  record_item_status(gen, "sample_b", "running")

  complete_item_atomic(gen, "sample_a", "completed",
    artifact_relpath = "/out/a.parquet")
  complete_item_atomic(gen, "sample_a", "completed",
    artifact_relpath = "/out/a.parquet")
  complete_item_atomic(gen, "sample_a", "failed", error = "late duplicate")

  g1 <- get_generation(gen)
  expect_equal(g1$completed_n, 1L)
  expect_equal(g1$failed_n, 0L)
  items1 <- get_generation_items(gen)
  expect_equal(items1$status[items1$sample_id == "sample_a"], "completed")

  complete_item_atomic(gen, "sample_b", "failed", error = "first attempt")
  g2 <- get_generation(gen)
  expect_equal(g2$completed_n, 1L)
  expect_equal(g2$failed_n, 1L)

  complete_item_atomic(gen, "sample_b", "completed",
    artifact_relpath = "/out/b.parquet")
  g3 <- get_generation(gen)
  expect_equal(g3$completed_n, 2L)
  expect_equal(g3$failed_n, 0L)
})

test_that("generation counter reconciliation repairs stale counts", {
  tmp <- tempfile(fileext = ".sqlite")
  on.exit(unlink(tmp))
  withr::local_options(list(dsimaging.asset_db = tmp))

  gen <- claim_or_reuse_generation("ds.v1", "feature_table", "hash_reconcile",
    owner_id = "tester", expected_n = 2L)$generation_id
  complete_item_atomic(gen, "sample_a", "completed",
    artifact_relpath = "/out/a.parquet")

  db <- dsImaging:::.asset_db_connect()
  DBI::dbExecute(db,
    "UPDATE asset_generations SET completed_n = 99, failed_n = 12
     WHERE generation_id = ?",
    params = list(gen))
  dsImaging:::.asset_db_close(db)

  reconcile_generation_counters(gen)
  repaired <- get_generation(gen)
  expect_equal(repaired$completed_n, 1L)
  expect_equal(repaired$failed_n, 0L)
})

test_that("stale claimed items are requeued for recovery", {
  tmp <- tempfile(fileext = ".sqlite")
  on.exit(unlink(tmp))
  withr::local_options(list(dsimaging.asset_db = tmp))

  gen <- claim_or_reuse_generation("ds.v1", "feature_table",
    "hash_stale_claim", owner_id = "tester", expected_n = 1L)$generation_id
  record_item_status(gen, "sample_a", "pending")
  claimed <- claim_pending_items(gen, 1L, claimer_id = "test")
  expect_equal(claimed, "sample_a")

  db <- dsImaging:::.asset_db_connect()
  DBI::dbExecute(db,
    "UPDATE asset_items SET claimed_at = '2000-01-01T00:00:00.000Z'
     WHERE generation_id = ? AND sample_id = ?",
    params = list(gen, "sample_a"))
  dsImaging:::.asset_db_close(db)

  n <- dsImaging:::requeue_stale_claimed_items(gen, timeout_secs = 1)
  expect_equal(n, 1L)
  item <- get_generation_items(gen)
  expect_equal(item$status, "pending")
  expect_true(is.na(item$claimed_at))
})

test_that("private item correlation is opaque, durable, and recoverable", {
  tmp <- tempfile(fileext = ".sqlite")
  on.exit(unlink(tmp))
  withr::local_options(list(dsimaging.asset_db = tmp))

  private_id <- "PHI_PATIENT_701_SCAN_A"
  gen <- claim_or_reuse_generation("lung", "radiomics_collection",
    "hash_private_correlation", owner_id = "tester",
    expected_n = 1L)$generation_id
  record_item_status(gen, private_id, "pending")

  token <- dsImaging:::.item_job_token(gen, private_id)
  expect_identical(dsImaging:::.item_job_token(gen, private_id), token)
  expect_match(token, "^[0-9a-f]{32}$")
  expect_false(grepl(private_id, token, fixed = TRUE))

  record_item_status(gen, private_id, "running")
  complete_item_atomic(gen, private_id, "completed",
    artifact_relpath = "/private/result.parquet")
  items <- get_generation_items(gen)
  expect_identical(items$job_token[[1L]], token)

  tags <- dsImaging:::.per_image_job_tags("lung", token, gen)
  expect_false(any(grepl(private_id, tags, fixed = TRUE)))
  expect_identical(
    dsImaging:::.sample_id_from_tags(tags, items), private_id)
})

test_that("active dsHPC jobs prevent stale claimed items from being duplicated", {
  asset_db <- tempfile(fileext = ".sqlite")
  home <- tempfile("dshpc-home")
  dir.create(home, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(c(asset_db, home), recursive = TRUE))
  withr::local_options(list(dsimaging.asset_db = asset_db, dshpc.home = home))

  gen <- claim_or_reuse_generation("lung", "radiomics_collection",
    "hash_active_claim", owner_id = "tester", expected_n = 1L)$generation_id
  record_item_status(gen, "sample_a", "pending")
  job_token <- dsImaging:::.item_job_token(gen, "sample_a")
  expect_equal(claim_pending_items(gen, 1L, claimer_id = "test"), "sample_a")

  adb <- dsImaging:::.asset_db_connect()
  DBI::dbExecute(adb,
    "UPDATE asset_items SET claimed_at = '2000-01-01T00:00:00.000Z'
     WHERE generation_id = ? AND sample_id = ?",
    params = list(gen, "sample_a"))
  dsImaging:::.asset_db_close(adb)

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(
    label = "dsImaging_image",
    tags = dsImaging:::.per_image_job_tags("lung", job_token, gen),
    steps = list(list(type = "emit", plane = "session",
      output_name = "x", value = 1))
  )
  dsHPC:::.store_create_job(db, "job_active_sample", "tester", spec, 1L)

  dsImaging:::.sync_generation_jobs(gen)
  n <- dsImaging:::requeue_stale_claimed_items(gen, timeout_secs = 1)

  expect_equal(n, 0L)
  item <- get_generation_items(gen)
  expect_equal(item$status, "running")
})

test_that("active retry jobs override previous failed item state", {
  asset_db <- tempfile(fileext = ".sqlite")
  home <- tempfile("dshpc-home")
  dir.create(home, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(c(asset_db, home), recursive = TRUE))
  withr::local_options(list(dsimaging.asset_db = asset_db, dshpc.home = home))

  gen <- claim_or_reuse_generation("lung", "radiomics_collection",
    "hash_active_retry", owner_id = "tester", expected_n = 1L)$generation_id
  complete_item_atomic(gen, "sample_a", "failed", error = "old failure")
  job_token <- dsImaging:::.item_job_token(gen, "sample_a")

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(
    label = "dsImaging_image",
    tags = dsImaging:::.per_image_job_tags("lung", job_token, gen),
    steps = list(list(type = "emit", plane = "session",
      output_name = "x", value = 1))
  )
  dsHPC:::.store_create_job(db, "job_retry_sample", "tester", spec, 1L)

  dsImaging:::.sync_generation_jobs(gen)

  item <- get_generation_items(gen)
  expect_equal(item$status, "running")
  expect_equal(get_generation(gen)$failed_n, 0L)
})

test_that("generation cancellation is admin-gated and cancels tagged jobs", {
  asset_db <- tempfile(fileext = ".sqlite")
  home <- tempfile("dshpc-home")
  dir.create(home, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(c(asset_db, home), recursive = TRUE))
  withr::local_options(list(
    dsimaging.asset_db = asset_db,
    dshpc.home = home,
    dshpc.admin_key = "secret"
  ))

  gen <- claim_or_reuse_generation("lung", "radiomics_collection",
    "hash_cancel", owner_id = "tester", expected_n = 2L)$generation_id
  update_generation(gen, state = "RUNNING")
  record_item_status(gen, "sample_a", "running")
  record_item_status(gen, "sample_b", "pending")
  job_token <- dsImaging:::.item_job_token(gen, "sample_a")

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(
    label = "dsImaging_image",
    tags = dsImaging:::.per_image_job_tags("lung", job_token, gen),
    visibility = "private",
    steps = list(list(type = "emit", plane = "session",
      output_name = "x", value = 1))
  )
  dsHPC:::.store_create_job(db, "job_sample_a", "tester", spec, 1L)
  dsHPC:::.store_update_job(db, "job_sample_a", state = "RUNNING")
  session <- new.env(parent = globalenv())
  workflow <- dsImaging:::.register_imaging_workflow(list(
    action = "running", generation_id = gen, dataset_id = "lung"), session)
  assign("workflow", workflow, envir = session)

  expect_error(
    eval(call("imagingRadiomicsCancelCollectionDS", "workflow",
      dsImaging:::.dsr_encode(list(.admin_key = "wrong")),
      dsImaging:::.dsr_encode("test cancel")), envir = session),
    "Admin access denied"
  )
  expect_equal(dsHPC:::.store_get_job(db, "job_sample_a")$state, "RUNNING")

  out <- eval(call("imagingRadiomicsCancelCollectionDS", "workflow",
    dsImaging:::.dsr_encode(list(.admin_key = "secret")),
    dsImaging:::.dsr_encode("test cancel")), envir = session)

  expect_equal(out$state, "CANCELLED")
  expect_named(out, "state")
  expect_equal(dsHPC:::.store_get_job(db, "job_sample_a")$state, "CANCELLED")
  expect_equal(get_generation(gen)$state, "CANCELLED")
  items <- get_generation_items(gen)
  expect_true(all(items$status == "skipped"))
})

test_that("generation cancellation accepts admin key from environment", {
  asset_db <- tempfile(fileext = ".sqlite")
  home <- tempfile("dshpc-home")
  dir.create(home, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(c(asset_db, home), recursive = TRUE))
  withr::local_options(list(
    dsimaging.asset_db = asset_db,
    dshpc.home = home,
    dshpc.admin_key = NULL,
    default.dshpc.admin_key = NULL
  ))
  withr::local_envvar(c(DSHPC_ADMIN_KEY = "env-secret"))

  gen <- claim_or_reuse_generation("lung", "radiomics_collection",
    "hash_cancel_env", owner_id = "tester", expected_n = 1L)$generation_id
  update_generation(gen, state = "RUNNING")
  record_item_status(gen, "sample_a", "running")
  job_token <- dsImaging:::.item_job_token(gen, "sample_a")

  db <- dsHPC:::.db_connect()
  on.exit(dsHPC:::.db_close(db), add = TRUE)
  spec <- list(
    label = "dsImaging_image",
    tags = dsImaging:::.per_image_job_tags("lung", job_token, gen),
    visibility = "private",
    steps = list(list(type = "emit", plane = "session",
      output_name = "x", value = 1))
  )
  dsHPC:::.store_create_job(db, "job_sample_env", "tester", spec, 1L)
  dsHPC:::.store_update_job(db, "job_sample_env", state = "RUNNING")
  session <- new.env(parent = globalenv())
  workflow <- dsImaging:::.register_imaging_workflow(list(
    action = "running", generation_id = gen, dataset_id = "lung"), session)
  assign("workflow", workflow, envir = session)

  out <- eval(call("imagingRadiomicsCancelCollectionDS", "workflow",
    dsImaging:::.dsr_encode(list(.admin_key = "env-secret")),
    dsImaging:::.dsr_encode("env cancel")), envir = session)

  expect_equal(out$state, "CANCELLED")
  expect_equal(dsHPC:::.store_get_job(db, "job_sample_env")$state,
    "CANCELLED")
})

test_that("collection publish enforces generation selected features", {
  skip_if_not_installed("arrow")
  output_root <- tempfile("collection")
  dir.create(output_root, recursive = TRUE)
  on.exit(unlink(output_root, recursive = TRUE))

  artifact_a <- tempfile(fileext = ".csv")
  artifact_b <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(sample_id = "sample_a", feature_a = 1,
    feature_b = 2, extra_feature = 99), artifact_a, row.names = FALSE)
  utils::write.csv(data.frame(sample_id = "sample_b", feature_a = 3,
    feature_b = 4), artifact_b, row.names = FALSE)

  completed <- data.frame(
    sample_id = c("sample_a", "sample_b"),
    artifact_relpath = c(artifact_a, artifact_b),
    stringsAsFactors = FALSE
  )

  out <- dsImaging:::.write_collection_feature_table(completed, output_root,
    selected_features = c("feature_a", "feature_b"))
  features <- as.data.frame(arrow::read_parquet(out))

  expect_equal(names(features), c("sample_id", "feature_a", "feature_b"))
  expect_equal(features$feature_a, c(1, 3))
  expect_false("extra_feature" %in% names(features))
  expect_error(
    dsImaging:::.write_collection_feature_table(completed, output_root,
      selected_features = c("feature_a", "missing_feature")),
    "missing selected feature"
  )
})

test_that("collection feature publication enforces one row per roster sample", {
  testthat::skip_if_not_installed("arrow")
  root <- withr::local_tempdir()
  artifact_a <- file.path(root, "a.csv")
  artifact_b <- file.path(root, "b.csv")
  utils::write.csv(data.frame(
    sample_id = c("sample_a", "sample_a"), feature = c(1, 2)),
    artifact_a, row.names = FALSE)
  utils::write.csv(data.frame(
    sample_id = "sample_b", feature = 3), artifact_b, row.names = FALSE)
  completed <- data.frame(
    sample_id = c("sample_a", "sample_b"),
    artifact_relpath = c(artifact_a, artifact_b),
    stringsAsFactors = FALSE)
  roster <- dsImaging:::.new_imaging_privacy_roster(
    c("sample_a", "sample_b"), c("patient_a", "patient_b"))

  expect_error(dsImaging:::.write_collection_feature_table(
    completed, file.path(root, "out"), roster = roster),
    "exactly one admitted sample")

  utils::write.csv(data.frame(
    sample_id = "sample_other", feature = 1),
    artifact_a, row.names = FALSE)
  expect_error(dsImaging:::.write_collection_feature_table(
    completed, file.path(root, "out"), roster = roster),
    "does not match")
})

test_that("invalid completed artifacts are requeued for recovery", {
  skip_if_not_installed("arrow")
  asset_db <- tempfile(fileext = ".sqlite")
  on.exit(unlink(asset_db), add = TRUE)
  withr::local_options(list(dsimaging.asset_db = asset_db))

  good <- tempfile(fileext = ".csv")
  bad <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(sample_id = "sample_a", feature_a = 1,
    feature_b = 2), good, row.names = FALSE)
  utils::write.csv(data.frame(sample_id = "sample_b", feature_a = 3),
    bad, row.names = FALSE)

  spec <- list(profile = list(selected_features = c("feature_a",
    "feature_b")))
  gen <- claim_or_reuse_generation("lung", "radiomics_collection",
    "hash_invalid_artifact", owner_id = "tester", expected_n = 2L,
    spec = spec)$generation_id
  record_item_status(gen, "sample_a", "completed", artifact_relpath = good)
  record_item_status(gen, "sample_b", "completed", artifact_relpath = bad)
  reconcile_generation_counters(gen)

  expect_equal(dsImaging:::.requeue_invalid_completed_items(gen), 1L)
  items <- get_generation_items(gen)
  expect_equal(items$status[items$sample_id == "sample_a"], "completed")
  expect_equal(items$status[items$sample_id == "sample_b"], "pending")
  expect_equal(get_generation(gen)$completed_n, 1L)
  expect_equal(get_generation(gen)$state, "RUNNING")
})

test_that("per-image assets are never reused across workflows", {
  skip_if_not_installed("arrow")
  asset_db <- tempfile(fileext = ".sqlite")
  on.exit(unlink(asset_db), add = TRUE)
  withr::local_options(list(dsimaging.asset_db = asset_db))

  good <- tempfile(fileext = ".csv")
  bad <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(sample_id = "sample_a", feature_a = 1,
    feature_b = 2), good, row.names = FALSE)
  utils::write.csv(data.frame(sample_id = "sample_b", feature_a = 3),
    bad, row.names = FALSE)

  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  bad_id <- dsImaging:::.asset_register(db, "lung", "per_image_result", bad,
    derivation_hash = "hash_bad")
  good_id <- dsImaging:::.asset_register(db, "lung", "per_image_result", good,
    derivation_hash = "hash_good")

  expect_null(dsImaging:::.existing_per_image_asset("lung", "hash_bad",
    selected_features = c("feature_a", "feature_b")))
  expect_null(dsImaging:::.existing_per_image_asset("lung", "hash_good",
    selected_features = c("feature_a", "feature_b")))
  expect_true(nzchar(bad_id))
  expect_true(nzchar(good_id))
})

test_that("orphan running items are requeued", {
  asset_db <- tempfile(fileext = ".sqlite")
  home <- tempfile("dshpc-home")
  dir.create(home, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(c(asset_db, home), recursive = TRUE), add = TRUE)
  withr::local_options(list(dsimaging.asset_db = asset_db, dshpc.home = home))

  gen <- claim_or_reuse_generation("lung", "radiomics_collection",
    "hash_orphan_running", owner_id = "tester", expected_n = 1L)$generation_id
  record_item_status(gen, "sample_a", "running")

  expect_equal(dsImaging:::.requeue_orphan_running_items(gen), 1L)
  item <- get_generation_items(gen)
  expect_equal(item$status, "pending")
  expect_match(item$error, "no active dsHPC job")
})
