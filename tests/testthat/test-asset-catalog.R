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
    "/data/radio1.parquet", derivation_hash = "samehash")

  aid2 <- dsImaging:::.asset_register(db, "ds.v1", "feature_table",
    "/data/radio2.parquet", derivation_hash = "samehash")

  # Should return the same asset_id (deduplicated)
  expect_equal(aid1, aid2)

  # Different hash = different asset
  aid3 <- dsImaging:::.asset_register(db, "ds.v1", "feature_table",
    "/data/radio3.parquet", derivation_hash = "differenthash")

  expect_false(aid1 == aid3)
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
    provenance = list(mask = "unet_v1"))
  dsImaging:::.asset_register(db, "ds.v1", "feature_table",
    "/data/radio_manual.parquet", derivation_hash = "hash_manual",
    provenance = list(mask = "manual"))
  dsImaging:::.asset_register(db, "ds.v1", "feature_table",
    "/data/radio_nnunet.parquet", derivation_hash = "hash_nnunet",
    provenance = list(mask = "nnunet_v2"))

  assets <- dsImaging:::.asset_list(db, "ds.v1", kind = "feature_table")
  expect_equal(nrow(assets), 3L)
})

test_that("aliases work", {
  tmp <- tempfile(fileext = ".sqlite")
  on.exit(unlink(tmp))
  withr::local_options(list(dsimaging.asset_db = tmp))

  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)

  aid1 <- dsImaging:::.asset_register(db, "ds.v1", "mask_root",
    "/data/masks_unet", derivation_hash = "h1")
  aid2 <- dsImaging:::.asset_register(db, "ds.v1", "mask_root",
    "/data/masks_nnunet", derivation_hash = "h2")

  # Set alias
  dsImaging:::.asset_set_alias(db, "ds.v1", "default_lung_mask", aid1)

  # Resolve
  resolved <- dsImaging:::.asset_resolve_alias(db, "ds.v1", "default_lung_mask")
  expect_equal(resolved, aid1)

  # Update alias to point to different asset
  dsImaging:::.asset_set_alias(db, "ds.v1", "default_lung_mask", aid2)
  resolved2 <- dsImaging:::.asset_resolve_alias(db, "ds.v1", "default_lung_mask")
  expect_equal(resolved2, aid2)
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

test_that("active dsJobs prevent stale claimed items from being duplicated", {
  asset_db <- tempfile(fileext = ".sqlite")
  home <- tempfile("dsjobs-home")
  dir.create(home, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(c(asset_db, home), recursive = TRUE))
  withr::local_options(list(dsimaging.asset_db = asset_db, dsjobs.home = home))

  gen <- claim_or_reuse_generation("lung", "radiomics_collection",
    "hash_active_claim", owner_id = "tester", expected_n = 1L)$generation_id
  record_item_status(gen, "sample_a", "pending")
  expect_equal(claim_pending_items(gen, 1L, claimer_id = "test"), "sample_a")

  adb <- dsImaging:::.asset_db_connect()
  DBI::dbExecute(adb,
    "UPDATE asset_items SET claimed_at = '2000-01-01T00:00:00.000Z'
     WHERE generation_id = ? AND sample_id = ?",
    params = list(gen, "sample_a"))
  dsImaging:::.asset_db_close(adb)

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)
  spec <- list(
    label = "dsImaging_image",
    tags = c("per_image", "lung", "sample_a", gen),
    steps = list(list(type = "emit", plane = "session",
      output_name = "x", value = 1))
  )
  dsJobs:::.store_create_job(db, "job_active_sample", "tester", spec, 1L)

  dsImaging:::.sync_generation_jobs(gen)
  n <- dsImaging:::requeue_stale_claimed_items(gen, timeout_secs = 1)

  expect_equal(n, 0L)
  item <- get_generation_items(gen)
  expect_equal(item$status, "running")
})

test_that("active retry jobs override previous failed item state", {
  asset_db <- tempfile(fileext = ".sqlite")
  home <- tempfile("dsjobs-home")
  dir.create(home, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(c(asset_db, home), recursive = TRUE))
  withr::local_options(list(dsimaging.asset_db = asset_db, dsjobs.home = home))

  gen <- claim_or_reuse_generation("lung", "radiomics_collection",
    "hash_active_retry", owner_id = "tester", expected_n = 1L)$generation_id
  complete_item_atomic(gen, "sample_a", "failed", error = "old failure")

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)
  spec <- list(
    label = "dsImaging_image",
    tags = c("per_image", "lung", "sample_a", gen),
    steps = list(list(type = "emit", plane = "session",
      output_name = "x", value = 1))
  )
  dsJobs:::.store_create_job(db, "job_retry_sample", "tester", spec, 1L)

  dsImaging:::.sync_generation_jobs(gen)

  item <- get_generation_items(gen)
  expect_equal(item$status, "running")
  expect_equal(get_generation(gen)$failed_n, 0L)
})

test_that("generation cancellation is admin-gated and cancels tagged jobs", {
  asset_db <- tempfile(fileext = ".sqlite")
  home <- tempfile("dsjobs-home")
  dir.create(home, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(c(asset_db, home), recursive = TRUE))
  withr::local_options(list(
    dsimaging.asset_db = asset_db,
    dsjobs.home = home,
    dsjobs.admin_key = "secret"
  ))

  gen <- claim_or_reuse_generation("lung", "radiomics_collection",
    "hash_cancel", owner_id = "tester", expected_n = 2L)$generation_id
  update_generation(gen, state = "RUNNING")
  record_item_status(gen, "sample_a", "running")
  record_item_status(gen, "sample_b", "pending")

  db <- dsJobs:::.db_connect()
  on.exit(dsJobs:::.db_close(db), add = TRUE)
  spec <- list(
    label = "dsImaging_image",
    tags = c("per_image", "lung", "sample_a", gen),
    visibility = "private",
    steps = list(list(type = "emit", plane = "session",
      output_name = "x", value = 1))
  )
  dsJobs:::.store_create_job(db, "job_sample_a", "tester", spec, 1L)
  dsJobs:::.store_update_job(db, "job_sample_a", state = "RUNNING")

  expect_error(
    imagingRadiomicsCancelCollectionDS(
      dsImaging:::.dsr_encode(gen),
      dsImaging:::.dsr_encode(list(.admin_key = "wrong")),
      dsImaging:::.dsr_encode("test cancel")
    ),
    "invalid admin_key"
  )
  expect_equal(dsJobs:::.store_get_job(db, "job_sample_a")$state, "RUNNING")

  out <- imagingRadiomicsCancelCollectionDS(
    dsImaging:::.dsr_encode(gen),
    dsImaging:::.dsr_encode(list(.admin_key = "secret")),
    dsImaging:::.dsr_encode("test cancel")
  )

  expect_equal(out$state, "CANCELLED")
  expect_equal(out$cancelled_jobs, 1L)
  expect_equal(dsJobs:::.store_get_job(db, "job_sample_a")$state, "CANCELLED")
  expect_equal(get_generation(gen)$state, "CANCELLED")
  items <- get_generation_items(gen)
  expect_true(all(items$status == "skipped"))
})
