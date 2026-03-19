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
