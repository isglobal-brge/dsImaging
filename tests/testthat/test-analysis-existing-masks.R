test_that("existing mask hashes are read from manifest asset indexes", {
  backend <- structure(list(type = "s3", config = list()),
    class = "dsimaging_backend")
  manifest <- list(assets = list(
    masks = list(
      kind = "mask_root",
      uri = "s3://imaging-data/datasets/lung/source/masks/",
      content_hash_index = "s3://imaging-data/datasets/lung/indexes/masks_content_hash_index.parquet"
    )
  ))
  resolved <- list(backend = backend)
  segmenter <- list(provider = "existing_mask_asset", mask_asset = "masks")

  testthat::local_mocked_bindings(
    read_hash_index = function(backend, index_uri) {
      data.frame(
        sample_id = c("case001", "case002"),
        uri = c("s3://x/case001_mask.nii.gz", "s3://x/case002_mask.nii.gz"),
        content_hash = c("hash-a", "hash-b"),
        stringsAsFactors = FALSE
      )
    },
    .package = "dsImaging"
  )

  hashes <- dsImaging:::.existing_mask_hashes(resolved, manifest, segmenter)
  expect_equal(hashes$case001, "hash-a")
  expect_equal(hashes$case002, "hash-b")
})

test_that("S3 mask resolution prefers the manifest hash index URI", {
  backend <- structure(list(type = "s3", config = list()),
    class = "dsimaging_backend")
  manifest <- list(assets = list(
    masks = list(
      uri = "s3://imaging-data/datasets/lung/source/masks/",
      content_hash_index = "s3://imaging-data/datasets/lung/indexes/masks_content_hash_index.parquet"
    )
  ))

  testthat::local_mocked_bindings(
    read_hash_index = function(backend, index_uri) {
      data.frame(
        sample_id = "case001",
        uri = "s3://imaging-data/datasets/lung/source/masks/case001_GTV-1.nii.gz",
        content_hash = "mask-hash",
        stringsAsFactors = FALSE
      )
    },
    backend_head = function(backend, uri) stop("index hit should avoid head"),
    .package = "dsImaging"
  )

  uri <- dsImaging:::.resolve_sample_mask(
    "s3://imaging-data/datasets/lung/source/masks/",
    "case001",
    backend = backend,
    manifest = manifest,
    mask_asset = "masks"
  )
  expect_equal(uri, "s3://imaging-data/datasets/lung/source/masks/case001_GTV-1.nii.gz")
})

test_that("S3 mask resolution falls back to common mask filename stems", {
  backend <- structure(list(type = "s3", config = list()),
    class = "dsimaging_backend")
  manifest <- list(assets = list(
    masks = list(uri = "s3://imaging-data/datasets/lung/source/masks/")
  ))

  testthat::local_mocked_bindings(
    read_hash_index = function(backend, index_uri) data.frame(),
    backend_head = function(backend, uri) {
      list(exists = identical(
        uri,
        "s3://imaging-data/datasets/lung/source/masks/case001_GTV-1.nii.gz"
      ))
    },
    .package = "dsImaging"
  )

  uri <- dsImaging:::.resolve_sample_mask(
    "s3://imaging-data/datasets/lung/source/masks/",
    "case001",
    backend = backend,
    manifest = manifest,
    mask_asset = "masks"
  )
  expect_equal(uri, "s3://imaging-data/datasets/lung/source/masks/case001_GTV-1.nii.gz")
})

test_that("S3 mask staging downloads into the dsHPC staging area", {
  backend <- structure(list(type = "s3", config = list()),
    class = "dsimaging_backend")
  home <- tempfile("dshpc-home")
  withr::local_options(list(dshpc.home = home, default.dshpc.home = home))

  testthat::local_mocked_bindings(
    backend_get_file = function(backend, uri, dest, overwrite = FALSE) {
      writeLines("mask", dest)
      invisible(dest)
    },
    .package = "dsImaging"
  )

  out <- dsImaging:::.stage_backend_file_for_job(
    "s3://imaging-data/datasets/lung/source/masks/case001_mask.nii.gz",
    sample_id = "case001",
    dataset_id = "lung",
    backend = backend,
    role = "masks"
  )

  expect_true(file.exists(out))
  expect_match(out, "staging/lung/masks", fixed = TRUE)
})

test_that("collection status works without a client-side dataset id", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(dsimaging.asset_db = db_path))

  gen <- claim_or_reuse_generation(
    dataset_id = "lung",
    kind = "radiomics_collection",
    derivation_hash = "status-test",
    expected_n = 1L,
    owner_id = "tester"
  )
  generation_id <- gen$generation_id
  record_item_status(generation_id, "case001", "completed")
  update_generation(generation_id, state = "RUNNING", completed_n = 1L)

  testthat::local_mocked_bindings(
    .sync_generation_jobs = function(generation_id) NULL,
    .package = "dsImaging"
  )

  status <- imagingRadiomicsCollectionStatusDS(
    dsImaging:::.dsr_encode(generation_id)
  )
  expect_true(status$is_done)
  expect_equal(status$completed, 1L)
})

test_that("transient submit errors are retryable", {
  expect_true(dsImaging:::.is_transient_job_submit_error("database is locked"))
  expect_true(dsImaging:::.is_transient_job_submit_error("quota exceeded"))
  expect_false(dsImaging:::.is_transient_job_submit_error("Mask file not found"))
})
