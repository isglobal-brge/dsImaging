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
        uri = paste0(
          "s3://imaging-data/datasets/lung/source/masks/case00", 1:2,
          "_mask.nii.gz"),
        content_hash = c(strrep("a", 64), strrep("b", 64)),
        stringsAsFactors = FALSE
      )
    },
    .package = "dsImaging"
  )

  hashes <- dsImaging:::.existing_mask_hashes(resolved, manifest, segmenter)
  expect_equal(hashes$case001, strrep("a", 64))
  expect_equal(hashes$case002, strrep("b", 64))
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
        content_hash = strrep("a", 64),
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

test_that("mask indexes cannot redirect into another collection", {
  backend <- structure(list(type = "s3", config = list()),
    class = "dsimaging_backend")
  manifest <- list(assets = list(masks = list(
    uri = "s3://imaging-data/datasets/collection-a/source/masks/",
    content_hash_index = paste0(
      "s3://imaging-data/datasets/collection-a/indexes/",
      "masks_content_hash_index.parquet"))))
  testthat::local_mocked_bindings(
    read_hash_index = function(...) data.frame(
      sample_id = "case001",
      uri = paste0("s3://imaging-data/datasets/collection-b/",
        "source/masks/secret.nii.gz"),
      content_hash = strrep("a", 64), stringsAsFactors = FALSE),
    backend_head = function(...) stop("invalid indexes must not fall back"),
    backend_list = function(...) stop("invalid indexes must not fall back"),
    .package = "dsImaging")

  expect_error(dsImaging:::.resolve_sample_mask(
    manifest$assets$masks$uri, "case001", backend, manifest, "masks"),
    "index is invalid")
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

test_that("local mask resolution matches S1 without selecting S10", {
  root <- withr::local_tempdir()
  s10 <- file.path(root, "S10_mask.nii.gz")
  file.create(s10)

  expect_null(dsImaging:::.resolve_sample_mask(root, "S1"))

  s1 <- file.path(root, "S1_mask.nii.gz")
  file.create(s1)
  expect_identical(dsImaging:::.resolve_sample_mask(root, "S1"),
    normalizePath(s1, winslash = "/"))
})

test_that("local mask resolution rejects symlinks outside its root", {
  root <- withr::local_tempdir()
  outside_root <- withr::local_tempdir()
  outside <- file.path(outside_root, "S1_mask.nii.gz")
  file.create(outside)
  link <- file.path(root, "S1_mask.nii.gz")
  linked <- suppressWarnings(file.symlink(outside, link))
  if (!isTRUE(linked)) skip("Symbolic links are unavailable on this platform")

  expect_null(dsImaging:::.resolve_sample_mask(root, "S1"))
})

test_that("local mask resolution fails closed on ambiguous exact stems", {
  root <- withr::local_tempdir()
  file.create(file.path(root, "S1_mask.nii.gz"))
  file.create(file.path(root, "S1_seg.nii.gz"))

  expect_error(dsImaging:::.resolve_sample_mask(root, "S1"),
    "Imaging mask selection is ambiguous.", fixed = TRUE)
})

test_that("S3 mask resolution uses exact stems and detects ambiguity", {
  backend <- structure(list(type = "s3", config = list()),
    class = "dsimaging_backend")
  root <- "s3://imaging-data/datasets/lung/source/masks/"

  testthat::local_mocked_bindings(
    backend_head = function(backend, uri) {
      list(exists = basename(uri) %in%
        c("S10_mask.nii.gz", "S1_mask.nii.gz", "S1_seg.nii.gz"))
    },
    .package = "dsImaging"
  )

  expect_error(dsImaging:::.resolve_sample_mask(root, "S1", backend = backend),
    "Imaging mask selection is ambiguous.", fixed = TRUE)

  testthat::local_mocked_bindings(
    backend_head = function(backend, uri) {
      list(exists = identical(basename(uri), "S10_mask.nii.gz"))
    },
    .package = "dsImaging"
  )
  expect_null(dsImaging:::.resolve_sample_mask(root, "S1", backend = backend))
})

test_that("mask indexes use their explicit sample mapping, not filenames", {
  backend <- structure(list(type = "s3", config = list()),
    class = "dsimaging_backend")
  manifest <- list(assets = list(masks = list(
    uri = "s3://imaging-data/datasets/lung/source/masks/",
    content_hash_index = paste0(
      "s3://imaging-data/datasets/lung/indexes/",
      "masks_content_hash_index.parquet"))))
  testthat::local_mocked_bindings(
    read_hash_index = function(...) data.frame(
      sample_id = "S1",
      uri = paste0(manifest$assets$masks$uri, "S10_mask.nii.gz"),
      content_hash = strrep("a", 64), stringsAsFactors = FALSE),
    backend_head = function(...) stop("invalid indexes must not fall back"),
    .package = "dsImaging")

  expect_identical(dsImaging:::.resolve_sample_mask(
    manifest$assets$masks$uri, "S1", backend, manifest, "masks"),
    paste0(manifest$assets$masks$uri, "S10_mask.nii.gz"))
})

test_that("collection workflows reject an incomplete existing-mask index", {
  skip_if_not_installed("arrow")
  root <- withr::local_tempdir()
  ids <- c("S1", "S2", "S3")
  collection <- write_test_imaging_collection(
    root, "incomplete.masks",
    data.frame(sample_id = ids, patient_id = paste0("P", 1:3),
      stringsAsFactors = FALSE))
  mask_root <- file.path(root, "source", "masks")
  dir.create(mask_root)
  mask_paths <- file.path(mask_root, c("opaque-a.nii.gz", "opaque-b.nii.gz"))
  vapply(mask_paths, function(path) {
    writeBin(charToRaw("mask"), path)
    path
  }, character(1))
  mask_index <- file.path(root, "indexes", "mask-index.parquet")
  arrow::write_parquet(data.frame(
    sample_id = ids[1:2], uri = mask_paths,
    content_hash = vapply(mask_paths, digest::digest, character(1),
      algo = "sha256", file = TRUE), stringsAsFactors = FALSE), mask_index)
  collection$manifest$assets$masks <- list(
    kind = "mask_root", uri = mask_root, content_hash_index = mask_index)

  backend <- storage_backend("file")
  admission <- dsImaging:::.imaging_privacy_admission(
    collection$manifest, backend)
  snapshot <- dsImaging:::.new_imaging_collection_snapshot(
    collection$manifest, backend, admission)
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L))
  expect_error(dsImaging:::.imaging_radiomics_scan_collection(
    "incomplete.masks",
    list(provider = "existing_mask_asset", mask_asset = "masks"),
    list(name = "demo_ct_firstorder_v1", bin_width = 25),
    "private",
    resolved_override = list(
      manifest = collection$manifest, backend = backend,
      collection_snapshot = snapshot)),
    "complete admitted collection", fixed = TRUE)
})

test_that("S3 mask staging downloads into the dsHPC staging area", {
  backend <- structure(list(type = "s3", config = list()),
    class = "dsimaging_backend")
  home <- tempfile("dshpc-home")
  withr::local_options(list(dshpc.home = home, default.dshpc.home = home))
  private_id <- "PHI_PATIENT_701_SCAN_A"

  testthat::local_mocked_bindings(
    backend_get_file = function(backend, uri, dest, overwrite = FALSE) {
      writeLines("mask", dest)
      invisible(dest)
    },
    .package = "dsImaging"
  )

  out <- dsImaging:::.stage_backend_file_for_job(
    paste0("s3://imaging-data/datasets/lung/source/masks/", private_id,
      "_mask.nii.gz"),
    sample_id = private_id,
    dataset_id = "lung",
    backend = backend,
    role = "masks"
  )

  expect_true(file.exists(out))
  expect_false(grepl(private_id, out, fixed = TRUE))
  expect_match(
    gsub("\\\\", "/", out),
    "/staging/dsimaging/[0-9a-f]{64}/masks/[0-9a-f]{64}\\.nii\\.gz$")
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

  status <- dsImaging:::.imaging_radiomics_collection_status(generation_id)
  expect_true(status$is_done)
  expect_equal(status$completed, 1L)
})

test_that("transient submit errors are retryable", {
  expect_true(dsImaging:::.is_transient_job_submit_error("database is locked"))
  expect_true(dsImaging:::.is_transient_job_submit_error("quota exceeded"))
  expect_false(dsImaging:::.is_transient_job_submit_error("Mask file not found"))
})

test_that("internal collection accounting retains exact full-roster counts", {
  # Internal scheduling and publication need exact accounting. Registered
  # analyst status methods project this payload to coarse state only.
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(dsimaging.asset_db = db_path))

  gen <- claim_or_reuse_generation(
    dataset_id = "lung6",
    kind = "radiomics_collection",
    derivation_hash = "exact-count-test",
    expected_n = 6L,
    owner_id = "tester"
  )
  generation_id <- gen$generation_id
  for (sid in paste0("case00", 1:6)) {
    record_item_status(generation_id, sid, "completed")
  }
  update_generation(generation_id, state = "RUNNING", completed_n = 6L)

  testthat::local_mocked_bindings(
    .sync_generation_jobs = function(generation_id) NULL,
    .package = "dsImaging"
  )

  status <- dsImaging:::.imaging_radiomics_collection_status(generation_id)
  expect_equal(status$total, 6L)
  expect_equal(status$completed, 6L)
  expect_equal(status$failed, 0L)
  expect_true(status$is_done)
})

test_that("segmentation manifests cannot reference masks outside an artifact", {
  root <- withr::local_tempdir()
  artifact <- file.path(root, "artifact")
  dir.create(artifact)
  inside <- file.path(artifact, "case001_mask.nii.gz")
  outside <- file.path(root, "private_mask.nii.gz")
  file.create(outside)
  manifest_path <- file.path(artifact, "seg_manifest.json")

  jsonlite::write_json(list(samples = list(case001 = list(
    primary_mask = outside))), manifest_path, auto_unbox = TRUE)
  expect_null(dsImaging:::.find_mask_in_artifact(artifact, "case001"))

  file.create(inside)
  jsonlite::write_json(list(samples = list(case001 = list(
    primary_mask = basename(inside)))), manifest_path, auto_unbox = TRUE)
  expect_identical(dsImaging:::.find_mask_in_artifact(artifact, "case001"),
                   normalizePath(inside))
})
