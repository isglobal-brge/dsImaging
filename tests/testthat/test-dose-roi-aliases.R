test_that("dose ROI aliases cannot repeat a resolved mask and voxel value", {
  root <- withr::local_tempdir()
  seal <- strrep("a", 64)
  withr::local_options(list(dsimaging.asset_db = file.path(root, "assets.sqlite")))
  db <- dsImaging:::.asset_db_connect()
  withr::defer(dsImaging:::.asset_db_close(db))
  mask_roots <- file.path(root, c("mask-a", "mask-b"))
  for (path in mask_roots) {
    dir.create(path)
    writeBin(charToRaw("identical synthetic mask bytes"), file.path(path, "mask.nii"))
  }
  ids <- vapply(mask_roots, function(path) {
    dsImaging:::.asset_register(db, "lung", "mask_root", path,
      collection_seal = seal, visibility = "global")
  }, character(1))
  dsImaging:::.asset_set_alias(db, "lung", "tumour_masks", ids[[1L]])
  dsImaging:::.asset_set_alias(db, "lung", "organ_masks", ids[[1L]])
  record <- function(uri, sample = "scan-1") list(
    sample_id = sample, source_kind = "mask_file", uri = uri,
    content_hash = strrep("b", 64), size = 30L, n_files = 1L)
  records <- list(
    rt_dose = list(record(file.path(root, "dose.dcm"))),
    rt_plan = list(record(file.path(root, "plan.dcm"))))
  authorized <- list(dataset_id = "lung", collection_seal = seal,
    manifest = list(dataset_id = "lung", assets = list(
      rt_dose = list(uri = root, type = "rt_dose"),
      rt_plan = list(uri = root, type = "rt_plan"))))
  registered <- 0L
  submitted <- NULL
  testthat::local_mocked_bindings(
    hpcUnitSelectionInternal = function(...) NULL,
    .package = "dsHPC")
  testthat::local_mocked_bindings(
    .authorized_imaging_dataset = function(...) authorized,
    .imaging_runtime_identity = function(...) strrep("c", 64),
    .imaging_worker_collection_map = function(...) list(records_by_asset = records),
    find_asset_by_hash = function(...) NULL,
    .register_imaging_worker_context = function(...) {
      registered <<- registered + 1L
      paste0("dsctx_", strrep("d", 64))
    },
    .content_hash_for_resource = function(...) NULL,
    .imaging_submit_job = function(job, ...) {
      submitted <<- job
      structure(list(capability = paste0("imgw_", strrep("e", 64))),
        class = "dsimaging_workflow_ref")
    },
    .package = "dsImaging")
  request <- function(assets, values = "1,1") {
    encoded <- dsImaging:::.dsr_encode(list(
      handle = "img", runner = "rt_dose_plan", output_asset = "roi_dose",
      config = list(roi_labels = "tumour,organ", mask_labels = values,
        mask_assets = assets)))
    imagingProcessAssetWorkflowDS(encoded)
  }

  # Exercise real catalog alias resolution before any worker context exists.
  expect_error(request("tumour_masks,organ_masks"),
    "resolves to an ambiguous mask and label pair", fixed = TRUE)
  expect_identical(registered, 0L)
  expect_null(submitted)
  expect_s3_class(request(paste("tumour_masks", ids[[1L]], sep = ","), "1,2"),
    "dsimaging_workflow_ref")
  expect_identical(registered, 1L)
  expect_identical(submitted$steps[[2L]]$config$mask_labels, "1,2")

  # Distinct catalog assets remain distinct even when their files are equal.
  dsImaging:::.asset_set_alias(db, "lung", "organ_masks", ids[[2L]])
  expect_identical(digest::digest(file.path(mask_roots[[1L]], "mask.nii"), file = TRUE),
    digest::digest(file.path(mask_roots[[2L]], "mask.nii"), file = TRUE))
  expect_s3_class(request("tumour_masks,organ_masks"), "dsimaging_workflow_ref")
  expect_identical(registered, 2L)

  # Source maps include their public names in the ordinary derivation hash;
  # changing those names must not hide that they reference the same files.
  authorized$manifest$assets$source_a <- list(uri = mask_roots[[1L]], type = "mask_root")
  authorized$manifest$assets$source_b <- list(uri = mask_roots[[1L]], type = "mask_root")
  records$source_a <- list(record(file.path(mask_roots[[1L]], "mask.nii")))
  records$source_b <- records$source_a
  expect_error(request("source_a,source_b"),
    "resolves to an ambiguous mask and label pair", fixed = TRUE)
  expect_identical(registered, 2L)
  records$source_b[[1L]]$uri <- file.path(mask_roots[[2L]], "mask.nii")
  authorized$manifest$assets$source_b$uri <- mask_roots[[2L]]
  expect_s3_class(request("source_a,source_b"), "dsimaging_workflow_ref")
  expect_identical(registered, 3L)
})
