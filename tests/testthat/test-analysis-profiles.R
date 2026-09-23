test_that("bundled profiles are listed", {
  profiles <- list_radiomics_profiles()
  expect_true(length(profiles) >= 4)
  expect_true("ibsi_ct_3d_v1" %in% profiles)
  expect_true("ibsi_mr_3d_v1" %in% profiles)
  expect_true("ibsi_force2d_v1" %in% profiles)
  expect_true("voxel_map_firstorder_v1" %in% profiles)
  expect_true("aerts_signature_v1" %in% profiles)
  expect_true("aerts_signature_v2" %in% profiles)
})

test_that("profiles can be read", {
  if (requireNamespace("yaml", quietly = TRUE)) {
    p <- read_radiomics_profile("ibsi_ct_3d_v1")
    expect_true(is.list(p))
    expect_true("setting" %in% names(p))
    expect_true("featureClass" %in% names(p))
    expect_equal(p$setting$binWidth, 25)
    expect_false(p$setting$force2D)
  }
})

test_that("Aerts signature profile is available", {
  p <- read_radiomics_profile("aerts_signature_v1")
  expect_equal(p$setting$binWidth, 25)
  expect_false(p$setting$normalize)
  expect_true(is.null(p$setting$resampledPixelSpacing))
  expect_true(p$setting$preCrop)
  expect_equal(p$setting$padDistance, 5)
  expect_equal(p$featureClass$firstorder, "Energy")
  expect_equal(p$featureClass$shape, "Compactness1")
  expect_equal(p$featureClass$glrlm, "RunLengthNonUniformity")
})

test_that("reading nonexistent profile errors", {
  expect_error(read_radiomics_profile("nonexistent_profile"), "not found")
})

test_that("published Aerts profile retains settings and exact feature metadata", {
  historical <- read_radiomics_profile("aerts_signature_v1")
  published <- read_radiomics_profile("aerts_signature_v2")
  expect_identical(published$setting, historical$setting)
  expect_identical(published$imageType, historical$imageType)
  expect_equal(published$featureClass$firstorder, "Energy")
  expect_equal(published$featureClass$shape, "Compactness2")
  expect_equal(published$featureClass$glrlm, "GrayLevelNonUniformity")
  path <- dsImaging:::.get_profile_path("aerts_signature_v1")
  expect_identical(digest::digest(path, file = TRUE, algo = "sha256"),
    "1cc5d18d45b885e96641e8dd202d54a80aed7416c7f97d06b44d7d4f051ed623")
  v1 <- imagingDescribeProfileDS("aerts_signature_v1")
  v2 <- imagingDescribeProfileDS("aerts_signature_v2")
  expect_match(v1$metadata$description, "Aerts-inspired historical", fixed = TRUE)
  expect_equal(v1$metadata$selected_features, c("original_firstorder_Energy",
    "original_shape_Compactness1", "original_glrlm_RunLengthNonUniformity",
    "wavelet-HLH_glrlm_RunLengthNonUniformity"))
  expect_null(dsImaging:::.imaging_profile_spec(
    list(name = "aerts_signature_v1"))$selected_features)
  expected <- c("original_firstorder_Energy", "original_shape_Compactness2",
    "original_glrlm_GrayLevelNonUniformity",
    "wavelet-HLH_glrlm_GrayLevelNonUniformity")
  expect_equal(v2$metadata$selected_features, expected)
  expect_equal(dsImaging:::.imaging_profile_spec(
    list(name = "aerts_signature_v2"))$selected_features, expected)
  expect_error(dsImaging:::.imaging_profile_spec(list(
    name = "aerts_signature_v2", selected_features = expected[1:3])),
    "published four-feature selection", fixed = TRUE)
  expect_true("aerts_signature_v2" %in% imagingListProfilesDS()$profile_name)
})

test_that("extract runner config preserves selected feature vectors", {
  cfg <- dsImaging:::.normalise_extract_config(list(
    selected_features = list("a", "b", "c")
  ))
  expect_equal(cfg$selected_features, c("a", "b", "c"))
})

test_that("published Aerts extraction yields exactly its four selected features", {
  python <- Sys.which("python3")
  skip_if(!nzchar(python), "python3 is unavailable")
  dependencies <- processx::run(python, c("-c",
    "import yaml, numpy, SimpleITK, radiomics, pandas, pyarrow"),
    error_on_status = FALSE)
  skip_if(dependencies$status != 0L, "PyRadiomics integration dependencies are unavailable")
  python_dir <- system.file("python", package = "dsImaging")
  if (!nzchar(python_dir)) python_dir <- normalizePath(
    testthat::test_path("..", "..", "inst", "python"), mustWork = TRUE)
  profile <- dsImaging:::.imaging_profile_spec(list(name = "aerts_signature_v2"))
  root <- withr::local_tempdir()
  images <- file.path(root, "images")
  masks <- file.path(root, "masks")
  dir.create(images)
  dir.create(masks)
  image_path <- file.path(images, "opaque-image.nii.gz")
  mask_path <- file.path(masks, "opaque-mask.nii.gz")
  generated <- processx::run(python, c("-c", paste(
    "import os, numpy as np, SimpleITK as sitk",
    "z, y, x = np.indices((16, 16, 16))",
    "image = (z + 2*y + 3*x).astype(np.float32)",
    "mask = np.zeros(image.shape, dtype=np.uint8)",
    "mask[3:13, 3:13, 3:13] = 1",
    "sitk.WriteImage(sitk.GetImageFromArray(image), os.environ['AERTS_IMAGE'])",
    "sitk.WriteImage(sitk.GetImageFromArray(mask), os.environ['AERTS_MASK'])",
    sep = "\n")), env = c(AERTS_IMAGE = image_path, AERTS_MASK = mask_path),
    error_on_status = FALSE)
  expect_equal(generated$status, 0L, info = generated$stderr)
  record <- function(path, kind) list(sample_id = "sample-1",
    source_kind = kind, uri = path, relative_path = basename(path), n_files = 1L,
    size = as.numeric(file.info(path)$size),
    content_hash = digest::digest(path, file = TRUE, algo = "sha256"))
  image_records <- list(record(image_path, "single_file"))
  mask_records <- list(record(mask_path, "mask_file"))
  context_id <- paste0("dsctx_", strrep("f", 64))
  context_path <- file.path(root, paste0(context_id, ".context.yaml"))
  yaml::write_yaml(list(schema_version = 1L, context_id = context_id,
    manifest = list(dataset_id = "private.dataset",
      assets = list(images = list(uri = images), masks = list(uri = masks)),
      .dsimaging_privacy_roster = list(sample_ids = list("sample-1"))),
    backend = list(type = "file", config = list()),
    collection_map = list(version = 1L, seal = strrep("b", 64),
      asset_names = list("images", "masks"), records = image_records,
      records_by_asset = list(images = image_records, masks = mask_records))),
    context_path)
  output <- file.path(root, "output")
  result <- processx::run(python, c(file.path(python_dir, "dsimaging_extract.py"),
    "--input", root, "--output", output, "--settings",
    dsImaging:::.get_profile_path(profile$name)), env = c(
      PYTHONPATH = paste(python_dir, Sys.getenv("PYTHONPATH"), sep = .Platform$path.sep),
      DSIMAGING_WORKER_CONTEXT_DIR = root, DSHPC_CFG_DATASET_ID = context_id,
      DSHPC_CFG_WORKER_CONTEXT = context_path,
      DSHPC_CFG_IMAGE_ASSET = "images", DSHPC_CFG_MASK_ASSET = "masks",
      DSHPC_CFG_SELECTED_FEATURES = paste(profile$selected_features, collapse = ","),
      DSHPC_INPUT_DIR = ""), error_on_status = FALSE)
  expect_equal(result$status, 0L, info = result$stderr)
  table <- as.data.frame(arrow::read_parquet(file.path(output, "radiomics.parquet")))
  expect_named(table, c(profile$selected_features, "sample_id"))
  expect_equal(table$sample_id, "sample-1")
  expect_true(all(is.finite(as.numeric(table[1L, profile$selected_features]))))
  expect_false(any(grepl("RunLengthNonUniformity|Compactness1", names(table))))
})
