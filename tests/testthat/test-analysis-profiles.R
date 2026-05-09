test_that("bundled profiles are listed", {
  profiles <- list_radiomics_profiles()
  expect_true(length(profiles) >= 4)
  expect_true("ibsi_ct_3d_v1" %in% profiles)
  expect_true("ibsi_mr_3d_v1" %in% profiles)
  expect_true("ibsi_force2d_v1" %in% profiles)
  expect_true("voxel_map_firstorder_v1" %in% profiles)
  expect_true("aerts_signature_v1" %in% profiles)
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
  expect_equal(p$featureClass$firstorder, "Energy")
  expect_equal(p$featureClass$shape, "Compactness1")
  expect_equal(p$featureClass$glrlm, "RunLengthNonUniformity")
})

test_that("reading nonexistent profile errors", {
  expect_error(read_radiomics_profile("nonexistent_profile"), "not found")
})

test_that("extract runner config preserves selected feature vectors", {
  cfg <- dsImaging:::.normalise_extract_config(list(
    selected_features = list("a", "b", "c")
  ))
  expect_equal(cfg$selected_features, c("a", "b", "c"))
})
