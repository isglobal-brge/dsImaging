test_that("public identifiers accept only bounded non-path tokens", {
  expect_identical(dsImaging:::.safe_public_identifier("hospital-pacs"),
                   "hospital-pacs")
  expect_identical(dsImaging:::.safe_public_identifier("model_family:v1"),
                   "model_family:v1")
  for (value in c(
    "/srv/private/model", "private\\model", "s3://private/model",
    "file:private", "private value", "private\nvalue", "../private",
    paste0("a", strrep("b", 128)))) {
    expect_true(is.na(dsImaging:::.safe_public_identifier(value)))
  }
})

test_that("dsImaging privacy profile is independent with a safe legacy fallback", {
  withr::local_options(list(
    dsimaging.privacy_profile = NULL,
    default.dsimaging.privacy_profile = NULL,
    dsflower.privacy_profile = "clinical_hardened",
    default.dsflower.privacy_profile = NULL
  ))
  expect_identical(dsImaging:::.get_active_profile(), "clinical_hardened")

  options(dsimaging.privacy_profile = "trusted_internal")
  expect_identical(dsImaging:::.get_active_profile(), "trusted_internal")
})

test_that("bucketed metadata never returns exact low counts", {
  withr::local_options(list(
    dsimaging.privacy_profile = "clinical_default",
    dsimaging.nfilter.subset = 3L,
    dsimaging.nfilter.tab = 3L
  ))
  expect_true(is.na(safe_metadata_count(0L)))
  expect_true(is.na(safe_metadata_count(1L)))
  expect_true(is.na(safe_metadata_count(2L)))
  expect_equal(safe_metadata_count(3L), 4L)
  expect_equal(safe_metadata_count(5L), 8L)

  distribution <- safe_metadata_distribution(c(rare = 2L, admitted = 3L))
  expect_null(distribution)
})

test_that("invalid nfilter options cannot weaken the privacy floor", {
  withr::local_options(list(
    dsimaging.nfilter.subset = 0,
    dsimaging.nfilter.tab = c(0, 1)
  ))
  expect_equal(dsImaging:::.nfilter_threshold(), 3L)
  expect_equal(dsImaging:::.get_nfilter_subset(), 3L)
  expect_equal(dsImaging:::.get_nfilter_tab(), 3L)
  expect_true(is.na(safe_metadata_count(2L)))
  expect_null(safe_metadata_distribution(c(rare = 2L)))
  expect_null(safe_metadata_distribution(c(missing = NA_integer_)))

  options(dsimaging.nfilter.subset = 3.1,
          dsimaging.nfilter.tab = Inf)
  expect_equal(dsImaging:::.get_nfilter_subset(), 4L)
  expect_equal(dsImaging:::.get_nfilter_tab(), 3L)
})

test_that("trusted profiles never bypass the DataSHIELD minimum cell size", {
  withr::local_options(list(
    dsimaging.nfilter.subset = 3L,
    dsimaging.nfilter.tab = 3L
  ))
  for (profile in c("sandbox_open", "trusted_internal")) {
    expect_true(is.na(safe_metadata_count(2L, profile = profile)))
    expect_equal(safe_metadata_count(3L, profile = profile), 3L)
    distribution <- safe_metadata_distribution(
      c(rare = 2L, admitted = 3L), profile = profile)
    expect_null(distribution)
  }
})
