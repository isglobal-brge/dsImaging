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
