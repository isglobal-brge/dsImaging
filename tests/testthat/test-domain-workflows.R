test_that("radiomics domain method composes a labelled dsHPC job", {
  submitted <- NULL
  testthat::local_mocked_bindings(
    find_asset_by_hash = function(dataset_id, derivation_hash) NULL,
    contentHashDS = function(resource_name) {
      list(resource_name = resource_name, content_hash = NA_character_,
        updated_at = NA_character_, source = "unsupported")
    },
    .package = "dsImaging"
  )
  testthat::local_mocked_bindings(
    hpcSubmitDS = function(spec_encoded) {
      submitted <<- dsImaging:::.dsr_decode(spec_encoded)
      list(job_id = submitted$job_id, state = "PENDING",
        name = submitted$name, submitted_at = "2026-05-25T10:00:00Z")
    },
    .package = "dsHPC"
  )

  req <- list(dataset_id = "lung1", image_asset = "images",
    mask_asset = "gtv", profile = list(name = "demo_ct_firstorder_v1",
      bin_width = 25, force2D = FALSE, feature_classes = "firstorder"),
    job_id = "job_domain_test")
  out <- imagingProcessRadiomicsAssetDS(dsImaging:::.dsr_encode(req))

  expect_equal(out$job_id, "job_domain_test")
  expect_equal(submitted$label, "dsImaging")
  expect_equal(submitted$steps[[2]]$runner, "pyradiomics_extract")
  expect_equal(submitted$steps[[3]]$publish_kind, "imaging_radiomics_asset")
})

test_that("profile aggregate methods expose safe profile metadata", {
  profiles <- imagingListProfilesDS()
  expect_s3_class(profiles, "data.frame")
  if (nrow(profiles) > 0L) {
    desc <- imagingDescribeProfileDS(profiles$profile_name[[1]])
    expect_true(is.list(desc))
    expect_true(nzchar(desc$name))
  }
})

test_that("domain submissions reject missing labels before dsHPC submit", {
  expect_error(
    dsImaging:::.imaging_submit_job(list(
      steps = list(list(type = "emit", plane = "session",
        output_name = "out", value = 1))
    )),
    "non-empty label"
  )
})
