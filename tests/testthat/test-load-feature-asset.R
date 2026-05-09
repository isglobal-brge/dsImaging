test_that("imagingLoadAssetDS loads radiomics collection feature tables", {
  testthat::skip_if_not_installed("arrow")
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    nfilter.subset = 3,
    default.nfilter.subset = 3
  ))

  features_dir <- tempfile("radiomics-asset")
  dir.create(features_dir)
  arrow::write_parquet(
    data.frame(
      sample_id = c("case001", "case002", "case003"),
      original_firstorder_Mean = c(1.1, 2.2, 3.3),
      outcome = c(0L, 1L, 1L)
    ),
    file.path(features_dir, "radiomics_features.parquet")
  )

  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  asset_id <- dsImaging:::.asset_register(
    db,
    dataset_id = "lung",
    kind = "radiomics_collection",
    path_or_root = features_dir,
    derivation_hash = "load-feature-test"
  )

  df <- dsImaging:::imagingLoadAssetDS("lung", asset_id)
  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 3L)
  expect_equal(df$original_firstorder_Mean, c(1.1, 2.2, 3.3))

  subset <- dsImaging:::imagingLoadAssetDS(
    "lung", asset_id,
    columns = c("sample_id", "outcome")
  )
  expect_equal(names(subset), c("sample_id", "outcome"))
})

test_that("imagingLoadAssetDS can join dataset metadata", {
  testthat::skip_if_not_installed("arrow")
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    nfilter.subset = 3,
    default.nfilter.subset = 3
  ))

  features_dir <- tempfile("radiomics-asset")
  dir.create(features_dir)
  arrow::write_parquet(
    data.frame(
      sample_id = c("case001", "case002", "case003"),
      original_firstorder_Energy = c(10, 20, 30)
    ),
    file.path(features_dir, "radiomics_features.parquet")
  )

  metadata_path <- tempfile(fileext = ".parquet")
  arrow::write_parquet(
    data.frame(
      sample_id = c("case001", "case002", "case003"),
      age = c(68, 71, 55),
      deadstatus_event = c(1L, 0L, 1L)
    ),
    metadata_path
  )

  assign("imaging_img", list(
    dataset_id = "lung",
    manifest = list(
      dataset_id = "lung",
      metadata = list(file = metadata_path, format = "parquet")
    ),
    backend = NULL
  ), envir = dsImaging:::.dsimaging_env)
  withr::defer(rm("imaging_img", envir = dsImaging:::.dsimaging_env))

  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  asset_id <- dsImaging:::.asset_register(
    db,
    dataset_id = "lung",
    kind = "radiomics_collection",
    path_or_root = features_dir,
    derivation_hash = "load-feature-metadata-test"
  )

  df <- dsImaging:::imagingLoadAssetDS(
    "lung", asset_id,
    include_metadata = TRUE
  )
  expect_equal(names(df), c(
    "sample_id", "original_firstorder_Energy", "age", "deadstatus_event"
  ))
  expect_equal(df$age, c(68, 71, 55))

  subset <- dsImaging:::imagingLoadAssetDS(
    "lung", asset_id,
    columns = c("sample_id", "deadstatus_event"),
    include_metadata = TRUE
  )
  expect_equal(names(subset), c("sample_id", "deadstatus_event"))

  repaired <- dsImaging:::imagingLoadAssetDS(
    "lung", asset_id,
    include_metadata = TRUE,
    syntactic_names = TRUE
  )
  expect_equal(names(repaired), c(
    "sample_id", "original_firstorder_Energy", "age", "deadstatus_event"
  ))
})

test_that("imagingLoadAssetDS can repair non-syntactic radiomics names", {
  testthat::skip_if_not_installed("arrow")
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    nfilter.subset = 3,
    default.nfilter.subset = 3
  ))

  features_dir <- tempfile("radiomics-asset")
  dir.create(features_dir)
  arrow::write_parquet(
    data.frame(
      sample_id = c("case001", "case002", "case003"),
      check.names = FALSE,
      "wavelet-HLH_glrlm_RunLengthNonUniformity" = c(1, 2, 3)
    ),
    file.path(features_dir, "radiomics_features.parquet")
  )

  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  asset_id <- dsImaging:::.asset_register(
    db,
    dataset_id = "lung",
    kind = "radiomics_collection",
    path_or_root = features_dir,
    derivation_hash = "load-feature-syntactic-test"
  )

  df <- dsImaging:::imagingLoadAssetDS(
    "lung", asset_id,
    syntactic_names = TRUE
  )
  expect_true("wavelet.HLH_glrlm_RunLengthNonUniformity" %in% names(df))
})
