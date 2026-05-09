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
