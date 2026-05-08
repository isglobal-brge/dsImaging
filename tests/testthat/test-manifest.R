# Tests for R/manifest.R

test_that("parse_manifest validates required fields", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(schema_version = 1), tmp)
  expect_error(parse_manifest(tmp), "dataset_id")
})

test_that("parse_manifest rejects invalid schema version", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    schema_version = 0,
    dataset_id = "test.ds",
    metadata = list(uri = "/tmp/test.parquet", format = "parquet")
  ), tmp)

  expect_error(parse_manifest(tmp), "schema_version")
})

test_that("parse_manifest rejects invalid dataset_id", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    schema_version = 1,
    dataset_id = "INVALID ID WITH SPACES",
    metadata = list(uri = "/tmp/test.parquet", format = "parquet")
  ), tmp)

  expect_error(parse_manifest(tmp), "dataset_id")
})

test_that("parse_manifest rejects non-absolute metadata path", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    schema_version = 1,
    dataset_id = "test.ds.v1",
    metadata = list(uri = "relative/path.csv", format = "csv")
  ), tmp)

  expect_error(parse_manifest(tmp), "must be absolute")
})

test_that("parse_manifest rejects path traversal", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    schema_version = 1,
    dataset_id = "test.ds.v1",
    metadata = list(uri = "/srv/datasets/../../../etc/passwd", format = "csv")
  ), tmp)

  expect_error(parse_manifest(tmp), "traversal")
})

test_that("parse_manifest accepts valid manifest", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    schema_version = 1,
    dataset_id = "radiology.chest_xray.v3",
    title = "Chest X-ray Dataset v3",
    metadata = list(
      uri = "/srv/datasets/samples.parquet",
      format = "parquet",
      id_col = "sample_id",
      label_col = "label"
    ),
    assets = list(
      images = list(kind = "image_root", uri = "/srv/datasets/images")
    )
  ), tmp)

  manifest <- parse_manifest(tmp)
  expect_equal(manifest$dataset_id, "radiology.chest_xray.v3")
  expect_equal(manifest$assets$images$kind, "image_root")
})

test_that("validate_asset rejects missing uri", {
  expect_error(
    validate_asset(list(kind = "image_root"), "images"),
    "uri required"
  )
})

test_that("validate_asset rejects unknown asset kind", {
  expect_error(
    validate_asset(list(kind = "video_stream", uri = "/srv/videos"), "videos"),
    "unknown kind"
  )
})

test_that("validate_asset accepts supported imaging asset kinds", {
  kinds <- c("image_root", "mask_root", "feature_table", "wsi_root",
             "dicom_series_root", "rt_struct_root", "rt_dose_file",
             "rt_plan_file", "multimodal_ref")
  for (kind in kinds) {
    expect_invisible(
      validate_asset(list(kind = kind, uri = file.path("/srv/data", kind)),
        kind)
    )
  }
})

test_that("parse_manifest accepts advanced asset kinds", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    schema_version = 1,
    dataset_id = "pathology.breast.v1",
    metadata = list(uri = "/srv/data/samples.csv", format = "csv"),
    assets = list(
      slides = list(
        kind = "wsi_root",
        uri = "/srv/data/wsi",
        path_col = "slide_path",
        tile_size = 512L,
        magnification = 20
      ),
      radiomics = list(
        kind = "feature_table",
        uri = "/srv/data/features.parquet",
        join_key = "slide_id"
      )
    )
  ), tmp)

  manifest <- parse_manifest(tmp)
  expect_equal(manifest$dataset_id, "pathology.breast.v1")
  expect_equal(manifest$assets$slides$kind, "wsi_root")
  expect_equal(manifest$assets$slides$tile_size, 512L)
})
