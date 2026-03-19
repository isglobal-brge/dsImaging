# Tests for R/manifest.R

test_that("parse_manifest validates required fields", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(version = 1), tmp)
  expect_error(parse_manifest(tmp), "missing required fields")
})

test_that("parse_manifest rejects invalid version", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    version = 2,
    dataset_id = "test.ds",
    modality = "image",
    metadata = list(file = "/tmp/test.parquet", format = "parquet")
  ), tmp)

  expect_error(parse_manifest(tmp), "Unsupported manifest version")
})

test_that("parse_manifest rejects invalid dataset_id", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    version = 1,
    dataset_id = "INVALID ID WITH SPACES",
    modality = "image",
    metadata = list(file = "/tmp/test.parquet", format = "parquet")
  ), tmp)

  expect_error(parse_manifest(tmp), "Invalid dataset_id format")
})

test_that("parse_manifest rejects non-absolute metadata path", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    version = 1,
    dataset_id = "test.ds.v1",
    modality = "image",
    metadata = list(file = "relative/path.csv", format = "csv")
  ), tmp)

  expect_error(parse_manifest(tmp), "must be absolute")
})

test_that("parse_manifest rejects path traversal", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    version = 1,
    dataset_id = "test.ds.v1",
    modality = "image",
    metadata = list(file = "/srv/datasets/../../../etc/passwd", format = "csv")
  ), tmp)

  expect_error(parse_manifest(tmp), "traversal")
})

test_that("parse_manifest accepts valid manifest", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    version = 1,
    dataset_id = "radiology.chest_xray.v3",
    title = "Chest X-ray Dataset v3",
    modality = "image",
    task_types = list("classification"),
    metadata = list(
      file = "/srv/datasets/samples.parquet",
      format = "parquet",
      id_col = "sample_id",
      label_col = "label"
    )
  ), tmp)

  manifest <- parse_manifest(tmp)
  expect_equal(manifest$dataset_id, "radiology.chest_xray.v3")
  expect_equal(manifest$modality, "image")
})

test_that("validate_asset rejects missing root for image_root", {
  expect_error(
    validate_asset(list(type = "image_root"), "images"),
    "missing 'root'"
  )
})

test_that("validate_asset rejects missing file for feature_table", {
  expect_error(
    validate_asset(list(type = "feature_table"), "features"),
    "missing 'file'"
  )
})

test_that("validate_asset rejects unknown asset type", {
  expect_error(
    validate_asset(list(type = "video_stream"), "videos"),
    "Unknown asset type"
  )
})

# -- v2 asset type tests --

test_that("validate_asset accepts wsi_root", {
  expect_invisible(
    validate_asset(list(type = "wsi_root", root = "/srv/wsi"), "slides")
  )
})

test_that("validate_asset validates wsi_root tile_size", {
  expect_error(
    validate_asset(list(type = "wsi_root", root = "/srv/wsi",
                         tile_size = -1), "slides"),
    "tile_size"
  )
})

test_that("validate_asset validates wsi_root magnification", {
  expect_error(
    validate_asset(list(type = "wsi_root", root = "/srv/wsi",
                         magnification = "bad"), "slides"),
    "magnification"
  )
})

test_that("validate_asset accepts dicom_series_root", {
  expect_invisible(
    validate_asset(list(type = "dicom_series_root",
                         root = "/srv/dicom_series"), "series")
  )
})

test_that("validate_asset accepts rt_struct_root", {
  expect_invisible(
    validate_asset(list(type = "rt_struct_root",
                         root = "/srv/rt_structs"), "contours")
  )
})

test_that("validate_asset accepts rt_dose_file", {
  expect_invisible(
    validate_asset(list(type = "rt_dose_file",
                         file = "/srv/rt/dose.dcm"), "dose")
  )
})

test_that("validate_asset accepts rt_plan_file", {
  expect_invisible(
    validate_asset(list(type = "rt_plan_file",
                         file = "/srv/rt/plan.dcm"), "plan")
  )
})

test_that("validate_asset accepts multimodal_ref", {
  expect_invisible(
    validate_asset(list(type = "multimodal_ref",
                         manifest = "/srv/other/manifest.yml"), "pet_scan")
  )
})

test_that("validate_asset rejects multimodal_ref without manifest", {
  expect_error(
    validate_asset(list(type = "multimodal_ref"), "pet"),
    "missing 'manifest'"
  )
})

test_that("parse_manifest accepts v2 asset types", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    version = 1,
    dataset_id = "pathology.breast.v1",
    modality = "wsi",
    metadata = list(file = "/srv/data/samples.csv", format = "csv"),
    assets = list(
      slides = list(
        type = "wsi_root",
        root = "/srv/data/wsi",
        path_col = "slide_path",
        tile_size = 512L,
        magnification = 20
      ),
      radiomics = list(
        type = "feature_table",
        file = "/srv/data/features.parquet",
        join_key = "slide_id"
      )
    )
  ), tmp)

  manifest <- parse_manifest(tmp)
  expect_equal(manifest$dataset_id, "pathology.breast.v1")
  expect_equal(manifest$assets$slides$type, "wsi_root")
  expect_equal(manifest$assets$slides$tile_size, 512L)
})
