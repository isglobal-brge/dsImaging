# Tests for R/validation.R

test_that(".validate_safe_path rejects empty paths", {
  expect_error(
    dsImaging:::.validate_safe_path("", "test"),
    "empty or NULL"
  )
  expect_error(
    dsImaging:::.validate_safe_path(NULL, "test"),
    "empty or NULL"
  )
})

test_that(".validate_safe_path rejects relative paths", {
  expect_error(
    dsImaging:::.validate_safe_path("relative/path", "test"),
    "must be absolute"
  )
})

test_that(".validate_safe_path rejects traversal", {
  expect_error(
    dsImaging:::.validate_safe_path("/srv/../etc/passwd", "test"),
    "traversal"
  )
})

test_that(".validate_safe_path accepts valid absolute paths", {
  expect_invisible(
    dsImaging:::.validate_safe_path("/srv/datasets/images", "test")
  )
})

test_that("validate_imaging_dataset returns structured results", {
  manifest <- list(
    version = 1,
    dataset_id = "test.dataset.v1",
    modality = "image",
    metadata = list(
      file = "/nonexistent/path/samples.csv",
      format = "csv"
    ),
    assets = list(
      images = list(type = "image_root", root = "/nonexistent/images")
    )
  )

  result <- validate_imaging_dataset(manifest)
  expect_type(result, "list")
  expect_false(result$valid)
  expect_true(length(result$errors) > 0)
})

test_that("validate_imaging_dataset passes for structurally valid manifest", {
  # Create real temp directories to pass existence checks
  tmp_meta <- tempfile(fileext = ".csv")
  tmp_images <- tempdir()
  on.exit(unlink(tmp_meta))

  writeLines("sample_id,label,image_relpath\n1,0,img1.png", tmp_meta)

  manifest <- list(
    version = 1,
    dataset_id = "test.dataset.v1",
    modality = "image",
    task_types = list("classification"),
    compatible_templates = list("pytorch_resnet18"),
    metadata = list(file = tmp_meta, format = "csv"),
    assets = list(
      images = list(type = "image_root", root = tmp_images,
                     path_col = "image_relpath")
    )
  )

  result <- validate_imaging_dataset(manifest)
  expect_true(result$valid)
  expect_equal(length(result$errors), 0)
})
