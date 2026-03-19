# Tests for R/descriptor.R

test_that("imaging_dataset_descriptor creates valid object", {
  manifest <- list(
    dataset_id = "test.dataset.v1",
    modality = "image",
    title = "Test Dataset",
    metadata = list(file = "/tmp/test.csv", format = "csv"),
    assets = list(
      images = list(type = "image_root", root = "/tmp/images")
    )
  )

  desc <- imaging_dataset_descriptor(manifest)
  expect_s3_class(desc, "ImagingDatasetDescriptor")
  expect_s3_class(desc, "FlowerDatasetDescriptor")
  expect_equal(desc$dataset_id, "test.dataset.v1")
  expect_equal(desc$source_kind, "image_bundle")
})

test_that("imaging_dataset_descriptor errors without dataset_id", {
  expect_error(
    imaging_dataset_descriptor(list(modality = "image")),
    "dataset_id"
  )
})

test_that("print method works for ImagingDatasetDescriptor", {
  manifest <- list(
    dataset_id = "test.ds.v1",
    modality = "image",
    title = "Test",
    task_types = list("classification"),
    metadata = list(file = "/tmp/test.csv"),
    assets = list(images = list(type = "image_root", root = "/tmp"))
  )

  desc <- imaging_dataset_descriptor(manifest)
  output <- capture.output(print(desc))
  expect_true(any(grepl("ImagingDatasetDescriptor", output)))
  expect_true(any(grepl("test.ds.v1", output)))
})
