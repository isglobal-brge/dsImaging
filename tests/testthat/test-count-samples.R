# Regression tests for .count_samples_from_manifest.
# A file-backend manifest using only the documented `metadata.uri` field
# previously failed dataset init with "sample count is unknown" because the
# counter only honored `metadata.file` and s3:// URIs.

test_that("sample count honors a local absolute metadata.uri", {
  csv <- tempfile(fileext = ".csv")
  on.exit(unlink(csv))
  writeLines(c("sample_id,age", paste0("case00", 1:6, ",50")), csv)

  manifest <- list(metadata = list(uri = csv, format = "csv"))
  expect_equal(dsImaging:::.count_samples_from_manifest(manifest), 6L)
})

test_that("sample count guesses csv format from a metadata.uri extension", {
  csv <- tempfile(fileext = ".csv")
  on.exit(unlink(csv))
  writeLines(c("sample_id", "a", "b", "c"), csv)

  manifest <- list(metadata = list(uri = csv))
  expect_equal(dsImaging:::.count_samples_from_manifest(manifest), 3L)
})

test_that("sample count still prefers metadata.file when both are present", {
  f <- tempfile(fileext = ".csv")
  u <- tempfile(fileext = ".csv")
  on.exit(unlink(c(f, u)))
  writeLines(c("sample_id", "a", "b"), f)
  writeLines(c("sample_id", "a", "b", "c"), u)

  manifest <- list(metadata = list(file = f, uri = u, format = "csv"))
  expect_equal(dsImaging:::.count_samples_from_manifest(manifest), 2L)
})

test_that("sample count remains NA for an s3 metadata.uri without backend", {
  manifest <- list(metadata = list(
    uri = "s3://bucket/datasets/x/metadata/samples.csv", format = "csv"))
  expect_true(is.na(dsImaging:::.count_samples_from_manifest(manifest)))
})
