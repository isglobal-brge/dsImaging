run_python_dose_labels_case <- function(case) {
  python <- Sys.which("python3")
  skip_if(!nzchar(python), "python3 is unavailable")
  dependencies <- processx::run(python, c("-c",
    "import yaml, numpy, pydicom, SimpleITK, PIL, nibabel"), error_on_status = FALSE)
  skip_if(dependencies$status != 0L,
          "Synthetic labelled dose integration dependencies are unavailable")
  python_dir <- system.file("python", package = "dsImaging")
  if (!nzchar(python_dir)) python_dir <- normalizePath(testthat::test_path(
    "..", "..", "inst", "python"), mustWork = TRUE)
  root <- withr::local_tempdir()
  result <- processx::run(python, c(
    testthat::test_path("fixtures", "admission_dose_labels.py"),
    "--case", case, "--root", file.path(root, "fixture")),
    env = c(PYTHONPATH = paste(python_dir, Sys.getenv("PYTHONPATH"),
                               sep = .Platform$path.sep)),
    error_on_status = FALSE)
  expect_equal(result$status, 0L, info = result$stderr)
  if (result$status != 0L) return(NULL)
  jsonlite::fromJSON(trimws(result$stdout))
}

test_that("labelled dose rows use only the complete public ROI vocabulary", {
  result <- run_python_dose_labels_case("labels")
  expect_identical(result$samples, 3L)
  expect_identical(result$rows, 9L)
  expect_identical(result$missing_rows, 4L)
  expect_identical(result$empty_rows, 9L)
  expect_true(result$ignored_private_labels)
  expect_true(result$scaled_labels)
})

test_that("sets of independently mapped masks retain exact dose association", {
  result <- run_python_dose_labels_case("sets")
  expect_identical(result$samples, 3L)
  expect_identical(result$rows, 6L)
  expect_identical(result$mask_assets, 2L)
  expect_identical(result$negative_cases, 4L)
})

test_that("labelled dose refuses ambiguous schemas associations and mask geometry", {
  result <- run_python_dose_labels_case("failures")
  expect_identical(result$schema_refusals, 15L)
  expect_identical(result$negative_cases, 34L)
})
