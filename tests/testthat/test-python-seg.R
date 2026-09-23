run_python_seg_case <- function(case) {
  python <- Sys.which("python3")
  skip_if(!nzchar(python), "python3 is unavailable")
  dependencies <- processx::run(python, c("-c",
    "import yaml, numpy, pydicom, SimpleITK, PIL"), error_on_status = FALSE)
  skip_if(dependencies$status != 0L, "Synthetic SEG dependencies are unavailable")
  python_dir <- system.file("python", package = "dsImaging")
  if (!nzchar(python_dir)) python_dir <- normalizePath(testthat::test_path(
    "..", "..", "inst", "python"), mustWork = TRUE)
  root <- withr::local_tempdir()
  result <- processx::run(python, c(
    testthat::test_path("fixtures", "admission_seg.py"),
    "--case", case, "--root", file.path(root, "fixture")),
    env = c(PYTHONPATH = paste(python_dir, Sys.getenv("PYTHONPATH"),
                               sep = .Platform$path.sep)),
    error_on_status = FALSE)
  expect_equal(result$status, 0L, info = result$stderr)
  if (result$status != 0L) return(NULL)
  jsonlite::fromJSON(trimws(result$stdout))
}

test_that("binary SEG selects exact segments and refuses ambiguous references or geometry", {
  result <- run_python_seg_case("seg")
  expect_identical(result$samples, 3L)
  expect_identical(result$masks, 3L)
  expect_identical(result$positive_selections, 7L)
  expect_gte(result$negative_cases, 55L)
  expect_true(result$single_frame)
  expect_true(result$empty_mask)
})

test_that("SEG verifies every mapped byte and the exact patient roster", {
  result <- run_python_seg_case("mapping")
  expect_identical(result$samples, 3L)
  expect_identical(result$verified_files, 12L)
  expect_gte(result$negative_cases, 6L)
})
