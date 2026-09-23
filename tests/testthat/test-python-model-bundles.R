run_python_model_case <- function(case) {
  python <- Sys.which("python3")
  skip_if(!nzchar(python), "python3 is unavailable")
  if (identical(case, "runners")) {
    dependencies <- processx::run(python,
      c("-c", "import numpy, SimpleITK"), error_on_status = FALSE)
    skip_if(dependencies$status != 0L,
            "Synthetic model runner dependencies are unavailable")
  }
  python_dir <- system.file("python", package = "dsImaging")
  if (!nzchar(python_dir)) python_dir <- normalizePath(testthat::test_path(
    "..", "..", "inst", "python"), mustWork = TRUE)
  root <- withr::local_tempdir()
  result <- processx::run(python, c(
    testthat::test_path("fixtures", "model_bundles.py"),
    "--case", case, "--root", file.path(root, "fixture")),
    env = c(PYTHONPATH = paste(python_dir, Sys.getenv("PYTHONPATH"),
                               sep = .Platform$path.sep)),
    error_on_status = FALSE)
  expect_equal(result$status, 0L, info = paste(result$stderr, result$stdout))
  if (result$status != 0L) return(NULL)
  jsonlite::fromJSON(trimws(result$stdout))
}

test_that("registered bundles verify exact bytes, safe paths and complete provider models", {
  result <- run_python_model_case("verification")
  expect_gte(result$negative_cases, 25L)
  expect_identical(result$listed_bundles, 1L)
  expect_true(result$digest_pinned)
  expect_true(result$crop_required)
  expect_identical(result$checkpoint_refusals, 6L)
})

test_that("admin installation registers only complete verified downloads and is idempotent", {
  result <- run_python_model_case("installer")
  expect_identical(result$downloads, 8L)
  expect_identical(result$negative_cases, 7L)
  expect_true(result$idempotent)
  expect_true(result$complete_manifest)
  expect_true(result$rollback_preserved)
})

test_that("pinned archives verify every member and refuse unsafe extraction", {
  result <- run_python_model_case("archives")
  expect_identical(result$verified_archives, 1L)
  expect_identical(result$negative_cases, 5L)
})

test_that("every provider uses local bundles and refuses direct and child downloads", {
  result <- run_python_model_case("runners")
  expect_identical(result$providers, 4L)
  expect_identical(result$download_refusals, 12L)
  expect_identical(result$negative_cases, 13L)
  expect_identical(result$cache_refusals, 4L)
  expect_true(result$explicit_paths)
  expect_true(result$offline)
  expect_true(result$composite_lungmask)
})
