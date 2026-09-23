run_python_admission_case <- function(case) {
  python <- Sys.which("python3")
  skip_if(!nzchar(python), "python3 is unavailable")
  dependencies <- processx::run(python, c("-c",
    "import yaml, numpy, pydicom, SimpleITK, PIL, rt_utils"),
    error_on_status = FALSE)
  skip_if(dependencies$status != 0L,
          "Synthetic DICOM/RT/slide integration dependencies are unavailable")
  python_dir <- system.file("python", package = "dsImaging")
  if (!nzchar(python_dir)) {
    python_dir <- normalizePath(testthat::test_path(
      "..", "..", "inst", "python"), mustWork = TRUE)
  }
  root <- withr::local_tempdir()
  result <- processx::run(python, c(
    testthat::test_path("fixtures", "admission_contract.py"),
    "--case", case, "--root", file.path(root, "fixture")),
    env = c(PYTHONPATH = paste(python_dir, Sys.getenv("PYTHONPATH"),
                               sep = .Platform$path.sep)),
    error_on_status = FALSE)
  expect_equal(result$status, 0L, info = result$stderr)
  if (result$status != 0L) return(NULL)
  jsonlite::fromJSON(trimws(result$stdout))
}

test_that("synthetic DICOM groups verify every admitted byte and exact file set", {
  result <- run_python_admission_case("mapping")
  expect_identical(result$samples, 3L)
  expect_identical(result$verified_files, 9L)
  expect_gte(result$negative_cases, 4L)
})

test_that("synthetic series convert only with exact patient and geometry association", {
  result <- run_python_admission_case("dicom")
  expect_identical(result$samples, 3L)
  expect_gte(result$negative_cases, 4L)
})

test_that("pydicom RTSTRUCT fixtures produce one exact mask for each sample", {
  result <- run_python_admission_case("rt")
  expect_identical(result$masks, 3L)
  expect_gte(result$negative_cases, 5L)
})

test_that("pydicom dose and plan fixtures produce complete per-ROI tables", {
  result <- run_python_admission_case("dose")
  expect_identical(result$samples, 3L)
  expect_identical(result$rows, 6L)
  expect_gte(result$negative_cases, 1L)
})

test_that("synthetic Pillow slides retain private exact tile fan-out", {
  result <- run_python_admission_case("wsi")
  expect_identical(result$slides, 3L)
  expect_identical(result$tiles, 4L)
  expect_identical(result$zero_tile_slides, 1L)
  expect_gte(result$negative_cases, 1L)
})

test_that("MONAI fake provider refuses missing extra or misplaced sample outputs", {
  result <- run_python_admission_case("monai")
  expect_identical(result$masks, 3L)
  expect_gte(result$negative_cases, 3L)
})

test_that("DataSHIELD routes execute and publish the same sealed synthetic inputs", {
  python <- Sys.which("python3")
  skip_if(!nzchar(python), "python3 is unavailable")
  dependencies <- processx::run(python, c("-c",
    "import yaml, numpy, pydicom, SimpleITK, PIL, rt_utils"),
    error_on_status = FALSE)
  skip_if(dependencies$status != 0L,
          "Synthetic DICOM/RT/slide integration dependencies are unavailable")
  python_dir <- system.file("python", package = "dsImaging")
  if (!nzchar(python_dir)) python_dir <- normalizePath(testthat::test_path(
    "..", "..", "inst", "python"), mustWork = TRUE)
  root <- withr::local_tempdir()
  source <- file.path(root, "source")
  generated <- processx::run(python, c(
    testthat::test_path("fixtures", "admission_contract.py"),
    "--case", "fixture", "--root", source),
    env = c(PYTHONPATH = python_dir), error_on_status = FALSE)
  expect_equal(generated$status, 0L, info = generated$stderr)
  if (generated$status != 0L) return(invisible(NULL))
  manifest_path <- file.path(source, "manifest.yaml")
  withr::local_options(list(
    dsimaging.asset_db = file.path(root, "assets.sqlite"),
    dsimaging.registry_path = file.path(root, "registry.yaml"),
    dsimaging.worker_context_dir = file.path(root, "contexts")))
  register_dataset("synthetic.admission", manifest_path, storage_backend("file"))
  session <- new.env(parent = globalenv())
  resource <- resourcer::newResource("images",
    "imaging+dataset://synthetic.admission")
  assign("res", ImagingDatasetResourceClient$new(resource), session)
  assign("img", eval(quote(imagingInitDS("res")), session), session)
  submitted <- NULL
  testthat::local_mocked_bindings(
    hpcUnitSelectionInternal = function(...) list(),
    hpcRuntimeIdentityInternal = function(...) strrep("a", 64),
    .package = "dsHPC")
  testthat::local_mocked_bindings(
    find_asset_by_hash = function(...) NULL,
    .content_hash_for_resource = function(...) NULL,
    .imaging_submit_job = function(job, ...) {
      submitted <<- job
      structure(list(capability = paste0("imgw_", strrep("a", 64))),
        class = "dsimaging_workflow_ref")
    }, .package = "dsImaging")
  cases <- list(
    dicom_convert = list(dicom_asset = "dicom"),
    rt_convert = list(rt_asset = "rt_struct", dicom_asset = "dicom", rois = "target"),
    rt_dose_plan = list(dose_asset = "rt_dose", plan_asset = "rt_plan", mask_asset = "masks"),
    wsi_tile = list(wsi_asset = "wsi", tile_size = 16L, stride = 16L, max_tiles = 2L),
    monai_bundle_infer = list(bundle_name = "synthetic"))
  scripts <- c(dicom_convert = "dsimaging_dicom_convert.py",
    rt_convert = "dsimaging_rt_convert.py", rt_dose_plan = "dsimaging_rt_dose.py",
    wsi_tile = "dsimaging_wsi_tile.py", monai_bundle_infer = "dsimaging_seg_monai.py")
  for (runner in names(cases)) {
    if (identical(runner, "monai_bundle_infer")) {
      request <- dsImaging:::.dsr_encode(list(handle = "img", image_asset = "images",
        segmenter = list(provider = "monai", bundle_name = "synthetic")))
      result <- eval(substitute(imagingProcessSegmentationCollectionDS(REQUEST),
                                list(REQUEST = request)), session)
    } else {
      request <- dsImaging:::.dsr_encode(list(handle = "img", runner = runner,
        config = cases[[runner]], output_asset = "derived"))
      result <- eval(substitute(imagingProcessAssetWorkflowDS(REQUEST),
                                list(REQUEST = request)), session)
    }
    expect_s3_class(result, "dsimaging_workflow_ref")
    expect_identical(names(unclass(result)), "capability")
    step <- submitted$steps[[2]]
    publish <- submitted$steps[[3]]
    expect_identical(step$runner, runner)
    env <- vapply(step$config, function(value) {
      if (is.logical(value)) tolower(as.character(value)) else as.character(value)
    }, character(1))
    names(env) <- paste0("DSHPC_CFG_", toupper(names(env)))
    env <- c(env, PYTHONPATH = paste(file.path(source, "fake_modules"), python_dir,
                                    sep = .Platform$path.sep),
      DSIMAGING_MODELS = file.path(source, "models"),
      DSIMAGING_WORKER_CONTEXT_DIR = file.path(root, "contexts"), DSHPC_INPUT_DIR = "")
    output <- file.path(root, runner)
    args <- c(file.path(python_dir, scripts[[runner]]), "--input", source,
              "--output", output)
    if (identical(runner, "monai_bundle_infer")) args <- c(args, "--bundle", "synthetic")
    execution <- processx::run(python, args, env = env, error_on_status = FALSE)
    expect_equal(execution$status, 0L, info = paste(runner, execution$stderr))
    if (execution$status != 0L) next
    seal <- dsImaging:::.assert_publishable_imaging_feature_asset(
      output, publish$asset_type, step$config, runner = runner)
    expect_match(seal, "^[0-9a-f]{64}$")
    expect_false(any(grepl("PHI_CASE|patient-[ABC]", execution$stdout)))
  }
})
