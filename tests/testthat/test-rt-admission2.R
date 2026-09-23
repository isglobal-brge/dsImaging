test_that("SEG and labelled-dose DataSHIELD requests execute from sealed maps", {
  python <- Sys.which("python3")
  skip_if(!nzchar(python), "python3 is unavailable")
  dependencies <- processx::run(python, c("-c",
    "import yaml, numpy, pydicom, SimpleITK, PIL, rt_utils, nibabel"),
    error_on_status = FALSE)
  skip_if(dependencies$status != 0L, "Synthetic RT integration dependencies are unavailable")
  python_dir <- system.file("python", package = "dsImaging")
  if (!nzchar(python_dir)) python_dir <- normalizePath(testthat::test_path(
    "..", "..", "inst", "python"), mustWork = TRUE)
  root <- withr::local_tempdir()
  withr::local_options(list(dsimaging.asset_db = file.path(root, "assets.sqlite"),
    dsimaging.registry_path = file.path(root, "registry.yaml"),
    dsimaging.worker_context_dir = file.path(root, "contexts")))
  submitted <- NULL
  testthat::local_mocked_bindings(
    hpcUnitSelectionInternal = function(...) list(),
    hpcRuntimeIdentityInternal = function(...) strrep("a", 64), .package = "dsHPC")
  testthat::local_mocked_bindings(find_asset_by_hash = function(...) NULL,
    .content_hash_for_resource = function(...) NULL,
    .imaging_submit_job = function(job, ...) {
      submitted <<- job
      structure(list(capability = paste0("imgw_", strrep("a", 64))),
        class = "dsimaging_workflow_ref")
    }, .package = "dsImaging")
  cases <- list(
    seg_label = list(fixture = "admission_seg.py", runner = "rt_convert",
      script = "dsimaging_rt_convert.py", assets = c("rt_seg", "dicom"),
      config = list(rt_asset = "rt_seg", dicom_asset = "dicom", rois = "target")),
    seg_number = list(fixture = "admission_seg.py", runner = "rt_convert",
      script = "dsimaging_rt_convert.py", assets = c("rt_seg", "dicom"),
      config = list(rt_asset = "rt_seg", dicom_asset = "dicom", segment_numbers = "1,7")),
    labelled = list(fixture = "admission_dose_labels.py", runner = "rt_dose_plan",
      script = "dsimaging_rt_dose.py", assets = c("rt_dose", "rt_plan", "masks"),
      config = list(mask_asset = "masks", roi_labels = "tumour,organ_at_risk,absent",
        mask_labels = "1,2,7")),
    mask_set = list(fixture = "admission_dose_labels.py", runner = "rt_dose_plan",
      script = "dsimaging_rt_dose.py",
      assets = c("rt_dose", "rt_plan", "tumour_masks", "oar_masks"),
      config = list(mask_assets = "tumour_masks,oar_masks", roi_labels = "tumour,organ_at_risk",
        mask_labels = "1,1")))
  for (name in names(cases)) {
    case <- cases[[name]]
    source <- file.path(root, name)
    generated <- processx::run(python, c(testthat::test_path("fixtures", case$fixture),
      "--case", "fixture", "--root", source), env = c(PYTHONPATH = python_dir),
      error_on_status = FALSE)
    expect_equal(generated$status, 0L, info = generated$stderr)
    if (generated$status != 0L) next
    register_dataset("synthetic.admission", file.path(source, "manifest.yaml"),
      storage_backend("file"))
    session <- new.env(parent = globalenv())
    resource <- resourcer::newResource("images", "imaging+dataset://synthetic.admission")
    assign("res", ImagingDatasetResourceClient$new(resource), session)
    assign("img", eval(quote(imagingInitDS("res")), session), session)
    request <- dsImaging:::.dsr_encode(list(handle = "img", runner = case$runner,
      config = case$config, output_asset = "derived"))
    result <- eval(substitute(imagingProcessAssetWorkflowDS(REQUEST),
      list(REQUEST = request)), session)
    expect_s3_class(result, "dsimaging_workflow_ref")
    expect_identical(names(unclass(result)), "capability")
    step <- submitted$steps[[2]]
    publish <- submitted$steps[[3]]
    context <- yaml::read_yaml(step$config$worker_context)
    expect_setequal(context$collection_map$asset_names, case$assets)
    for (records in context$collection_map$records_by_asset) {
      expect_length(records, 3L)
      expect_setequal(vapply(records, `[[`, character(1), "privacy_id"),
        c("patient-A", "patient-B", "patient-C"))
    }
    env <- vapply(step$config, as.character, character(1))
    names(env) <- paste0("DSHPC_CFG_", toupper(names(env)))
    env <- c(env, PYTHONPATH = python_dir, DSHPC_INPUT_DIR = "",
      DSIMAGING_WORKER_CONTEXT_DIR = file.path(root, "contexts"))
    output <- file.path(root, paste0(name, "_output"))
    execution <- processx::run(python, c(file.path(python_dir, case$script),
      "--input", source, "--output", output), env = env, error_on_status = FALSE)
    expect_equal(execution$status, 0L, info = execution$stderr)
    if (execution$status != 0L) next
    expect_match(dsImaging:::.assert_publishable_imaging_feature_asset(
      output, publish$asset_type, step$config, case$runner), "^[0-9a-f]{64}$")
    expect_false(grepl("PHI_CASE|patient-[ABC]|dose_mean", execution$stdout))
    if (identical(case$runner, "rt_dose_plan")) {
      db <- dsImaging:::.asset_db_connect()
      asset_id <- dsImaging:::.asset_register(db, "synthetic.admission", "dose_table",
        output, visibility = "global", collection_seal = context$manifest$.dsimaging_collection_seal,
        provenance = list(runner = case$runner, config = step$config))
      dsImaging:::.asset_db_close(db)
      table <- eval(substitute(imagingLoadAssetDS("img", ASSET), list(ASSET = asset_id)), session)
      expect_equal(nrow(table), if (identical(name, "labelled")) 9L else 6L)
      expect_true(all(is.na(table$dose_mean[table$dose_voxels == 0])))
      expect_setequal(table$roi_label, strsplit(case$config$roi_labels, ",", fixed = TRUE)[[1]])
    }
  }
})
