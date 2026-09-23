# Exact source associations for the newly admitted clinical routes.
add_test_source_map <- function(collection, name, kind, groups = FALSE) {
  root <- dirname(collection$manifest_path)
  source <- file.path(root, "source", name)
  dir.create(source, recursive = TRUE)
  rows <- utils::read.csv(collection$manifest$metadata$uri,
                           stringsAsFactors = FALSE)
  ids <- rows$sample_id
  files <- lapply(seq_along(ids), function(i) {
    relative <- if (groups) paste0("group-", i, "/slice-", 1:3, ".dcm") else
      paste0("opaque-", i, ".dcm")
    lapply(relative, function(path) {
      dest <- file.path(source, path)
      dir.create(dirname(dest), recursive = TRUE, showWarnings = FALSE)
      writeBin(charToRaw(paste(name, path)), dest)
      list(path = path, role = if (groups) "slice" else "primary",
        content_hash = digest::digest(file = dest, algo = "sha256"),
        size = as.numeric(file.info(dest)$size))
    })
  })
  hashes <- vapply(files, function(group) {
    if (!groups) return(group[[1]]$content_hash)
    text <- paste0(vapply(group, function(file) paste0(
      file$path, "\t", file$size, "\t", file$content_hash, "\n"),
      character(1)), collapse = "")
    digest::digest(text, algo = "sha256", serialize = FALSE)
  }, character(1))
  relative <- if (groups) paste0("group-", seq_along(ids)) else
    vapply(files, function(group) group[[1]]$path, character(1))
  source_kind <- if (groups) "dicom_series" else "single_file"
  sm <- file.path(root, "metadata", paste0(name, "-samples.csv"))
  idx <- file.path(root, "indexes", paste0(name, "-index.csv"))
  utils::write.csv(data.frame(
    sample_id = ids, source_kind = source_kind,
    primary_uri = if (groups) "" else relative,
    files_json = vapply(files, jsonlite::toJSON, character(1), auto_unbox = TRUE),
    content_hash = hashes, n_files = if (groups) 3L else 1L),
    sm, row.names = FALSE)
  utils::write.csv(data.frame(
    sample_id = ids, source_kind = source_kind,
    uri = file.path(source, relative), content_hash = hashes,
    size = vapply(files, function(group) sum(vapply(
      group, `[[`, numeric(1), "size")), numeric(1))), idx, row.names = FALSE)
  collection$manifest$assets[[name]] <- list(kind = kind, uri = source,
    sample_manifests = list(uri = sm, format = "csv"),
    content_hash_index = list(uri = idx, format = "csv"))
  yaml::write_yaml(collection$manifest, collection$manifest_path)
  collection
}

admission_test_collection <- function(root) {
  collection <- write_test_imaging_collection(root, "admission.test",
    data.frame(sample_id = paste0("scan", 1:3),
      patient_id = paste0("patient", 1:3)))
  for (name in c("dicom", "rt_struct", "rt_dose", "rt_plan", "wsi")) {
    kind <- switch(name, dicom = "dicom_series_root",
      rt_struct = "rt_struct_root", rt_dose = "rt_dose_file",
      rt_plan = "rt_plan_file", wsi = "wsi_root")
    collection <- add_test_source_map(collection, name, kind,
                                       groups = identical(name, "dicom"))
  }
  collection
}

admission_test_handle <- function(collection) {
  dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(collection$manifest), "img",
    backend = storage_backend("file"),
    manifest_uri = collection$manifest_path, require_snapshot = TRUE)
}

test_that("secondary source maps and every series file enter the collection seal", {
  collection <- admission_test_collection(withr::local_tempdir())
  handle <- admission_test_handle(collection)
  snapshot <- handle$collection_snapshot
  expect_setequal(names(snapshot$records_by_asset),
    c("images", "dicom", "rt_struct", "rt_dose", "rt_plan", "wsi"))
  expect_identical(vapply(snapshot$records_by_asset$dicom,
    `[[`, character(1), "privacy_id"), paste0("patient", 1:3))
  expect_length(snapshot$records_by_asset$dicom[[1]]$files, 3L)
  expect_true(all(vapply(snapshot$records_by_asset$dicom[[1]]$files,
    function(file) grepl("^[0-9a-f]{64}$", file$content_hash), logical(1))))

  authorized <- list(handle = handle, manifest = handle$manifest,
    dataset_id = handle$dataset_id, backend = handle$backend,
    privacy_roster = handle$privacy_roster)
  worker <- dsImaging:::.imaging_worker_manifest(authorized,
    asset_names = c("dicom", "rt_struct"))
  mapping <- dsImaging:::.imaging_worker_collection_map(authorized, worker)
  expect_identical(mapping$records_by_asset$dicom[[1]]$files,
                   snapshot$records_by_asset$dicom[[1]]$files)

  session <- new.env(parent = globalenv())
  assign("img", dsImaging:::.register_imaging_handle(handle, session), session)
  index <- collection$manifest$assets$rt_dose$content_hash_index$uri
  rows <- utils::read.csv(index, stringsAsFactors = FALSE)
  rows$content_hash[[1]] <- strrep("a", 64)
  utils::write.csv(rows, index, row.names = FALSE)
  expect_error(eval(quote(imagingMetadataDS("img")), session), "integrity|snapshot")
})

test_that("ambiguous, incomplete and unsealed source groups fail admission", {
  for (mutation in c("missing", "extra", "unhashed", "wrong_hash", "duplicate",
                     "noncanonical")) {
    collection <- admission_test_collection(withr::local_tempdir())
    asset <- collection$manifest$assets$dicom
    sm <- utils::read.csv(asset$sample_manifests$uri, stringsAsFactors = FALSE)
    if (mutation == "noncanonical") {
      sm$sample_id[[1]] <- paste0(" ", sm$sample_id[[1]])
      utils::write.csv(sm, asset$sample_manifests$uri, row.names = FALSE)
    }
    if (mutation == "missing") unlink(file.path(asset$uri, "group-1/slice-2.dcm"))
    if (mutation == "extra") writeLines("extra", file.path(asset$uri, "group-1/extra.dcm"))
    if (mutation %in% c("unhashed", "wrong_hash", "duplicate")) {
      files <- jsonlite::fromJSON(sm$files_json[[1]], simplifyVector = FALSE)
      if (mutation == "unhashed") files[[1]]$content_hash <- NULL
      if (mutation == "wrong_hash") files[[1]]$content_hash <- strrep("b", 64)
      if (mutation == "duplicate") files[[2]]$path <- files[[1]]$path
      sm$files_json[[1]] <- jsonlite::toJSON(files, auto_unbox = TRUE)
      utils::write.csv(sm, asset$sample_manifests$uri, row.names = FALSE)
    }
    expect_error(admission_test_handle(collection), "integrity", info = mutation)
  }
  collection <- admission_test_collection(withr::local_tempdir())
  asset <- collection$manifest$assets$rt_plan
  rows <- utils::read.csv(asset$sample_manifests$uri, stringsAsFactors = FALSE)
  rows$sample_id[[3]] <- rows$sample_id[[2]]
  utils::write.csv(rows, asset$sample_manifests$uri, row.names = FALSE)
  expect_error(admission_test_handle(collection), "integrity")
})

test_that("secondary association files cannot escape the collection", {
  collection <- admission_test_collection(withr::local_tempdir())
  outside <- withr::local_tempfile(fileext = ".csv")
  file.copy(collection$manifest$assets$rt_plan$sample_manifests$uri, outside)
  collection$manifest$assets$rt_plan$sample_manifests$uri <- outside
  yaml::write_yaml(collection$manifest, collection$manifest_path)
  expect_error(admission_test_handle(collection), "outside its collection")
})

test_that("series reject symlinked files even inside their own root", {
  collection <- admission_test_collection(withr::local_tempdir())
  root <- collection$manifest$assets$dicom$uri
  path <- file.path(root, "group-1/slice-1.dcm")
  target <- file.path(root, "group-1/slice-2.dcm")
  unlink(path)
  expect_true(file.symlink(target, path))
  expect_error(admission_test_handle(collection), "integrity")
})

test_that("DataSHIELD clinical workflows bind exact assets to an authorized handle", {
  root <- withr::local_tempdir()
  collection <- admission_test_collection(root)
  withr::local_options(list(
    dsimaging.registry_path = file.path(root, "registry.yaml"),
    dsimaging.worker_context_dir = file.path(root, "contexts")))
  register_dataset("admission.test", collection$manifest_path, storage_backend("file"))
  session <- new.env(parent = globalenv())
  resource <- resourcer::newResource("images", "imaging+dataset://admission.test")
  assign("res", ImagingDatasetResourceClient$new(resource), session)
  assign("img", eval(quote(imagingInitDS("res")), session), session)
  submitted <- list()
  testthat::local_mocked_bindings(
    hpcUnitSelectionInternal = function(...) list(),
    hpcRuntimeIdentityInternal = function(...) strrep("a", 64),
    .package = "dsHPC")
  testthat::local_mocked_bindings(
    find_asset_by_hash = function(...) NULL,
    .content_hash_for_resource = function(...) NULL,
    .imaging_submit_job = function(job, ...) {
      submitted[[length(submitted) + 1L]] <<- job
      structure(list(capability = paste0("imgw_", strrep("a", 64))),
        class = "dsimaging_workflow_ref")
    }, .package = "dsImaging")
  cases <- list(
    rt_convert = list(rt_asset = "rt_struct", dicom_asset = "dicom"),
    rt_dose_plan = list(dose_asset = "rt_dose", plan_asset = "rt_plan"),
    wsi_tile = list(wsi_asset = "wsi"),
    dicom_convert = list(dicom_asset = "dicom"))
  for (runner in names(cases)) {
    request <- dsImaging:::.dsr_encode(list(handle = "img", runner = runner,
      config = cases[[runner]], output_asset = "derived"))
    result <- eval(substitute(imagingProcessAssetWorkflowDS(REQUEST),
                              list(REQUEST = request)), session)
    expect_s3_class(result, "dsimaging_workflow_ref")
    step <- submitted[[length(submitted)]]$steps[[2]]
    expect_identical(step$runner, runner)
    context <- yaml::read_yaml(step$config$worker_context)
    expect_setequal(names(context$collection_map$records_by_asset),
                     unlist(cases[[runner]], use.names = FALSE))
    for (records in context$collection_map$records_by_asset) {
      expect_identical(vapply(records, `[[`, character(1), "sample_id"),
                       paste0("scan", 1:3))
      expect_identical(vapply(records, `[[`, character(1), "privacy_id"),
                       paste0("patient", 1:3))
    }
    expect_identical(names(unclass(result)), "capability")
  }
  request <- dsImaging:::.dsr_encode(list(handle = "img", image_asset = "images",
    segmenter = list(provider = "monai", bundle_name = "test_bundle")))
  result <- eval(substitute(imagingProcessSegmentationCollectionDS(REQUEST),
                            list(REQUEST = request)), session)
  expect_s3_class(result, "dsimaging_workflow_ref")
  expect_identical(submitted[[length(submitted)]]$steps[[2]]$runner,
                   "monai_bundle_infer")
})

test_that("capabilities list newly admitted runners without worker paths", {
  capabilities <- imagingCapabilitiesDS()
  expect_true(all(c("rt_convert", "rt_dose_plan", "wsi_tile",
    "monai_bundle_infer", "dicom_convert") %in% capabilities$runners$runner))
  expect_setequal(names(capabilities$runners), c("runner", "present"))
})
