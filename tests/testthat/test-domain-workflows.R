test_that("radiomics domain method composes a labelled dsHPC job", {
  submitted <- NULL
  submitted_unit <- NULL
  unit_selection <- list(
    schema_version = 1L, source = "resource", unit_id = "unit_alpha",
    resource_pool_id = "unit_alpha", type = "external",
    config_seal = strrep("b", 64), config = list(),
    allowed_labels = list("dsImaging"), allowed_runners = list())
  testthat::local_mocked_bindings(
    hpcUnitSelectionInternal = function(...) unit_selection,
    .package = "dsHPC"
  )
  testthat::local_mocked_bindings(
    .authorized_imaging_dataset = function(handle_symbol, dataset_id = NULL,
                                           owner_env = NULL) {
      list(dataset_id = "lung1", handle = list(collection_snapshot = list(
        version = 1L, seal = strrep("a", 64), artifacts = list(),
        records = list())), manifest = list(),
        backend = NULL, privacy = list(), n_privacy_units = 3L)
    },
    .imaging_worker_manifest = function(...) list(
      dataset_id = "lung1", assets = list()),
    .imaging_worker_collection_map = function(...) NULL,
    .imaging_worker_asset_identity = function(...) strrep("f", 64),
    .register_imaging_worker_context = function(authorized,
                                                asset_names = NULL,
                                                collection_map = NULL,
                                                worker_manifest = NULL) {
      paste0("dsctx_", strrep("a", 64))
    },
    find_asset_by_hash = function(dataset_id, derivation_hash,
                                  collection_seal) NULL,
    .content_hash_for_resource = function(resource_name) {
      list(resource_name = resource_name, content_hash = NA_character_,
        updated_at = NA_character_, source = "unsupported")
    },
    .imaging_submit_job = function(job, ..., unit_selection = NULL) {
      submitted <<- job
      submitted_unit <<- unit_selection
      structure(list(capability = paste0("imgw_", strrep("1", 64))),
        class = "dsimaging_workflow_ref")
    },
    .package = "dsImaging"
  )

  req <- list(handle = "img", dataset_id = "lung1", image_asset = "images",
    mask_asset = "gtv", profile = list(name = "demo_ct_firstorder_v1",
      bin_width = 25, force2D = FALSE, feature_classes = "firstorder"))
  encoded <- dsImaging:::.dsr_encode(req)
  out <- imagingProcessRadiomicsAssetDS(encoded)

  expect_s3_class(out, "dsimaging_workflow_ref")
  expect_false("job_id" %in% names(out))
  expect_null(submitted$job_id)
  expect_equal(submitted$label, "dsImaging")
  expect_identical(submitted$visibility, "private")
  expect_equal(submitted$steps[[2]]$runner, "pyradiomics_extract")
  expect_equal(submitted$steps[[3]]$publish_kind, "imaging_radiomics_asset")
  expect_identical(submitted_unit, unit_selection)
  expect_match(submitted$steps[[2]]$config$dataset_id,
               "^dsctx_[0-9a-f]{64}$")
  expect_identical(anyDuplicated(names(submitted$steps[[2]]$config)), 0L)
})

test_that("exact asset map hashes are ordered and cover immutable identity", {
  record <- function(id, suffix) list(
    sample_id = id, source_kind = "mask_file",
    uri = paste0("s3://imaging/masks/", suffix, ".nii.gz"),
    relative_path = paste0(suffix, ".nii.gz"),
    content_hash = strrep(suffix, 64), size = 100L,
    n_files = 1L, version_id = paste0("version-", suffix))
  records <- list(record("case-B", "b"), record("case-A", "a"))
  map_hash <- function(value) dsImaging:::.imaging_worker_asset_map_hash(
    list(records_by_asset = list(masks = value)), "masks")
  baseline <- map_hash(records)

  expect_identical(baseline, map_hash(rev(records)))
  for (field in c("uri", "version_id", "content_hash", "size")) {
    changed <- records
    changed[[1L]][[field]] <- switch(field,
      uri = "s3://imaging/masks/replaced.nii.gz",
      version_id = "version-replaced",
      content_hash = strrep("c", 64),
      size = 101L)
    expect_false(identical(baseline, map_hash(changed)), info = field)
  }
})

test_that("changed source masks cannot reuse a direct radiomics asset", {
  mask_hash <- strrep("b", 64)
  observed_hashes <- character(0)
  submitted <- FALSE
  registered <- 0L
  collection_seal <- strrep("a", 64)
  exact_map <- function() {
    image_record <- list(
      sample_id = "case-A", source_kind = "single_file",
      uri = "s3://imaging/source/images/opaque.nii.gz",
      relative_path = "opaque.nii.gz", content_hash = strrep("d", 64),
      size = 200L, n_files = 1L, version_id = "image-version-1")
    mask_record <- list(
      sample_id = "case-A", source_kind = "mask_file",
      uri = "s3://imaging/source/masks/opaque.nii.gz",
      relative_path = "opaque.nii.gz", content_hash = mask_hash,
      size = 100L, n_files = 1L, version_id = "version-1")
    list(version = 1L, seal = collection_seal,
      asset_names = list("images", "masks"), records = list(image_record),
      records_by_asset = list(
        images = list(image_record), masks = list(mask_record)))
  }
  testthat::local_mocked_bindings(
    .authorized_imaging_dataset = function(...) list(
      dataset_id = "lung1", collection_seal = collection_seal,
      handle = list(collection_seal = collection_seal), manifest = list(),
      backend = NULL, privacy = list(), n_privacy_units = 3L),
    .imaging_worker_manifest = function(...) list(
      dataset_id = "lung1", assets = list()),
    .imaging_worker_collection_map = function(...) exact_map(),
    find_asset_by_hash = function(dataset_id, derivation_hash,
                                  collection_seal) {
      observed_hashes <<- c(observed_hashes, derivation_hash)
      if (length(observed_hashes) == 1L ||
          identical(derivation_hash, observed_hashes[[1L]])) {
        return("asset-existing")
      }
      NULL
    },
    .register_imaging_worker_context = function(
        authorized, asset_names = NULL, collection_map = NULL,
        worker_manifest = NULL) {
      registered <<- registered + 1L
      expect_identical(collection_map, exact_map())
      expect_identical(worker_manifest$dataset_id, "lung1")
      paste0("dsctx_", strrep("a", 64))
    },
    .content_hash_for_resource = function(...) NULL,
    .imaging_submit_job = function(job, ...) {
      submitted <<- TRUE
      structure(list(capability = paste0("imgw_", strrep("2", 64))),
        class = "dsimaging_workflow_ref")
    },
    .package = "dsImaging"
  )
  encoded <- dsImaging:::.dsr_encode(list(
    handle = "img", dataset_id = "lung1", image_asset = "images",
    mask_asset = "masks",
    profile = list(name = "demo_ct_firstorder_v1")))

  first <- imagingProcessRadiomicsAssetDS(encoded)
  mask_hash <- strrep("c", 64)
  second <- imagingProcessRadiomicsAssetDS(encoded)

  expect_s3_class(first, "dsimaging_workflow_ref")
  expect_s3_class(second, "dsimaging_workflow_ref")
  expect_length(observed_hashes, 2L)
  expect_false(identical(observed_hashes[[1L]], observed_hashes[[2L]]))
  expect_identical(registered, 1L)
  expect_true(submitted)

  mask_hash <- strrep("b", 64)
  observed_hashes <- character(0)
  registered <- 0L
  submitted <- FALSE
  chained <- dsImaging:::.dsr_encode(list(
    handle = "img", dataset_id = "lung1", image_asset = "images",
    segmenter = list(
      provider = "existing_mask_asset", mask_asset = "masks"),
    profile = list(name = "demo_ct_firstorder_v1")))
  imagingProcessSegmentAndExtractDS(chained)
  mask_hash <- strrep("c", 64)
  imagingProcessSegmentAndExtractDS(chained)

  expect_length(observed_hashes, 2L)
  expect_false(identical(observed_hashes[[1L]], observed_hashes[[2L]]))
  expect_identical(registered, 1L)
  expect_true(submitted)
})

test_that("derived alias promotion changes radiomics input identity", {
  collection_seal <- strrep("a", 64)
  resolved_asset_id <- paste0("asset_", strrep("1", 32))
  observed_hashes <- character(0)
  registered_manifest_ids <- character(0)
  image_record <- list(
    sample_id = "case-A", source_kind = "single_file",
    uri = "s3://imaging/source/images/opaque.nii.gz",
    relative_path = "opaque.nii.gz", content_hash = strrep("d", 64),
    size = 200L, n_files = 1L, version_id = "image-version-1")
  exact_map <- list(
    version = 1L, seal = collection_seal,
    asset_names = list("images"), records = list(image_record),
    records_by_asset = list(images = list(image_record)))
  testthat::local_mocked_bindings(
    .authorized_imaging_dataset = function(...) list(
      dataset_id = "lung1", collection_seal = collection_seal,
      handle = list(collection_seal = collection_seal), manifest = list(),
      backend = NULL, privacy = list(), n_privacy_units = 3L),
    .imaging_worker_manifest = function(authorized, asset_names) {
      assets <- list()
      for (asset_name in asset_names) {
        if (identical(asset_name, "images")) {
          assets[[asset_name]] <- list(uri = "s3://imaging/source/images")
        } else {
          assets[[asset_name]] <- list(
            uri = paste0("/derived/", resolved_asset_id),
            .dsimaging_asset_identity = list(
              asset_id = resolved_asset_id,
              derivation_hash = if (endsWith(resolved_asset_id, "1")) {
                strrep("b", 64)
              } else strrep("c", 64)))
        }
      }
      list(dataset_id = "lung1", assets = assets)
    },
    .imaging_worker_collection_map = function(...) exact_map,
    find_asset_by_hash = function(dataset_id, derivation_hash,
                                  collection_seal) {
      observed_hashes <<- c(observed_hashes, derivation_hash)
      if (length(observed_hashes) == 1L ||
          identical(derivation_hash, observed_hashes[[1L]])) {
        return(paste0("asset_", strrep("e", 32)))
      }
      NULL
    },
    .register_imaging_worker_context = function(
        authorized, asset_names = NULL, collection_map = NULL,
        worker_manifest = NULL) {
      registered_manifest_ids <<- c(registered_manifest_ids,
        worker_manifest$assets$latest_masks$.dsimaging_asset_identity$asset_id)
      paste0("dsctx_", strrep("a", 64))
    },
    .content_hash_for_resource = function(...) NULL,
    .imaging_submit_job = function(job, ...) {
      structure(list(capability = paste0("imgw_", strrep("2", 64))),
        class = "dsimaging_workflow_ref")
    },
    .package = "dsImaging"
  )

  exercise <- function(method, encoded) {
    observed_hashes <<- character(0)
    registered_manifest_ids <<- character(0)
    resolved_asset_id <<- paste0("asset_", strrep("1", 32))
    method(encoded)
    resolved_asset_id <<- paste0("asset_", strrep("2", 32))
    method(encoded)
    expect_length(observed_hashes, 2L)
    expect_false(identical(observed_hashes[[1L]], observed_hashes[[2L]]))
    expect_identical(
      registered_manifest_ids, paste0("asset_", strrep("2", 32)))
  }

  exercise(imagingProcessRadiomicsAssetDS, dsImaging:::.dsr_encode(list(
    handle = "img", dataset_id = "lung1", image_asset = "images",
    mask_asset = "latest_masks",
    profile = list(name = "demo_ct_firstorder_v1"))))
  exercise(imagingProcessSegmentAndExtractDS, dsImaging:::.dsr_encode(list(
    handle = "img", dataset_id = "lung1", image_asset = "images",
    segmenter = list(
      provider = "existing_mask_asset", mask_asset = "latest_masks"),
    profile = list(name = "demo_ct_firstorder_v1"))))
})

test_that("workflow configs reject analyst paths and orchestration controls", {
  testthat::local_mocked_bindings(
    .authorized_imaging_dataset = function(handle_symbol, dataset_id = NULL,
                                           owner_env = NULL) {
      list(dataset_id = "lung1", handle = list(), manifest = list(),
        backend = NULL, privacy = list(), n_privacy_units = 3L)
    },
    .package = "dsImaging"
  )

  base <- list(handle = "img", dataset_id = "lung1",
    runner = "image_preprocess", config = list(image_asset = "images"),
    output_asset = "preprocessed")
  for (field in c("image_root", "worker_context", "sample_id",
                  "generation_id", "dataset_id")) {
    request <- base
    request$config[[field]] <- "/private/other-collection"
    encoded <- dsImaging:::.dsr_encode(request)
    expect_error(
      imagingProcessAssetWorkflowDS(encoded),
      "unsupported field", fixed = TRUE)
  }

  request <- base
  request$config$image_asset <- "../../other-collection"
  encoded <- dsImaging:::.dsr_encode(request)
  expect_error(
    imagingProcessAssetWorkflowDS(encoded),
    "logical server-side name", fixed = TRUE)

  request <- base
  request$publish_kind <- "raw_file"
  encoded <- dsImaging:::.dsr_encode(request)
  expect_error(
    imagingProcessAssetWorkflowDS(encoded),
    "unsupported field", fixed = TRUE)
})

test_that("workflows without exact patient association fail closed", {
  for (runner in c("rt_convert", "rt_dose_plan", "wsi_tile")) {
    expect_error(
      dsImaging:::.imaging_runner_contract(runner),
      "exact admitted sample mapping", fixed = TRUE)
  }
  expect_error(
    dsImaging:::.imaging_segmenter_spec(list(
      provider = "monai", bundle_name = "example")),
    "exact per-sample contract", fixed = TRUE)
  expect_error(
    dsImaging:::.imaging_runner_config(
      "imaging_qc_visuals",
      list(image_asset = "images", max_images = 2L)),
    "unsupported field", fixed = TRUE)
  expect_identical(
    dsImaging:::.imaging_runner_contract("dicom_convert")$asset_type,
    "image_root")
  expect_identical(
    dsImaging:::.imaging_runner_config("dicom_convert", list())$converter,
    "simpleitk")
  expect_error(dsImaging:::.imaging_runner_config(
    "dicom_convert", list(converter = "dcm2niix")))
})

test_that("segmenter and profile specifications cannot select server paths", {
  expect_error(
    dsImaging:::.imaging_segmenter_spec(list(
      provider = "nnunetv2", model_name = "/srv/private/model", fold = "all")),
    "logical server-side name", fixed = TRUE)
  expect_error(
    dsImaging:::.imaging_segmenter_spec(list(
      provider = "totalsegmentator", task = "total",
      worker_context = "/srv/private/context.yaml")),
    "unsupported field", fixed = TRUE)
  expect_error(
    dsImaging:::.imaging_profile_spec(list(
      name = "demo_ct_firstorder_v1", settings_file = "/tmp/custom.yaml")),
    "unsupported field", fixed = TRUE)
  expect_error(
    dsImaging:::.imaging_profile_spec(list(name = "/tmp/custom.yaml")),
    "logical server-side name", fixed = TRUE)
})

test_that("generic workflows build publication fields server-side", {
  submitted <- NULL
  context_id <- paste0("dsctx_", strrep("b", 64))
  testthat::local_mocked_bindings(
    .authorized_imaging_dataset = function(handle_symbol, dataset_id = NULL,
                                           owner_env = NULL) {
      list(dataset_id = "lung1", handle = list(), manifest = list(),
        backend = NULL, privacy = list(), n_privacy_units = 3L)
    },
    .register_imaging_worker_context = function(authorized,
                                                asset_names = NULL) context_id,
    .content_hash_for_resource = function(resource_name) NULL,
    .imaging_submit_job = function(job, ...) {
      submitted <<- job
      structure(list(capability = paste0("imgw_", strrep("2", 64))),
        class = "dsimaging_workflow_ref")
    },
    .package = "dsImaging"
  )

  request <- list(handle = "img", dataset_id = "lung1",
    runner = "image_preprocess",
    config = list(image_asset = "images", operations = "normalize"),
    output_asset = "preprocessed", alias = "normalized")
  encoded <- dsImaging:::.dsr_encode(request)
  imagingProcessAssetWorkflowDS(encoded)

  run <- submitted$steps[[2]]
  publish <- submitted$steps[[3]]
  expect_identical(run$runner, "dsimaging_image_preprocess")
  expect_identical(publish$runner, "dsimaging_image_preprocess")
  expect_identical(run$config$dataset_id, context_id)
  expect_identical(run$config$worker_context,
    dsImaging:::.imaging_worker_context_file(context_id))
  expect_identical(anyDuplicated(names(run$config)), 0L)
  expect_identical(publish$asset_type, "image_root")
  expect_identical(publish$publish_kind, "imaging_asset")
  expect_identical(submitted$visibility, "private")
  expect_false(any(c("sample_id", "generation_id") %in% names(run$config)))
})

test_that("analytical workflow requests cannot select global visibility", {
  encoded <- dsImaging:::.dsr_encode(list(visibility = "global"))
  methods <- list(
    imagingProcessRadiomicsCollectionDS,
    imagingProcessRadiomicsAssetDS,
    imagingProcessSegmentationCollectionDS,
    imagingProcessSegmentAndExtractDS,
    imagingProcessAssetWorkflowDS,
    imagingProcessQcCollectionDS
  )

  for (method in methods) {
    expect_error(method(encoded), "unsupported field", fixed = TRUE)
  }
  expect_error(imagingConvertCollectionDS(encoded),
               "unsupported field", fixed = TRUE)
})

test_that("profile aggregate methods expose safe profile metadata", {
  profiles <- imagingListProfilesDS()
  expect_s3_class(profiles, "data.frame")
  if (nrow(profiles) > 0L) {
    profile_name <- profiles$profile_name[[1]]
    desc <- imagingDescribeProfileDS(profile_name)
    expect_true(is.list(desc))
    expect_true(nzchar(desc$name))
  }
})

test_that("domain submissions reject missing labels before dsHPC submit", {
  expect_error(
    dsImaging:::.imaging_submit_job(list(
      steps = list(list(type = "emit", plane = "session",
        output_name = "out", value = 1))
    )),
    "non-empty label"
  )
})

test_that("domain submissions pass the owning session to dsHPC", {
  session <- new.env(parent = globalenv())
  observed_session <- NULL
  context_id <- paste0("dsctx_", strrep("a", 64))
  testthat::local_mocked_bindings(
    hpcSubmitInternal = function(spec_encoded, session_env = NULL,
                                 unit_selection = NULL) {
      observed_session <<- session_env
      list(job_id = "job_test", .dshpc_capability = "opaque")
    },
    .package = "dsHPC")

  reference <- dsImaging:::.imaging_submit_job(
    list(label = "dsImaging", steps = list(list(
      type = "emit", plane = "session", output_name = "out", value = 1))),
    owner_env = session, dataset_id = "lung1",
    worker_context_id = context_id, output_asset_kind = "feature_table")

  expect_s3_class(reference, "dsimaging_workflow_ref")
  expect_true(identical(observed_session, session))
})

test_that("job workflows expose only coarse status and their exact private asset", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(dsimaging.asset_db = db_path))
  job_id <- "job_11111111-2222-4333-8444-555555555555"
  db <- dsImaging:::.asset_db_connect()
  asset_id <- dsImaging:::.asset_register(
    db, "lung1", "feature_table", tempfile("features-"),
    derivation_hash = "expected-hash", created_by_job = job_id,
    visibility = "private")
  dsImaging:::.asset_db_close(db)

  session <- new.env(parent = globalenv())
  ref <- dsImaging:::.register_imaging_workflow(list(
    action = "job", dataset_id = "lung1",
    worker_context_id = paste0("dsctx_", strrep("a", 64)),
    output_asset_kind = "feature_table", derivation_hash = "expected-hash",
    job = list(job_id = job_id, .dshpc_capability = "secret")), session)
  assign("workflow", ref, envir = session)
  assign("imagingWorkflowStatusDS", dsImaging::imagingWorkflowStatusDS,
         envir = session)
  cleaned <- character()
  testthat::local_mocked_bindings(
    hpcStatusInternal = function(job, required_label = NULL) {
      expect_identical(required_label, "dsImaging")
      list(state = "FINISHED", is_done = TRUE)
    },
    .package = "dsHPC"
  )
  testthat::local_mocked_bindings(
    .unregister_imaging_worker_context = function(context_id) {
      cleaned <<- c(cleaned, context_id)
      invisible(TRUE)
    },
    .package = "dsImaging"
  )

  status <- eval(quote(imagingWorkflowStatusDS("workflow")), envir = session)
  expect_named(status, c("state", "is_done", "asset_id"))
  expect_identical(status$asset_id, asset_id)
  expect_length(cleaned, 1L)
  # Resolve explicitly in the owning environment, as DataSHIELD does.
  stored <- eval(quote(dsImaging:::.resolve_imaging_workflow("workflow")),
                 envir = session)
  expect_identical(stored$action, "terminal")
  expect_null(stored$job)
  expect_null(stored$worker_context_id)
})

test_that("a successful job without its exact published asset fails closed", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(dsimaging.asset_db = db_path))
  session <- new.env(parent = globalenv())
  ref <- dsImaging:::.register_imaging_workflow(list(
    action = "job", dataset_id = "lung1",
    worker_context_id = paste0("dsctx_", strrep("d", 64)),
    output_asset_kind = "feature_table",
    job = list(
      job_id = "job_11111111-2222-4333-8444-666666666666",
      .dshpc_capability = "secret")), session)
  assign("workflow", ref, envir = session)
  assign("imagingWorkflowStatusDS", dsImaging::imagingWorkflowStatusDS,
         envir = session)
  testthat::local_mocked_bindings(
    hpcStatusInternal = function(job, required_label = NULL) {
      expect_identical(required_label, "dsImaging")
      list(state = "FINISHED", is_done = TRUE)
    },
    .package = "dsHPC"
  )
  testthat::local_mocked_bindings(
    .unregister_imaging_worker_context = function(context_id) invisible(TRUE),
    .package = "dsImaging"
  )

  expect_error(
    eval(quote(imagingWorkflowStatusDS("workflow")), envir = session),
    "Workflow result is unavailable", fixed = TRUE)
  status <- eval(quote(imagingWorkflowStatusDS("workflow")), envir = session)
  expect_identical(status, list(state = "FAILED", is_done = TRUE))
})

test_that("active workflows cannot be destroyed and terminal cleanup is exact", {
  session <- new.env(parent = globalenv())
  ref <- dsImaging:::.register_imaging_workflow(list(
    action = "job", dataset_id = "lung1",
    worker_context_id = paste0("dsctx_", strrep("b", 64)),
    output_asset_kind = "mask_root",
    job = list(
      job_id = "job_aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
      .dshpc_capability = "secret")), session)
  assign("workflow", ref, envir = session)
  assign("imagingWorkflowDestroyDS", dsImaging::imagingWorkflowDestroyDS,
         envir = session)
  testthat::local_mocked_bindings(
    hpcStatusInternal = function(job, required_label = NULL) {
      expect_identical(required_label, "dsImaging")
      list(state = "RUNNING", is_done = FALSE)
    },
    .package = "dsHPC"
  )
  expect_error(
    eval(quote(imagingWorkflowDestroyDS("workflow")), envir = session),
    "active imaging workflow")
  expect_true(exists("workflow", envir = session, inherits = FALSE))
  state <- dsImaging:::.imaging_session_state(session, create = FALSE)
  expect_true(exists(ref$capability,
    envir = state$workflows, inherits = FALSE))
})

test_that("workflow destroy returns an idempotent transport tombstone", {
  session <- new.env(parent = globalenv())
  reference <- dsImaging:::.register_imaging_workflow(
    list(action = "reuse_asset", dataset_id = "lung1"), session)
  assign("workflow", reference, envir = session)
  assign("imagingWorkflowDestroyDS", dsImaging::imagingWorkflowDestroyDS,
         envir = session)

  other_session <- new.env(parent = globalenv())
  other_reference <- dsImaging:::.register_imaging_workflow(
    list(action = "reuse_asset", dataset_id = "other"), other_session)
  assign("other_workflow", other_reference, envir = other_session)
  assign("workflow", reference, envir = other_session)
  assign("imagingWorkflowDestroyDS", dsImaging::imagingWorkflowDestroyDS,
         envir = other_session)
  expect_error(
    eval(quote(imagingWorkflowDestroyDS("workflow")), envir = other_session),
    "unavailable")

  tombstone <- eval(
    quote(imagingWorkflowDestroyDS("workflow")), envir = session)
  expect_identical(tombstone, reference)
  expect_false(exists("workflow", envir = session, inherits = FALSE))
  state <- dsImaging:::.imaging_session_state(session, create = FALSE)
  expect_identical(
    state$workflows[[reference$capability]],
    dsImaging:::.dsimaging_session_tombstone)

  assign("workflow", tombstone, envir = session)
  retry <- eval(quote(imagingWorkflowDestroyDS("workflow")), envir = session)
  expect_identical(retry, reference)
  expect_false(exists("workflow", envir = session, inherits = FALSE))
})

test_that("worker context cleanup removes only its exact registry entry", {
  root <- withr::local_tempdir()
  registry_path <- file.path(root, "registry.yaml")
  context_root <- file.path(root, "contexts")
  dir.create(context_root)
  context_id <- paste0("dsctx_", strrep("c", 64))
  manifest_path <- file.path(context_root, paste0(context_id,
    ".manifest.yaml"))
  context_path <- file.path(context_root, paste0(context_id,
    ".context.yaml"))
  yaml::write_yaml(list(schema_version = 1L), manifest_path)
  yaml::write_yaml(list(schema_version = 1L), context_path)
  withr::local_options(list(
    dsimaging.registry_backend = "file",
    dsimaging.registry_path = registry_path,
    dsimaging.worker_context_dir = context_root
  ))
  dsImaging:::register_dataset(
    context_id, manifest_path, dsImaging::storage_backend("file"))
  expect_true(dsImaging:::.unregister_imaging_worker_context(context_id))
  expect_false(file.exists(manifest_path))
  expect_false(file.exists(context_path))
  expect_false(context_id %in% names(dsImaging:::.load_registry()))
  expect_error(
    dsImaging:::.unregister_imaging_worker_context("../other"),
    "Invalid imaging worker context")
})
