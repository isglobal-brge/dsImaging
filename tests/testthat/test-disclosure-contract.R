privacy_manifest <- function(metadata_path, patient_ids,
                             dataset_id = "privacy.test",
                             label_col = "label") {
  data.frame(
    sample_id = paste0("scan", seq_along(patient_ids)),
    patient_id = patient_ids,
    label = rep(c("case", "control"), length.out = length(patient_ids)),
    stringsAsFactors = FALSE
  ) |>
    utils::write.csv(metadata_path, row.names = FALSE)

  list(
    schema_version = 1L,
    dataset_id = dataset_id,
    modality = "mri",
    metadata = list(
      uri = metadata_path,
      file = metadata_path,
      format = "csv",
      id_col = "sample_id",
      privacy_unit = "patient",
      privacy_unit_col = "patient_id",
      privacy_unit_canonicalization = "trim-utf8-v2",
      label_col = label_col
    ),
    assets = list()
  )
}

test_that("imaging admission counts distinct patients, not image rows", {
  metadata_path <- tempfile(fileext = ".csv")
  on.exit(unlink(metadata_path))
  manifest <- privacy_manifest(metadata_path, rep("only-patient", 6L))
  desc <- imaging_dataset_descriptor(manifest)

  withr::local_options(list(dsimaging.nfilter.subset = 3L))
  expect_error(
    dsImaging:::.make_imaging_handle(desc, "img"),
    "fewer than 3 privacy units"
  )

  manifest <- privacy_manifest(
    metadata_path,
    c("patient-a", "patient-a", "patient-b", "patient-b", "patient-c")
  )
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(manifest), "img")
  expect_equal(handle$n_privacy_units, 3L)
  expect_equal(handle$privacy$privacy_unit_col, "patient_id")
  expect_equal(handle$privacy_roster$sample_count, 5L)
  expect_equal(handle$privacy_roster$privacy_unit_count, 3L)
  expect_match(handle$privacy_roster$roster_hash, "^[0-9a-f]{64}$")
})

test_that("an admitted handle fails closed if the patient roster changes", {
  metadata_path <- tempfile(fileext = ".csv")
  on.exit(unlink(metadata_path))
  manifest <- privacy_manifest(
    metadata_path, c("patient-a", "patient-a", "patient-b", "patient-c"))
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(manifest), "img")
  session <- new.env(parent = globalenv())
  handle_ref <- dsImaging:::.register_imaging_handle(handle, session)
  assign("img", handle_ref, envir = session)

  metadata <- utils::read.csv(metadata_path, stringsAsFactors = FALSE)
  utils::write.csv(metadata[4:1, ], metadata_path, row.names = FALSE)
  expect_equal(eval(quote(imagingMetadataDS("img")$dataset_id),
                    envir = session), "privacy.test")
  consumer_handle <- eval(
    substitute(
      dsImaging:::.resolve_imaging_handle_for_consumer("img", CAPABILITY),
      list(CAPABILITY = handle_ref$capability)),
    envir = session)
  expect_equal(consumer_handle$privacy_roster$roster_hash,
               handle$privacy_roster$roster_hash)
  rebound_ref <- dsImaging:::.register_imaging_handle(handle, session)
  assign("img", rebound_ref, envir = session)
  expect_error(eval(
    substitute(
      dsImaging:::.resolve_imaging_handle_for_consumer("img", CAPABILITY),
      list(CAPABILITY = handle_ref$capability)),
    envir = session), "capability changed")

  metadata$patient_id[c(2, 3)] <- metadata$patient_id[c(3, 2)]
  utils::write.csv(metadata, metadata_path, row.names = FALSE)
  expect_error(eval(quote(imagingMetadataDS("img")), envir = session),
               "admitted privacy roster")
  expect_error(eval(
    quote(dsImaging:::.resolve_imaging_handle_for_consumer("img")),
    envir = session), "admitted privacy roster")
})

test_that("legacy manifests without a patient privacy contract fail closed", {
  metadata_path <- tempfile(fileext = ".csv")
  on.exit(unlink(metadata_path))
  utils::write.csv(data.frame(sample_id = letters[1:4]), metadata_path,
                   row.names = FALSE)
  manifest <- list(
    schema_version = 1L,
    dataset_id = "legacy.test",
    metadata = list(uri = metadata_path, file = metadata_path, format = "csv"),
    assets = list()
  )

  expect_error(imaging_dataset_descriptor(manifest), "privacy contract")
})

test_that("sample, patient, and label roles must use distinct columns", {
  metadata_path <- tempfile(fileext = ".csv")
  on.exit(unlink(metadata_path))
  manifest <- privacy_manifest(
    metadata_path, c("patient-a", "patient-b", "patient-c"))
  manifest$metadata$privacy_unit_col <- "sample_id"
  expect_error(imaging_dataset_descriptor(manifest), "non-structural")

  manifest$metadata$privacy_unit_col <- "source_kind"
  expect_error(imaging_dataset_descriptor(manifest), "non-structural")

  manifest$metadata$privacy_unit_col <- "n_files"
  expect_error(imaging_dataset_descriptor(manifest), "non-structural")

  manifest <- privacy_manifest(
    metadata_path, c("patient-a", "patient-b", "patient-c"),
    label_col = "patient_id")
  expect_error(imaging_dataset_descriptor(manifest), "non-structural")
})

test_that("exact roster validation checks the sample-to-patient mapping", {
  roster <- dsImaging:::.new_imaging_privacy_roster(
    c("scan-a", "scan-b", "scan-c"),
    c("patient-a", "patient-a", "patient-b"))

  expect_invisible(dsImaging:::.assert_exact_imaging_roster(
    c("scan-c", "scan-a", "scan-b"), roster,
    privacy_ids = c("patient-b", "patient-a", "patient-a")))
  expect_error(dsImaging:::.assert_exact_imaging_roster(
    c("scan-a", "scan-b", "scan-c"), roster,
    privacy_ids = c("patient-a", "patient-b", "patient-a")),
  "complete admitted collection")
})

test_that("privacy metadata rejects reserved missing-value tokens", {
  metadata_path <- tempfile(fileext = ".csv")
  on.exit(unlink(metadata_path))
  for (token in c("na", "NaN", "NULL", "<NA>", "NaT")) {
    manifest <- privacy_manifest(
      metadata_path, c("patient-a", "patient-b", token))
    expect_error(
      dsImaging:::.make_imaging_handle(
        imaging_dataset_descriptor(manifest), "img"),
      "privacy contract")
  }
})

test_that("sample identifiers must already be canonical", {
  metadata_path <- tempfile(fileext = ".csv")
  on.exit(unlink(metadata_path))
  manifest <- privacy_manifest(
    metadata_path, c("patient-a", "patient-b", "patient-c"))
  metadata <- utils::read.csv(metadata_path, stringsAsFactors = FALSE)
  metadata$sample_id[[1L]] <- paste0(" ", metadata$sample_id[[1L]], " ")
  utils::write.csv(metadata, metadata_path, row.names = FALSE)
  expect_error(
    dsImaging:::.make_imaging_handle(
      imaging_dataset_descriptor(manifest), "img"),
    "privacy contract")
})

test_that("handle lookup is session-local and never falls back by name", {
  metadata_path <- tempfile(fileext = ".csv")
  on.exit(unlink(metadata_path))
  manifest <- privacy_manifest(
    metadata_path, c("patient-a", "patient-b", "patient-c"))
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(manifest), "img")

  assign("imaging_img", handle, envir = dsImaging:::.dsimaging_env)
  on.exit(rm(list = "imaging_img", envir = dsImaging:::.dsimaging_env), add = TRUE)

  expect_error(dsImaging:::.getImagingHandle("missing"),
               "No imaging handle")
  expect_error(imagingGetManifestDS("missing"),
               "not available through DataSHIELD")
  expect_error(imagingGetBackendDS("missing"),
               "not available through DataSHIELD")
})

test_that("label distributions use only the declared label and patient units", {
  metadata_path <- tempfile(fileext = ".csv")
  on.exit(unlink(metadata_path))
  manifest <- privacy_manifest(
    metadata_path,
    rep(paste0("patient-", letters[1:6]), each = 2L))
  df <- utils::read.csv(metadata_path, stringsAsFactors = FALSE)
  df$label <- rep(c("case", "case", "control", "control", "control",
                    "control"), each = 2L)
  utils::write.csv(df, metadata_path, row.names = FALSE)
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(manifest), "img")

  env <- new.env(parent = globalenv())
  assign("img", dsImaging:::.register_imaging_handle(handle, env), envir = env)
  withr::local_options(list(
    dsimaging.nfilter.subset = 3L,
    dsimaging.nfilter.tab = 3L,
    nfilter.levels.density = 0.5,
    dsimaging.privacy_profile = "clinical_default"
  ))

  out <- eval(quote(imagingLabelDistDS("img")), envir = env)
  expect_named(out, c("label", "n"))
  expect_equal(nrow(out), 0L)
  expect_false("suppressed_classes" %in% names(attributes(out)))
  expect_error(
    eval(quote(imagingLabelDistDS("img", "patient_id")), envir = env),
    "declared label"
  )
})

test_that("label distributions require an administrator-declared public vocabulary", {
  metadata_path <- tempfile(fileext = ".csv")
  on.exit(unlink(metadata_path))
  manifest <- privacy_manifest(
    metadata_path, paste0("patient-", letters[1:6]))
  manifest$metadata$label_levels <- c("case", "control")
  metadata <- utils::read.csv(metadata_path, stringsAsFactors = FALSE)
  metadata$label <- rep(c("case", "control"), each = 3L)
  utils::write.csv(metadata, metadata_path, row.names = FALSE)
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(manifest), "img")
  env <- new.env(parent = globalenv())
  assign("img", dsImaging:::.register_imaging_handle(handle, env), envir = env)
  withr::local_options(list(
    dsimaging.nfilter.subset = 3L,
    dsimaging.nfilter.tab = 3L,
    nfilter.levels.density = 0.5,
    dsimaging.privacy_profile = "sandbox_open"
  ))

  out <- eval(quote(imagingLabelDistDS("img")), envir = env)
  expect_equal(out$label, c("case", "control"))
  expect_equal(out$n, c(3L, 3L))

  metadata$label[[1L]] <- "/srv/private/cohort-A"
  utils::write.csv(metadata, metadata_path, row.names = FALSE)
  expect_error(
    eval(quote(imagingLabelDistDS("img")), envir = env),
    "privacy contract")
})

test_that("unobserved approved label levels suppress the full distribution", {
  metadata_path <- tempfile(fileext = ".csv")
  on.exit(unlink(metadata_path))
  manifest <- privacy_manifest(
    metadata_path, paste0("patient-", letters[1:6]))
  manifest$metadata$label_levels <- c("case", "control", "rare")
  metadata <- utils::read.csv(metadata_path, stringsAsFactors = FALSE)
  metadata$label <- rep(c("case", "control"), each = 3L)
  utils::write.csv(metadata, metadata_path, row.names = FALSE)
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(manifest), "img")
  env <- new.env(parent = globalenv())
  assign("img", dsImaging:::.register_imaging_handle(handle, env), envir = env)
  withr::local_options(list(
    dsimaging.nfilter.subset = 3L,
    dsimaging.nfilter.tab = 3L,
    nfilter.levels.density = 0.5,
    dsimaging.privacy_profile = "sandbox_open"
  ))

  out <- eval(quote(imagingLabelDistDS("img")), envir = env)
  expect_named(out, c("label", "n"))
  expect_equal(nrow(out), 0L)
})

test_that("public label vocabularies cannot contain private identifiers", {
  metadata_path <- tempfile(fileext = ".csv")
  manifest <- privacy_manifest(
    metadata_path, paste0("patient-", letters[1:6]))
  metadata <- utils::read.csv(metadata_path, stringsAsFactors = FALSE)
  metadata$label <- rep(c("case", "control"), each = 3L)
  utils::write.csv(metadata, metadata_path, row.names = FALSE)

  for (private_value in c(metadata$sample_id[[1L]],
                          metadata$patient_id[[1L]])) {
    unsafe <- manifest
    unsafe$metadata$label_levels <- c("case", "control", private_value)
    expect_error(
      dsImaging:::.make_imaging_handle(
        imaging_dataset_descriptor(unsafe), "img"),
      "privacy contract")
  }
})

test_that("registered aggregates exclude row-level and hash oracles", {
  description <- packageDescription("dsImaging")
  aggregate <- trimws(strsplit(
    description$AggregateMethods, ",", fixed = TRUE)[[1]])
  assign <- trimws(strsplit(
    description$AssignMethods, ",", fixed = TRUE)[[1]])
  registered <- unique(c(aggregate, assign))
  forbidden <- c(
    "contentHashDS", "imagingGetBackendDS", "imagingGetManifestDS",
    "imagingLabelsDS", "imagingMasksDS", "imagingListDatasetsDS",
    "imagingAssetDetailDS", "imagingAliasesDS", "imagingLineageDS",
    "imagingDeduplicateDS",
    "imagingRadiomicsScanCollectionDS", "imagingRadiomicsSubmitBatchDS",
    "imagingRadiomicsCollectionStatusDS",
    "imagingRadiomicsRecoverCollectionDS",
    "imagingRadiomicsPublishCollectionDS", "imagingListGenerationsDS",
    "imagingSegmentationValidateMasksDS"
  )
  expect_length(intersect(aggregate, forbidden), 0L)
  expect_false("imagingSegmentationGetMaskPaths" %in% registered)
  exported_ds <- grep("DS$", getNamespaceExports("dsImaging"), value = TRUE)
  expect_setequal(setdiff(exported_ds, registered), forbidden)
})

test_that("stale allowlists cannot execute legacy low-level DS methods", {
  calls <- list(
    quote(contentHashDS("dataset")),
    quote(imagingGetBackendDS("img")),
    quote(imagingGetManifestDS("img")),
    quote(imagingLabelsDS("img")),
    quote(imagingMasksDS("img")),
    quote(imagingListDatasetsDS()),
    quote(imagingAssetDetailDS("img", "asset")),
    quote(imagingAliasesDS("img")),
    quote(imagingLineageDS("asset")),
    quote(imagingDeduplicateDS("dataset", "hash")),
    quote(imagingListGenerationsDS()),
    quote(imagingRadiomicsScanCollectionDS("dataset", "segmenter",
      "profile", "private")),
    quote(imagingRadiomicsSubmitBatchDS("generation", "samples",
      "dataset", "fingerprints")),
    quote(imagingRadiomicsCollectionStatusDS("generation")),
    quote(imagingRadiomicsRecoverCollectionDS("generation")),
    quote(imagingRadiomicsPublishCollectionDS("generation", "dataset")),
    quote(imagingSegmentationValidateMasksDS("generation", "dataset")),
    quote(imagingSegmentationGetMaskPaths("generation", "dataset"))
  )
  for (call in calls) {
    expect_error(eval(call), "not available through DataSHIELD")
  }
})

test_that("portable generation context rejects raw credentials", {
  withr::local_options(list(dsimaging.credentials = list(
    `resource-ref` = list(access_key = "ADMIN_ACCESS",
                          secret_key = "ADMIN_SECRET"))))
  backend <- structure(list(
    type = "s3",
    config = list(
      endpoint = "https://object.example",
      credentials_ref = "resource-ref",
      access_key = "PRIVATE_ACCESS",
      secret_key = "PRIVATE_SECRET",
      resource = list(identity = "PRIVATE_ID", secret = "PRIVATE_RESOURCE_SECRET")
    )
  ), class = "dsimaging_backend")

  expect_error(dsImaging:::.portable_backend_context(backend),
    "Worker storage credentials are not configured", fixed = TRUE)
  backend$config[c("access_key", "secret_key", "resource")] <- NULL
  context <- dsImaging:::.portable_backend_context(backend)
  text <- jsonlite::toJSON(context, auto_unbox = TRUE)
  expect_false(grepl("PRIVATE_", text, fixed = TRUE))
  expect_equal(context$config$credentials_ref, "resource-ref")
})

test_that("session resources stay private and are never persisted implicitly", {
  root <- withr::local_tempdir()
  credential_path <- file.path(root, "credentials.yaml")
  resource <- structure(list(
    name = "images", url = "imaging+dataset://privacy.test",
    identity = "PRIVATE_ACCESS", secret = "PRIVATE_SECRET"
  ), class = c("resource", "list"))
  backend <- structure(list(type = "s3", config = list(
    resource = resource, endpoint = "https://object.example")),
    class = "dsimaging_backend")
  withr::local_options(list(dsimaging.credentials_path = credential_path))

  session_backend <- dsImaging:::.sanitize_imaging_backend(
    backend, "privacy.test")
  expect_identical(session_backend$config$resource, resource)
  expect_false(file.exists(credential_path))
  expect_error(
    dsImaging:::.portable_backend_context(session_backend),
    "Worker storage credentials are not configured", fixed = TRUE)
  expect_false(file.exists(credential_path))
})

test_that("worker contexts use only administrator-managed credential refs", {
  resource <- structure(list(
    name = "images", url = "imaging+dataset://privacy.test",
    identity = "PRIVATE_ACCESS", secret = "PRIVATE_SECRET"
  ), class = c("resource", "list"))
  backend <- structure(list(type = "s3", config = list(
    resource = resource, endpoint = "https://object.example")),
    class = "dsimaging_backend")
  withr::local_options(list(
    dsimaging.worker_credentials_ref = "managed-images",
    dsimaging.credentials = list(`managed-images` = list(
      access_key = "ADMIN_ACCESS", secret_key = "ADMIN_SECRET"))
  ))

  context <- dsImaging:::.portable_backend_context(backend)
  serialized <- jsonlite::toJSON(context, auto_unbox = TRUE)
  expect_identical(context$config$credentials_ref, "managed-images")
  expect_false(grepl("PRIVATE_|ADMIN_", serialized))
  expect_null(context$config$resource)
})

test_that("dataset registry rejects session and inline credentials", {
  root <- withr::local_tempdir()
  registry_path <- file.path(root, "registry.yaml")
  withr::local_options(list(dsimaging.registry_path = registry_path))
  resource_backend <- structure(list(type = "s3", config = list(
    resource = list(identity = "access", secret = "secret"),
    endpoint = "https://object.example")), class = "dsimaging_backend")
  inline_backend <- structure(list(type = "s3", config = list(
    access_key = "access", secret_key = "secret",
    endpoint = "https://object.example")), class = "dsimaging_backend")

  expect_error(dsImaging:::register_dataset(
    "context.one", file.path(root, "manifest.yaml"), resource_backend),
    "administrator-managed", fixed = TRUE)
  expect_error(dsImaging:::register_dataset(
    "context.two", file.path(root, "manifest.yaml"), inline_backend),
    "administrator-managed", fixed = TRUE)
  expect_false(file.exists(registry_path))
})

test_that("handle and workflow capabilities are bound to one session", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(dsimaging.asset_db = db_path))
  metadata_path <- tempfile(fileext = ".csv")
  on.exit(unlink(metadata_path), add = TRUE)
  manifest <- privacy_manifest(
    metadata_path, c("patient-a", "patient-b", "patient-c"))
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(manifest), "img")

  session_a <- new.env(parent = globalenv())
  session_b <- new.env(parent = globalenv())
  handle_ref <- dsImaging:::.register_imaging_handle(handle, session_a)
  assign("img", handle_ref, envir = session_a)
  assign("img", handle_ref, envir = session_b)
  expect_equal(
    eval(quote(imagingMetadataDS("img")$dataset_id), envir = session_a),
    "privacy.test")
  expect_error(
    eval(quote(imagingMetadataDS("img")), envir = session_b),
    "cross-session")

  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  reused_asset <- dsImaging:::.asset_register(
    db, "privacy.test", "feature_table", "/features", visibility = "global",
    collection_seal = handle$collection_seal)
  workflow_ref <- dsImaging:::.register_imaging_workflow(
    list(action = "reuse_asset", asset_id = reused_asset,
         dataset_id = "privacy.test"), session_a)
  assign("workflow", workflow_ref, envir = session_a)
  assign("workflow", workflow_ref, envir = session_b)
  status <- eval(quote(imagingCollectionStatusDS("workflow")),
                 envir = session_a)
  expect_named(status, c("state", "is_done", "asset_id"))
  expect_error(
    eval(quote(imagingCollectionStatusDS("workflow")), envir = session_b),
    "cross-session")
  expect_identical(names(unclass(workflow_ref)), "capability")
})

test_that("session resolvers never fall back to process-global references", {
  metadata_path <- tempfile(fileext = ".csv")
  on.exit(unlink(metadata_path), add = TRUE)
  manifest <- privacy_manifest(
    metadata_path, c("patient-a", "patient-b", "patient-c"))
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(manifest), "img")
  handle_symbol <- paste0("global_img_", gsub("-", "", uuid::UUIDgenerate()))
  workflow_symbol <- paste0(
    "global_workflow_", gsub("-", "", uuid::UUIDgenerate()))
  handle_ref <- dsImaging:::.register_imaging_handle(handle, globalenv())
  workflow_ref <- dsImaging:::.register_imaging_workflow(
    list(action = "source_asset", dataset_id = "privacy.test"), globalenv())
  assign(handle_symbol, handle_ref, envir = globalenv())
  assign(workflow_symbol, workflow_ref, envir = globalenv())
  on.exit({
    rm(list = c(handle_symbol, workflow_symbol), envir = globalenv())
    state <- dsImaging:::.imaging_session_state(globalenv(), create = FALSE)
    rm(list = handle_ref$capability,
       envir = state$handles)
    rm(list = workflow_ref$capability,
       envir = state$workflows)
  }, add = TRUE)

  other_session <- new.env(parent = globalenv())
  expect_error(eval(
    substitute(imagingMetadataDS(SYMBOL), list(SYMBOL = handle_symbol)),
    envir = other_session), "No imaging handle")
  expect_error(eval(
    substitute(imagingWorkflowStatusDS(SYMBOL),
               list(SYMBOL = workflow_symbol)),
    envir = other_session), "Unknown or unavailable")
})

test_that("imagingDestroyDS removes only a session-owned handle capability", {
  metadata_path <- tempfile(fileext = ".csv")
  on.exit(unlink(metadata_path), add = TRUE)
  manifest <- privacy_manifest(
    metadata_path, c("patient-a", "patient-b", "patient-c"))
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(manifest), "img")

  session_a <- new.env(parent = globalenv())
  session_b <- new.env(parent = globalenv())
  reference <- dsImaging:::.register_imaging_handle(handle, session_a)
  other_reference <- dsImaging:::.register_imaging_handle(handle, session_b)
  assign("img", reference, envir = session_a)
  assign("img", reference, envir = session_b)
  assign("other_img", other_reference, envir = session_b)
  assign("imagingDestroyDS", dsImaging::imagingDestroyDS, envir = session_a)
  assign("imagingDestroyDS", dsImaging::imagingDestroyDS, envir = session_b)

  expect_error(eval(quote(imagingDestroyDS("img")), envir = session_b),
               "unavailable")
  state_a <- dsImaging:::.imaging_session_state(session_a, create = FALSE)
  expect_true(exists(reference$capability,
                     envir = state_a$handles,
                     inherits = FALSE))
  expect_true(exists("img", envir = session_a, inherits = FALSE))

  destroyed <- eval(quote(imagingDestroyDS("img")), envir = session_a)
  expect_s3_class(destroyed, "dsimaging_handle_ref")
  expect_false(exists("img", envir = session_a, inherits = FALSE))
  expect_identical(
    state_a$handles[[reference$capability]],
    dsImaging:::.dsimaging_session_tombstone)

  assign("retry", destroyed, envir = session_a)
  expect_s3_class(
    eval(quote(imagingDestroyDS("retry")), envir = session_a),
    "dsimaging_handle_ref")
  expect_false(exists("retry", envir = session_a, inherits = FALSE))

  # An unknown but structurally valid reference is not an owner-local retry.
  forged <- structure(list(capability = paste0("imgh_", strrep("f", 64))),
                      class = "dsimaging_handle_ref")
  assign("forged", forged, envir = session_a)
  expect_error(
    eval(quote(imagingDestroyDS("forged")), envir = session_a),
    "unavailable")
  expect_true(exists("forged", envir = session_a, inherits = FALSE))

  assign("malformed", list(capability = "not-a-capability"),
         envir = session_a)
  expect_error(eval(quote(imagingDestroyDS("malformed")), envir = session_a),
               "unavailable")
  expect_true(exists("malformed", envir = session_a, inherits = FALSE))
})

test_that("collection status exposes no roster or exact progress counters", {
  session <- new.env(parent = globalenv())
  workflow_ref <- dsImaging:::.register_imaging_workflow(list(
    action = "running", generation_id = paste0("gen_", strrep("b", 32)),
    dataset_id = "privacy.test"), session)
  assign("workflow", workflow_ref, envir = session)
  testthat::local_mocked_bindings(
    .imaging_radiomics_collection_status = function(generation_id) list(
      state = "RUNNING", is_done = FALSE, total = 17L, completed = 4L,
      failed = 1L, pending_ids = c("patient-a", "patient-b")),
    get_generation = function(generation_id) NULL,
    .package = "dsImaging"
  )

  status <- eval(quote(imagingCollectionStatusDS("workflow")), envir = session)
  expect_named(status, c("state", "is_done"))
  expect_false(any(c("total", "completed", "failed", "pending_ids",
                     "generation_id") %in% names(status)))
})

test_that("registered metadata methods keep node storage diagnostics private", {
  capture_conditions <- function(expr) {
    warnings <- character(0)
    value <- withCallingHandlers(
      tryCatch(force(expr), error = identity),
      warning = function(w) {
        warnings <<- c(warnings, conditionMessage(w))
        invokeRestart("muffleWarning")
      })
    list(value = value, warnings = warnings)
  }

  root <- withr::local_tempdir(pattern = "private-node-storage-")
  metadata_path <- file.path(root, "patients.csv")
  manifest <- privacy_manifest(
    metadata_path, c("patient-a", "patient-b", "patient-c"))
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(manifest), "img")
  session <- new.env(parent = globalenv())
  assign("img", dsImaging:::.register_imaging_handle(handle, session),
         envir = session)
  unlink(metadata_path)
  dir.create(metadata_path)

  metadata_failure <- capture_conditions(
    eval(quote(imagingMetadataDS("img")), envir = session))
  expect_s3_class(metadata_failure$value, "error")
  expect_identical(conditionMessage(metadata_failure$value),
                   "Imaging metadata is unavailable.")
  expect_length(metadata_failure$warnings, 0L)

  profile_dir <- file.path(root, "profiles")
  dir.create(profile_dir)
  writeLines("setting: [", file.path(profile_dir, "broken.yaml"))
  withr::local_options(list(dsimaging.analysis.home = root))
  profile_failure <- capture_conditions(imagingDescribeProfileDS("broken"))
  expect_s3_class(profile_failure$value, "error")
  expect_identical(conditionMessage(profile_failure$value),
                   "Profile description is unavailable.")
  expect_length(profile_failure$warnings, 0L)

  models_dir <- file.path(root, "models")
  dir.create(file.path(models_dir, "provider", "task", ".installed"),
             recursive = TRUE)
  withr::local_options(list(dsimaging.analysis.models_dir = models_dir))
  models <- capture_conditions(imagingListModelsDS())
  expect_s3_class(models$value, "data.frame")
  expect_named(models$value, c("provider", "task", "ready"))
  expect_equal(nrow(models$value), 0L)
  expect_length(models$warnings, 0L)

  public_diagnostics <- c(
    metadata_failure$warnings,
    conditionMessage(metadata_failure$value),
    profile_failure$warnings,
    conditionMessage(profile_failure$value),
    models$warnings)
  expect_false(any(grepl(root, public_diagnostics, fixed = TRUE)))
})

test_that("handle refresh never warns with the private manifest path", {
  root <- withr::local_tempdir(pattern = "private-manifest-refresh-")
  collection <- write_test_imaging_collection(
    root, "private.refresh",
    data.frame(
      sample_id = paste0("scan", 1:3),
      patient_id = paste0("patient", 1:3),
      stringsAsFactors = FALSE))
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(collection$manifest), "img",
    backend = storage_backend("file"),
    manifest_uri = collection$manifest_path,
    require_snapshot = TRUE)
  session <- new.env(parent = globalenv())
  assign("img", dsImaging:::.register_imaging_handle(handle, session),
         envir = session)
  unlink(collection$manifest_path)
  dir.create(collection$manifest_path)

  warnings <- character()
  error <- tryCatch(
    withCallingHandlers(
      eval(quote(imagingMetadataDS("img")), envir = session),
      warning = function(w) {
        warnings <<- c(warnings, conditionMessage(w))
        invokeRestart("muffleWarning")
      }),
    error = conditionMessage)
  expect_identical(
    error,
    "Imaging dataset no longer matches its admitted collection snapshot.")
  expect_length(warnings, 0L)
  expect_false(any(grepl(root, c(error, warnings), fixed = TRUE)))
})

test_that("registered metadata projects only canonical public identifiers", {
  root <- withr::local_tempdir(pattern = "private-public-fields-")
  metadata_path <- file.path(root, "patients.csv")
  manifest <- privacy_manifest(
    metadata_path, c("patient-a", "patient-b", "patient-c"))
  manifest$title <- "s3://private-bucket/cohort-title"
  manifest$modality <- "/srv/private/modality"
  manifest$task_types <- c("classification", "s3://private/task")
  manifest$compatible_templates <- c("resnet-18", "file:private-template")
  manifest$assets <- list(
    safe_asset = list(kind = "/srv/private/kind"),
    `s3://private-bucket/asset` = list(kind = "mask_root"))
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(manifest), "img")
  session <- new.env(parent = globalenv())
  assign("img", dsImaging:::.register_imaging_handle(handle, session),
         envir = session)

  metadata <- eval(quote(imagingMetadataDS("img")), envir = session)
  expect_identical(metadata$title, "unknown")
  expect_identical(metadata$modality, "unknown")
  expect_identical(metadata$task_types, "classification")
  expect_identical(metadata$templates, "resnet-18")
  assets <- eval(quote(imagingAssetsDS("img")), envir = session)
  expect_identical(assets$name, "safe_asset")
  expect_identical(assets$type, "unknown")

  testthat::local_mocked_bindings(
    validate_imaging_dataset = function(...) list(
      valid = FALSE, errors = "private", warnings = character(0),
      asset_status = list(
        safe_asset = list(type = "s3://private/type", valid = FALSE),
        `/srv/private/asset` = list(type = "image_root", valid = TRUE))),
    .package = "dsImaging")
  validation <- eval(quote(imagingValidateDS("img")), envir = session)
  expect_identical(validation$asset_status$name, "safe_asset")
  expect_identical(validation$asset_status$type, "unknown")

  public <- jsonlite::toJSON(list(
    metadata = metadata, assets = assets, validation = validation))
  expect_false(grepl("/srv/private", public, fixed = TRUE))
  expect_false(grepl("s3://", public, fixed = TRUE))
  expect_false(grepl("file:private", public, fixed = TRUE))
})

test_that("partial collection publication is rejected at the domain boundary", {
  request <- dsImaging:::.dsr_encode(list(
    collection_symbol = "workflow", allow_partial = TRUE))
  expect_error(
    imagingPublishRadiomicsAssetDS(request),
    "Partial collection publication is not permitted",
    fixed = TRUE
  )
})

test_that("worker contexts never promote the Opal dataset id", {
  root <- withr::local_tempdir()
  metadata_path <- file.path(root, "samples.csv")
  manifest <- privacy_manifest(
    metadata_path, c("patient-a", "patient-b", "patient-c"),
    dataset_id = "opal.private.dataset")
  manifest$assets <- list(
    images = list(type = "image_root", uri = file.path(root, "images")),
    unrequested = list(type = "mask_root", uri = file.path(root, "secret")))
  backend <- storage_backend("file")
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(manifest), "img", backend = backend,
    manifest_uri = file.path(root, "source-manifest.yaml"))
  registry_path <- file.path(root, "registry.yaml")
  withr::local_options(list(
    dshpc.home = root,
    dsimaging.registry_path = registry_path,
    dsimaging.worker_context_dir = file.path(root, "contexts")
  ))

  expect_false(file.exists(registry_path))
  context_id <- dsImaging:::.register_imaging_worker_context(list(
    handle = handle, dataset_id = handle$dataset_id,
    manifest = handle$manifest, backend = handle$backend,
    privacy = handle$privacy, n_privacy_units = handle$n_privacy_units,
    privacy_roster = handle$privacy_roster), asset_names = "images")
  registry <- yaml::yaml.load_file(registry_path)
  registry$schema_version <- NULL
  expect_match(context_id, "^dsctx_[0-9a-f]{64}$")
  expect_false("opal.private.dataset" %in% names(registry))
  expect_identical(names(registry), context_id)
  expect_null(registry[[context_id]]$dataset_id)
  expect_true(file.exists(registry[[context_id]]$manifest))
  context_manifest <- yaml::yaml.load_file(registry[[context_id]]$manifest)
  expect_identical(names(context_manifest$assets), "images")
  expect_equal(context_manifest$.dsimaging_privacy_roster$roster_hash,
               handle$privacy_roster$roster_hash)
})

test_that("publish locks block dataset admission", {
  root <- withr::local_tempdir()
  metadata_path <- file.path(root, "samples.csv")
  manifest <- privacy_manifest(
    metadata_path, c("patient-a", "patient-b", "patient-c"))
  manifest_path <- file.path(root, "manifest.yaml")
  yaml::write_yaml(manifest, manifest_path)
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(manifest), "img",
    backend = storage_backend("file"), manifest_uri = manifest_path)
  session <- new.env(parent = globalenv())
  assign("img", dsImaging:::.register_imaging_handle(handle, session),
         envir = session)
  file.create(file.path(root, ".publish-lock"))

  expect_error(
    dsImaging:::.make_imaging_handle(
      imaging_dataset_descriptor(manifest), "img",
      backend = storage_backend("file"), manifest_uri = manifest_path),
    "being updated")
  expect_error(eval(
    quote(dsImaging:::.resolve_imaging_handle_for_consumer("img")),
    envir = session), "being updated")
})

test_that("capability metadata omits server paths and scheduler diagnostics", {
  testthat::local_mocked_bindings(
    list_installed_models = function() data.frame(
      provider = c("example", "/private/provider"),
      task = c("task", "s3://private/task"),
      path = c("/private/model", "/private/model-2"),
      installed_at = c("2026-01-01", "2026-01-02"),
      stringsAsFactors = FALSE),
    list_imaging_radiomics_profiles = function() c(
      "safe_profile", "s3://private/profile", "file:private-profile",
      "private profile"),
    .imaging_runner_health = function() data.frame(
      runner = c("image_embeddings", "rt_dose_plan"),
      path = c("/private/safe.yml", "/private/disabled.yml"),
      present = c(TRUE, TRUE),
      stringsAsFactors = FALSE),
    .package = "dsImaging"
  )
  capabilities <- imagingCapabilitiesDS()
  expect_false(any(c("envs", "scheduler", "onload_errors") %in%
                   names(capabilities)))
  expect_named(capabilities$models, c("provider", "task", "ready"))
  expect_equal(capabilities$models$provider, c("example", "unknown"))
  expect_equal(capabilities$models$task, c("task", "unknown"))
  expect_identical(capabilities$profiles, "safe_profile")
  expect_named(capabilities$runners, c("runner", "present"))
  expect_identical(capabilities$runners$runner, "image_embeddings")
  expect_false(grepl("/private/", jsonlite::toJSON(capabilities), fixed = TRUE))
})

test_that("model installation never reflects non-canonical identifiers", {
  calls <- 0L
  private_diagnostic <- paste(
    "/var/lib/dsimaging/models/nnunetv2/Task001",
    "s3://private-bucket/models", "patient-007")
  testthat::local_mocked_bindings(
    .verify_radiomics_admin = function(...) invisible(TRUE),
    install_model = function(...) {
      calls <<- calls + 1L
      warning(private_diagnostic, call. = FALSE)
      message(private_diagnostic)
      invisible(TRUE)
    },
    .package = "dsImaging")
  provider <- "/srv/private/model-provider"
  task <- "s3://private-bucket/patient-007"

  result <- imagingInstallModelDS("admin", provider, task)
  expect_identical(result, list(
    status = "failed", provider = "unknown", task = "unknown",
    error = "installation_failed"))
  expect_identical(calls, 0L)
  public <- jsonlite::toJSON(result)
  expect_false(grepl(provider, public, fixed = TRUE))
  expect_false(grepl(task, public, fixed = TRUE))
  expect_false(grepl("patient-007", public, fixed = TRUE))

  warnings <- character()
  messages <- character()
  valid <- withCallingHandlers(
    imagingInstallModelDS("admin", "nnunetv2", "Task001"),
    warning = function(w) {
      warnings <<- c(warnings, conditionMessage(w))
      invokeRestart("muffleWarning")
    },
    message = function(m) {
      messages <<- c(messages, conditionMessage(m))
      invokeRestart("muffleMessage")
    })
  expect_identical(valid, list(
    status = "installed", provider = "nnunetv2", task = "Task001"))
  expect_identical(calls, 1L)
  expect_length(warnings, 0L)
  expect_length(messages, 0L)
})
