test_that("validated global assets are published as opaque tracking references", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(dsimaging.asset_db = db_path))
  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  asset_id <- dsImaging:::.asset_register(
    db, "study", "feature_table", tempfile("feature-root-"),
    derivation_hash = strrep("b", 64), visibility = "global",
    collection_seal = strrep("a", 64))
  published <- NULL
  finished <- NULL
  testthat::local_mocked_bindings(
    hpcTrackingPublishReferenceInternal = function(
        tracking_id, output_name, reference, classification) {
      published <<- list(tracking_id = tracking_id,
        output_name = output_name, reference = reference,
        classification = classification)
    },
    hpcTrackingFinishInternal = function(tracking_id, success = TRUE) {
      finished <<- list(tracking_id = tracking_id, success = success)
    },
    .package = "dsHPC")

  tracking_id <- "trk_11111111-2222-4333-8444-555555555555"
  expect_invisible(dsImaging:::.imaging_tracking_publish_asset(
    tracking_id, asset_id, finish = TRUE))
  expect_identical(published, list(tracking_id = tracking_id,
    output_name = "imaging_asset", reference = asset_id,
    classification = "server_reusable"))
  expect_identical(finished,
    list(tracking_id = tracking_id, success = TRUE))
  expect_false(any(grepl("feature-root", unlist(published), fixed = TRUE)))

  private_id <- dsImaging:::.asset_register(
    db, "study", "feature_table", tempfile("private-root-"),
    visibility = "private")
  expect_error(dsImaging:::.imaging_tracking_publish_asset(
    tracking_id, private_id), "unavailable", fixed = TRUE)
})

test_that("second and third asset reuse do not republish a sealed root", {
  tracking_id <- "trk_11111111-2222-4333-8444-555555555555"
  asset_id <- paste0("asset_", strrep("a", 32))
  published <- 0L
  resolved <- 0L
  testthat::local_mocked_bindings(
    .imaging_tracking_publish_asset = function(...) {
      published <<- published + 1L
      invisible(asset_id)
    },
    .imaging_tracking_resolve_asset = function(...) {
      resolved <<- resolved + 1L
      asset_id
    },
    .imaging_tracking_asset = function(...) list(asset_id = asset_id),
    .package = "dsImaging")

  expect_invisible(dsImaging:::.imaging_tracking_reuse_asset(
    list(tracking_id = tracking_id, is_done = FALSE), asset_id))
  for (i in 1:2) {
    expect_invisible(dsImaging:::.imaging_tracking_reuse_asset(
      list(tracking_id = tracking_id, is_done = TRUE), asset_id))
  }

  expect_identical(published, 1L)
  expect_identical(resolved, 2L)
})

test_that("sealed scoped roots retain local asset reuse", {
  tracking_id <- "trk_11111111-2222-4333-8444-555555555555"
  asset_id <- paste0("asset_", strrep("a", 32))
  testthat::local_mocked_bindings(
    .imaging_tracking_asset = function(...) list(asset_id = asset_id),
    .imaging_tracking_resolve_asset = function(...) stop("scoped"),
    .package = "dsImaging")
  withr::local_options(list(dshpc.queue_visibility = "scoped"))
  expect_invisible(dsImaging:::.imaging_tracking_reuse_asset(
    list(tracking_id = tracking_id, is_done = TRUE), asset_id))
})

test_that("direct publisher defers root sealing until the primary is terminal", {
  tracking_id <- "trk_11111111-2222-4333-8444-555555555555"
  asset_id <- paste0("asset_", strrep("a", 32))
  observed <- NULL
  testthat::local_mocked_bindings(
    hpcTrackingForJobInternal = function(job_id) tracking_id,
    .package = "dsHPC")
  testthat::local_mocked_bindings(
    .imaging_tracking_publish_asset = function(tracking_id, asset_id,
                                                finish = FALSE, ...) {
      observed <<- list(tracking_id = tracking_id, asset_id = asset_id,
        finish = finish)
      invisible(asset_id)
    },
    .package = "dsImaging")

  expect_true(dsImaging:::.imaging_tracking_publish_job_asset(
    "job_11111111-2222-4333-8444-555555555555", asset_id))
  expect_identical(observed, list(tracking_id = tracking_id,
    asset_id = asset_id, finish = FALSE))
})

test_that("opaque dsHPC references resolve only to active dsImaging assets", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(dsimaging.asset_db = db_path))
  db <- dsImaging:::.asset_db_connect()
  asset_id <- dsImaging:::.asset_register(
    db, "study", "feature_table", tempfile("feature-root-"),
    derivation_hash = strrep("b", 64), visibility = "global",
    collection_seal = strrep("a", 64))
  dsImaging:::.asset_db_close(db)
  provider <- "dsImaging"
  testthat::local_mocked_bindings(
    hpcTrackingResolveOutputInternal = function(...) structure(list(
      provider = provider, reference = asset_id,
      classification = "server_reusable", output_name = "imaging_asset",
      tracking_id = "trk_11111111-2222-4333-8444-555555555555"),
      class = c("dshpc_domain_output_reference", "list")),
    .package = "dsHPC")
  opaque <- structure(list(
    tracking_id = "trk_11111111-2222-4333-8444-555555555555",
    output_name = "imaging_asset", kind = "domain_reference",
    classification = "server_reusable"),
    class = c("dshpc_output_reference", "list"))

  expect_identical(
    dsImaging:::.imaging_resolve_asset_argument(opaque), asset_id)
  provider <- "anotherPackage"
  expect_error(dsImaging:::.imaging_resolve_asset_argument(opaque),
    "unavailable", fixed = TRUE)
})

test_that("direct workflows submit one idempotent primary under a logical root", {
  session <- new.env(parent = globalenv())
  context_id <- paste0("dsctx_", strrep("a", 64))
  tracking_id <- "trk_11111111-2222-4333-8444-555555555555"
  observed <- NULL
  testthat::local_mocked_bindings(
    hpcRuntimeIdentityInternal = function(unit_selection = NULL)
      strrep("d", 64),
    hpcTrackingCreateInternal = function(reuse_key = NULL, kind = "analysis") list(
      tracking_id = tracking_id, state = "queued", is_done = FALSE,
      kind = kind, reused = FALSE),
    hpcSubmitInternal = function(spec_encoded, session_env = NULL,
                                 unit_selection = NULL, tracking_id = NULL,
                                 tracking_role = NULL,
                                 tracking_finalize = FALSE) {
      observed <<- list(spec = dsImaging:::.dsr_decode(spec_encoded),
        session_env = session_env, tracking_id = tracking_id,
        tracking_role = tracking_role,
        tracking_finalize = tracking_finalize)
      list(job_id = "job_11111111-2222-4333-8444-555555555555",
        .dshpc_capability = "private-capability", state = "PENDING")
    },
    .package = "dsHPC")

  reference <- dsImaging:::.imaging_submit_job(
    list(label = "dsImaging", visibility = "global", steps = list(
      list(type = "emit", plane = "session", output_name = "x", value = 1))),
    owner_env = session, dataset_id = "study",
    worker_context_id = context_id, output_asset_kind = "feature_table",
    derivation_hash = strrep("b", 64),
    collection_seal = strrep("c", 64))
  assign("workflow", reference, envir = session)
  workflow <- eval(quote(dsImaging:::.resolve_imaging_workflow("workflow")),
                   envir = session)

  expect_identical(observed$tracking_id, tracking_id)
  expect_identical(observed$tracking_role, "primary")
  expect_true(observed$tracking_finalize)
  expect_identical(observed$spec$visibility, "global")
  expect_identical(observed$spec$reuse_fingerprint, strrep("b", 64))
  expect_identical(workflow$tracking_id, tracking_id)
  expect_identical(workflow$action, "job")
})

test_that("shared imaging reuse changes with the installed runtime contract", {
  runtime_identity <- strrep("a", 64)
  reuse_keys <- character(0)
  testthat::local_mocked_bindings(
    hpcRuntimeIdentityInternal = function(unit_selection = NULL)
      runtime_identity,
    hpcTrackingCreateInternal = function(reuse_key = NULL, kind = "analysis") {
      reuse_keys <<- c(reuse_keys, reuse_key)
      list(tracking_id = paste0(
        "trk_11111111-2222-4333-8444-", sprintf("%012d", length(reuse_keys))),
        state = "queued", is_done = FALSE, kind = kind, reused = FALSE)
    },
    .package = "dsHPC")

  dsImaging:::.imaging_tracking_create("feature_table", strrep("b", 64))
  runtime_identity <- strrep("c", 64)
  dsImaging:::.imaging_tracking_create("feature_table", strrep("b", 64))

  expect_length(reuse_keys, 2L)
  expect_false(identical(reuse_keys[[1L]], reuse_keys[[2L]]))
})

test_that("a new session recovers a collection only through its admitted handle", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(dsimaging.asset_db = db_path))
  tracking_id <- "trk_11111111-2222-4333-8444-555555555555"
  collection_seal <- strrep("a", 64)
  generation <- claim_or_reuse_generation(
    dataset_id = "study", kind = "radiomics_collection",
    derivation_hash = strrep("b", 64), collection_seal = collection_seal,
    visibility = "global", owner_id = "owner", expected_n = 3L,
    spec = list(tracking_id = tracking_id))
  expect_identical(generation$action, "run_new")
  expect_identical(
    dsImaging:::.imaging_generation_for_tracking(tracking_id)$generation_id,
    generation$generation_id)

  admitted_seal <- collection_seal
  testthat::local_mocked_bindings(
    .authorized_imaging_dataset = function(handle_symbol, dataset_id = NULL,
                                           owner_env = NULL) list(
      dataset_id = "study", collection_seal = admitted_seal,
      handle = list(collection_seal = admitted_seal)),
    .package = "dsImaging")
  testthat::local_mocked_bindings(
    hpcTrackingStatusInternal = function(id) list(
      tracking_id = id, state = "running", is_done = FALSE,
      kind = "analysis"),
    .package = "dsHPC")

  session <- new.env(parent = globalenv())
  reference <- dsImaging:::.imaging_recover_tracking_workflow(
    tracking_id, "img", session)
  assign("recovered", reference, envir = session)
  workflow <- dsImaging:::.resolve_imaging_workflow(
    "recovered", owner_env = session)
  expect_identical(workflow$action, "running")
  expect_identical(workflow$generation_id, generation$generation_id)
  expect_identical(workflow$tracking_id, tracking_id)

  admitted_seal <- strrep("c", 64)
  expect_error(dsImaging:::.imaging_recover_tracking_workflow(
    tracking_id, "img", new.env(parent = globalenv())), "unavailable",
    fixed = TRUE)
})
