feature_view_fixture <- function(env = parent.frame(), subset_asset = FALSE) {
  rows <- data.frame(
    sample_id = paste0("scan", 1:4),
    patient_id = c("patient1", "patient1", "patient2", "patient3"),
    diagnosis = c("case", "case", "control", "control"),
    stringsAsFactors = FALSE)
  metadata_path <- tempfile(fileext = ".csv")
  utils::write.csv(rows, metadata_path, row.names = FALSE)
  manifest <- test_privacy_manifest(
    metadata_path, label_col = "diagnosis")
  manifest$metadata$label_levels <- c("case", "control")
  admission <- dsImaging:::.imaging_privacy_admission(manifest)
  handle <- list(
    dataset_id = "lung", manifest = manifest, backend = NULL,
    manifest_uri = NULL, privacy = admission$contract,
    n_privacy_units = admission$n_privacy_units,
    privacy_roster = admission$roster,
    collection_seal = strrep("a", 64))
  reference <- dsImaging:::.register_imaging_handle(handle, env)
  assign("img", reference, envir = env)

  feature_rows <- if (isTRUE(subset_asset)) rows[1:3, ] else rows
  feature_path <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    sample_id = feature_rows$sample_id,
    radiomics_mean = seq_len(nrow(feature_rows)),
    radiomics_energy = 2 * seq_len(nrow(feature_rows))),
    feature_path, row.names = FALSE)
  db <- dsImaging:::.asset_db_connect()
  asset_id <- dsImaging:::.asset_register(
    db, "lung", "feature_table", feature_path,
    visibility = "global", collection_seal = strrep("a", 64))
  dsImaging:::.asset_db_close(db)
  list(asset_id = asset_id, rows = rows)
}

test_that("opaque feature views retain the exact patient roster", {
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    nfilter.subset = 3L,
    default.nfilter.subset = 3L))
  fixture <- feature_view_fixture(environment())
  asset_id <- fixture$asset_id

  reference <- imagingFeatureViewDS("img", asset_id)
  expect_s3_class(reference, "dsimaging_feature_view_ref")
  expect_identical(names(reference), "capability")
  expect_match(reference$capability, "^imgf_[0-9a-f]{64}$")
  expect_false(is.data.frame(reference))
  expect_error(reference[1, , drop = FALSE])

  assign("features", reference, envir = environment())
  resolved <- dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "features", expected_capability = reference$capability,
    owner_env = environment())
  expect_identical(
    names(resolved$data),
    c("sample_id", "radiomics_mean", "radiomics_energy", "diagnosis",
      "patient_id"))
  expect_identical(
    resolved$data$patient_id,
    fixture$rows$patient_id[match(
      resolved$data$sample_id, fixture$rows$sample_id)])
  expect_identical(resolved$privacy_roster$privacy_unit_count, 3L)

  tombstone <- imagingFeatureViewDestroyDS("features")
  state <- dsImaging:::.imaging_session_state(
    environment(), create = FALSE)
  expect_identical(
    state$feature_views[[reference$capability]],
    dsImaging:::.dsimaging_session_tombstone)
  assign("features", tombstone, envir = environment())
  expect_identical(imagingFeatureViewDestroyDS("features"), reference)
})

test_that("encoded multi-column feature selections cross the expression boundary", {
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    nfilter.subset = 3L,
    default.nfilter.subset = 3L))
  fixture <- feature_view_fixture(environment())
  asset_id <- fixture$asset_id
  encoded <- dsImaging:::.dsr_encode(
    c("radiomics_mean", "radiomics_energy"))

  reference <- imagingFeatureViewDS("img", asset_id, encoded)
  assign("features", reference, envir = environment())
  resolved <- dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "features", owner_env = environment())

  expect_identical(
    names(resolved$data),
    c("sample_id", "radiomics_mean", "radiomics_energy", "diagnosis",
      "patient_id"))

  exported <- imagingLoadAssetDS("img", asset_id, encoded)
  expect_identical(
    names(exported), c("radiomics_mean", "radiomics_energy"))
})

test_that("encoded multi-column selections survive DSLite deparse and parse", {
  skip_if_not_installed("DSLite")
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    nfilter.subset = 3L,
    default.nfilter.subset = 3L))
  server <- DSLite::newDSLiteServer(config = list(), strict = TRUE)
  server$assignMethod(
    "imagingFeatureViewDS", "dsImaging::imagingFeatureViewDS")
  server_name <- paste0("dsimaging_feature_view_server_", Sys.getpid())
  assign(server_name, server, envir = .GlobalEnv)
  withr::defer(rm(list = server_name, envir = .GlobalEnv))
  connection <- DSLite::dsConnect(
    DSLite::DSLite(), name = "site", url = server_name)
  withr::defer(DSLite::dsDisconnect(connection))
  session <- server$getSession(connection@sid)
  fixture <- feature_view_fixture(session)
  encoded <- dsImaging:::.dsr_encode(
    c("radiomics_mean", "radiomics_energy"))
  expression <- sprintf(
    'imagingFeatureViewDS("img", "%s", "%s")',
    fixture$asset_id, encoded)

  expect_no_error(DSLite::dsAssignExpr(
    connection, "features", expression, async = FALSE))
  reference <- server$getSessionData(connection@sid, "features")
  expect_s3_class(reference, "dsimaging_feature_view_ref")
  resolved <- dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "features", owner_env = session)
  expect_identical(
    names(resolved$data),
    c("sample_id", "radiomics_mean", "radiomics_energy", "diagnosis",
      "patient_id"))
})

test_that("feature views reject incomplete assets and cross-session copies", {
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    nfilter.subset = 3L,
    default.nfilter.subset = 3L))
  fixture <- feature_view_fixture(environment(), subset_asset = TRUE)
  asset_id <- fixture$asset_id
  expect_error(
    imagingFeatureViewDS("img", asset_id),
    "feature view could not be created")

  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite")))
  complete <- feature_view_fixture(environment())
  asset_id <- complete$asset_id
  reference <- imagingFeatureViewDS("img", asset_id)
  assign("features", reference, envir = environment())
  other <- new.env(parent = globalenv())
  dsImaging:::.register_imaging_handle(list(unrelated = TRUE), other)
  assign("features", reference, envir = other)
  assign("imagingFeatureViewDestroyDS",
         dsImaging::imagingFeatureViewDestroyDS, envir = other)
  expect_error(
    dsImaging:::.resolve_imaging_feature_view_for_consumer(
      "features", reference$capability, owner_env = other),
    "cross-session")
  expect_error(
    eval(quote(imagingFeatureViewDestroyDS("features")), envir = other),
    "unavailable")
})

test_that("destroying or rebinding authority invalidates a feature view", {
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    nfilter.subset = 3L,
    default.nfilter.subset = 3L))
  first <- feature_view_fixture(environment())
  asset_id <- first$asset_id
  reference <- imagingFeatureViewDS("img", asset_id)
  assign("features", reference, envir = environment())

  second <- feature_view_fixture(environment())
  asset_id <- second$asset_id
  replacement <- imagingFeatureViewDS("img", asset_id)
  assign("features", replacement, envir = environment())
  state <- dsImaging:::.imaging_session_state(
    environment(), create = FALSE)
  expect_true(exists(
    replacement$capability, envir = state$feature_views, inherits = FALSE))
  expect_error(
    dsImaging:::.resolve_imaging_feature_view_for_consumer(
      "features", reference$capability, owner_env = environment()),
    "stale")

  assign("features", replacement, envir = environment())
  imagingDestroyDS("img")
  expect_identical(
    state$feature_views[[replacement$capability]],
    dsImaging:::.dsimaging_session_tombstone)
  expect_error(
    dsImaging:::.resolve_imaging_feature_view_for_consumer(
      "features", replacement$capability, owner_env = environment()),
    "stale")
})

test_that("naked feature export permanently taints its owner session", {
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    nfilter.subset = 3L,
    default.nfilter.subset = 3L))
  fixture <- feature_view_fixture(environment())
  asset_id <- fixture$asset_id
  table <- imagingLoadAssetDS("img", asset_id)
  expect_true(dsImaging:::.imaging_session_exported_feature_table(
    environment()))

  derived <- table[1:3, , drop = FALSE]
  copy <- derived
  rm(table, derived, copy)
  imagingDestroyDS("img")
  expect_true(dsImaging:::.imaging_session_exported_feature_table(
    environment()))
})

test_that("private session state is hidden, locked, and collectable", {
  collected <- tempfile("dsimaging-session-collected-")
  local({
    owner <- new.env(parent = emptyenv())
    reference <- dsImaging:::.register_imaging_handle(
      list(private_resource = new.env(parent = emptyenv())), owner)
    state <- dsImaging:::.imaging_session_state(owner, create = FALSE)
    expect_true(bindingIsLocked(
      dsImaging:::.DSIMAGING_SESSION_STATE_BINDING, owner))
    expect_true(environmentIsLocked(state))
    expect_true(exists(
      reference$capability, envir = state$handles, inherits = FALSE))
    reg.finalizer(state, function(e) file.create(collected), onexit = FALSE)
  })

  for (attempt in seq_len(10L)) {
    gc()
    if (file.exists(collected)) break
  }
  expect_true(file.exists(collected))
})
