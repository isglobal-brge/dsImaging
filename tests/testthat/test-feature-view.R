feature_view_fixture <- function(env = parent.frame(), subset_asset = FALSE,
                                 label_col = "diagnosis") {
  rows <- data.frame(
    sample_id = paste0("scan", 1:4),
    patient_id = c("patient1", "patient1", "patient2", "patient3"),
    diagnosis = c("case", "case", "control", "control"),
    stringsAsFactors = FALSE)
  metadata_path <- tempfile(fileext = ".csv")
  utils::write.csv(rows, metadata_path, row.names = FALSE)
  manifest <- test_privacy_manifest(metadata_path, label_col = label_col)
  if (!is.null(label_col)) {
    manifest$metadata$label_levels <- c("case", "control")
  }
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

external_clinical_rows <- function(rows) {
  patients <- unique(rows$patient_id)
  data.frame(
    sample_id = paste0("not-an-image-id-", seq_along(patients)),
    link_id = rev(patients),
    patient_id = rev(patients),
    age = c(40L, 55L, 70L),
    sex = c("F", "M", "F"),
    outcome = c("control", "case", "case"),
    stringsAsFactors = FALSE)
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

test_that("feature views safely join an external same-session clinical table", {
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    nfilter.subset = 3L,
    default.nfilter.subset = 3L))
  fixture <- feature_view_fixture(environment(), label_col = NULL)
  asset_id <- fixture$asset_id
  clinical <- external_clinical_rows(fixture$rows)
  assign("clinical", clinical, envir = environment())
  feature_selection <- dsImaging:::.dsr_encode("radiomics_mean")
  clinical_selection <- dsImaging:::.dsr_encode(c("age", "sex"))
  public_levels <- dsImaging:::.dsr_encode(c("case", "control"))

  reference <- imagingFeatureViewDS(
    "img", asset_id,
    columns = feature_selection,
    clinical_symbol = "clinical",
    clinical_columns = clinical_selection,
    target_col = "outcome",
    target_levels = public_levels)
  expect_s3_class(reference, "dsimaging_feature_view_ref")
  expect_identical(names(reference), "capability")
  assign("features", reference, envir = environment())

  resolved <- dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "features", expected_capability = reference$capability,
    owner_env = environment())
  expect_identical(names(resolved$data), c(
    "sample_id", "radiomics_mean", "age", "sex", "outcome", "patient_id"))
  matched <- match(resolved$data$patient_id, clinical$link_id)
  expect_identical(resolved$data$age, clinical$age[matched])
  expect_identical(resolved$data$sex, clinical$sex[matched])
  expect_identical(resolved$data$outcome, clinical$outcome[matched])
  expect_length(unique(resolved$data$age[
    resolved$data$patient_id == "patient1"]), 1L)
  expect_identical(
    resolved$data$patient_id,
    fixture$rows$patient_id[match(
      resolved$data$sample_id, fixture$rows$sample_id)])
  expect_identical(resolved$privacy$label_col, "outcome")
  expect_identical(resolved$privacy$label_levels, c("case", "control"))
  expect_identical(resolved$manifest$metadata$label_col, "outcome")
  expect_identical(
    resolved$manifest$metadata$label_levels, c("case", "control"))

  source <- dsImaging:::.resolve_imaging_handle_for_consumer(
    "img", owner_env = environment())
  expect_null(source$privacy$label_col)
  expect_null(source$manifest$metadata$label_col)
  expect_identical(get("clinical", envir = environment()), clinical)

  assign("clinical", data.frame(rebound = TRUE), envir = environment())
  expect_no_error(dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "features", expected_capability = reference$capability,
    owner_env = environment()))
})

test_that("external clinical linkage is total and keeps the imaging roster", {
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    nfilter.subset = 3L,
    default.nfilter.subset = 3L))
  fixture <- feature_view_fixture(environment(), label_col = NULL)
  asset_id <- fixture$asset_id
  valid <- external_clinical_rows(fixture$rows)
  variants <- list(
    missing = valid[-1L, , drop = FALSE],
    extra = rbind(valid, transform(valid[1L, , drop = FALSE],
      link_id = "scan-extra", patient_id = "patient-extra")),
    duplicate = rbind(valid[-1L, , drop = FALSE], valid[2L, , drop = FALSE]),
    unknown = transform(valid, link_id = paste0("other", seq_len(nrow(valid)))),
    missing_id = transform(valid, link_id = replace(link_id, 1L, NA_character_)),
    noncanonical = transform(
      valid, link_id = replace(link_id, 1L, paste0(link_id[[1L]], " ")))
  )
  for (name in names(variants)) {
    table <- variants[[name]]
    assign("clinical", table, envir = environment())
    reference <- imagingFeatureViewDS(
      "img", asset_id, clinical_symbol = "clinical",
      clinical_id_col = "link_id", clinical_columns = "age",
      target_col = "outcome")
    assign("features", reference, envir = environment())
    resolved <- dsImaging:::.resolve_imaging_feature_view_for_consumer(
      "features", owner_env = environment())
    expect_identical(nrow(resolved$data), 4L, info = name)
    expect_identical(resolved$privacy_roster$privacy_unit_count, 3L,
                     info = name)
    expect_setequal(resolved$data$sample_id, fixture$rows$sample_id)
    usable <- !is.na(table$link_id) &
      table$link_id == dsImaging:::.canonical_imaging_privacy_ids(table$link_id)
    duplicate <- duplicated(table$link_id) |
      duplicated(table$link_id, fromLast = TRUE)
    lookup <- table$link_id
    lookup[!usable | duplicate] <- NA_character_
    matched <- match(resolved$data$patient_id, lookup)
    expect_identical(resolved$data$age, table$age[matched], info = name)
    expect_identical(resolved$data$outcome, table$outcome[matched], info = name)
    expect_identical(
      resolved$data$patient_id,
      fixture$rows$patient_id[match(
        resolved$data$sample_id, fixture$rows$sample_id)],
      info = name)
  }

  assign("clinical", valid[nrow(valid):1L, , drop = FALSE],
         envir = environment())
  expect_no_error(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", target_col = "outcome"))
})

test_that("external patient values never control the imaging privacy unit", {
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    nfilter.subset = 3L,
    default.nfilter.subset = 3L))
  fixture <- feature_view_fixture(environment(), label_col = NULL)
  asset_id <- fixture$asset_id
  clinical <- external_clinical_rows(fixture$rows)
  clinical$patient_id[[1L]] <- "wrong-patient"
  assign("clinical", clinical, envir = environment())
  reference <- imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", target_col = "outcome")
  assign("features", reference, envir = environment())
  resolved <- dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "features", owner_env = environment())
  expect_identical(
    resolved$data$patient_id,
    fixture$rows$patient_id[match(
      resolved$data$sample_id, fixture$rows$sample_id)])

  clinical$patient_id <- NULL
  assign("clinical", clinical, envir = environment())
  reference <- imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", target_col = "outcome")
  assign("features", reference, envir = environment())
  resolved <- dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "features", owner_env = environment())
  expect_identical(
    resolved$data$patient_id,
    fixture$rows$patient_id[match(
      resolved$data$sample_id, fixture$rows$sample_id)])

  clinical <- external_clinical_rows(fixture$rows)
  assign("clinical", clinical, envir = environment())
  generic <- "^Imaging feature view could not be created\\.$"
  expect_error(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", clinical_columns = "patient_id",
    target_col = "outcome"), generic)
  expect_error(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", clinical_columns = "sample_id",
    target_col = "outcome"), generic)
  expect_error(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", target_col = "patient_id"), generic)
})

test_that("external clinical linkage rejects unsafe or ambiguous schemas", {
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    nfilter.subset = 3L,
    default.nfilter.subset = 3L))
  fixture <- feature_view_fixture(environment(), label_col = NULL)
  asset_id <- fixture$asset_id
  clinical <- external_clinical_rows(fixture$rows)
  assign("clinical", clinical, envir = environment())
  generic <- "^Imaging feature view could not be created\\.$"

  expect_error(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical"), generic)
  expect_error(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "missing", target_col = "outcome"), generic)
  expect_error(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", clinical_columns = "missing",
    target_col = "outcome"), generic)
  expect_error(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", clinical_columns = "link_id",
    target_col = "outcome"), generic)
  expect_error(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", clinical_columns = "outcome",
    target_col = "outcome"), generic)
  expect_error(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", clinical_columns = "unsafe column",
    target_col = "outcome"), generic)

  collision <- clinical
  collision$radiomics_mean <- seq_len(nrow(collision))
  assign("clinical", collision, envir = environment())
  expect_error(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", clinical_columns = "radiomics_mean",
    target_col = "outcome"), generic)
  expect_error(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", target_col = "radiomics_mean"), generic)

  missing_target <- clinical
  missing_target$outcome[[1L]] <- NA_character_
  assign("clinical", missing_target, envir = environment())
  expect_no_error(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", target_col = "outcome"))
})

test_that("external target levels are explicit public policy, never inferred", {
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    nfilter.subset = 3L,
    default.nfilter.subset = 3L))
  fixture <- feature_view_fixture(environment(), label_col = NULL)
  asset_id <- fixture$asset_id
  clinical <- external_clinical_rows(fixture$rows)
  assign("clinical", clinical, envir = environment())
  generic <- "^Imaging feature view could not be created\\.$"

  reference <- imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", target_col = "outcome")
  assign("features", reference, envir = environment())
  resolved <- dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "features", owner_env = environment())
  expect_null(resolved$privacy$label_levels)
  expect_null(resolved$manifest$metadata$label_levels)

  clinical$binary_outcome <- c(0, 1, 1)
  assign("clinical", clinical, envir = environment())
  numeric_levels <- dsImaging:::.dsr_encode(c(0, 1))
  numeric_reference <- imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", target_col = "binary_outcome",
    target_levels = numeric_levels)
  assign("numeric_features", numeric_reference, envir = environment())
  numeric_resolved <- dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "numeric_features", owner_env = environment())
  expect_identical(numeric_resolved$privacy$label_levels, c("0", "1"))
  expect_identical(numeric_resolved$data$binary_outcome,
                   clinical$binary_outcome[match(
                     numeric_resolved$data$patient_id, clinical$link_id)])

  for (levels in list(c("case", "case"), c("case", "not public"), "case")) {
    expect_error(imagingFeatureViewDS(
      "img", asset_id, clinical_symbol = "clinical",
      clinical_id_col = "link_id", target_col = "outcome",
      target_levels = levels), generic)
  }
  for (levels in list(
      c("case", "other"), c("case", fixture$rows$sample_id[[1L]]))) {
    expect_no_error(imagingFeatureViewDS(
      "img", asset_id, clinical_symbol = "clinical",
      clinical_id_col = "link_id", target_col = "outcome",
      target_levels = levels))
  }
})

test_that("clinical covariates are authorized by role, not private values", {
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    nfilter.subset = 3L,
    default.nfilter.subset = 3L))
  fixture <- feature_view_fixture(environment(), label_col = NULL)
  asset_id <- fixture$asset_id
  clinical <- external_clinical_rows(fixture$rows)
  clinical$site <- c(fixture$rows$sample_id[[1L]], "north", "south")
  assign("clinical", clinical, envir = environment())

  expect_no_error(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_columns = "site", target_col = "outcome"))
})

test_that("external linkage rejects matrix-shaped clinical columns", {
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    nfilter.subset = 3L,
    default.nfilter.subset = 3L))
  fixture <- feature_view_fixture(environment(), label_col = NULL)
  asset_id <- fixture$asset_id
  valid <- external_clinical_rows(fixture$rows)
  generic <- "^Imaging feature view could not be created\\.$"

  matrix_id <- valid[1:2, c("age", "outcome"), drop = FALSE]
  matrix_id$link_id <- I(matrix(
    c(valid$link_id, "patient-extra"), nrow = 2L))
  assign("clinical", matrix_id, envir = environment())
  expect_error(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", clinical_columns = "age",
    target_col = "outcome"), generic)

  matrix_target <- valid
  matrix_target$outcome <- I(matrix(valid$outcome, ncol = 1L))
  assign("clinical", matrix_target, envir = environment())
  expect_error(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", clinical_columns = "age",
    target_col = "outcome"), generic)

  matrix_covariate <- valid
  matrix_covariate$age <- I(matrix(valid$age, ncol = 1L))
  assign("clinical", matrix_covariate, envir = environment())
  expect_error(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", clinical_columns = "age",
    target_col = "outcome"), generic)
})

test_that("external clinical tables must be bound in the handle session", {
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    nfilter.subset = 3L,
    default.nfilter.subset = 3L))
  owner <- new.env(parent = environment())
  fixture <- feature_view_fixture(owner, label_col = NULL)
  assign("asset_id", fixture$asset_id, envir = owner)
  assign("clinical", external_clinical_rows(fixture$rows),
         envir = environment())
  assign("not_a_table", "clinical", envir = owner)
  generic <- "^Imaging feature view could not be created\\.$"

  expect_error(eval(quote(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    clinical_id_col = "link_id", target_col = "outcome")), envir = owner),
    generic)
  expect_error(eval(quote(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "not_a_table",
    clinical_id_col = "link_id", target_col = "outcome")), envir = owner),
    generic)
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

test_that("external clinical linkage survives the DSLite expression boundary", {
  skip_if_not_installed("DSLite")
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L,
    nfilter.subset = 3L,
    default.nfilter.subset = 3L))
  server <- DSLite::newDSLiteServer(config = list(), strict = TRUE)
  server$assignMethod(
    "imagingFeatureViewDS", "dsImaging::imagingFeatureViewDS")
  server_name <- paste0("dsimaging_external_feature_view_", Sys.getpid())
  assign(server_name, server, envir = .GlobalEnv)
  withr::defer(rm(list = server_name, envir = .GlobalEnv))
  connection <- DSLite::dsConnect(
    DSLite::DSLite(), name = "site", url = server_name)
  withr::defer(DSLite::dsDisconnect(connection))
  session <- server$getSession(connection@sid)
  fixture <- feature_view_fixture(session, label_col = NULL)
  assign("clinical", external_clinical_rows(fixture$rows), envir = session)
  feature_columns <- dsImaging:::.dsr_encode("radiomics_mean")
  clinical_columns <- dsImaging:::.dsr_encode(c("age", "sex"))
  target_levels <- dsImaging:::.dsr_encode(c("case", "control"))
  expression <- sprintf(
    paste0('imagingFeatureViewDS("img", "%s", "%s", "clinical", ',
           '"patient_id", "%s", "outcome", "%s")'),
    fixture$asset_id, feature_columns, clinical_columns, target_levels)

  expect_no_error(DSLite::dsAssignExpr(
    connection, "features", expression, async = FALSE))
  reference <- server$getSessionData(connection@sid, "features")
  expect_s3_class(reference, "dsimaging_feature_view_ref")
  resolved <- dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "features", owner_env = session)
  expect_identical(names(resolved$data), c(
    "sample_id", "radiomics_mean", "age", "sex", "outcome", "patient_id"))
  expect_identical(resolved$manifest$metadata$label_col, "outcome")
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
  clinical <- table
  clinical$outcome <- c("case", "case", "control", "control")
  assign("clinical", clinical, envir = environment())
  expect_error(imagingFeatureViewDS(
    "img", asset_id, clinical_symbol = "clinical",
    target_col = "outcome"),
    "^Imaging feature view could not be created\\.$")
  expect_no_error(imagingFeatureViewDS("img", asset_id))
  rm(table, derived, copy, clinical)
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
