test_that("imagingLoadAssetDS loads radiomics collection feature tables", {
  testthat::skip_if_not_installed("arrow")
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    nfilter.subset = 3,
    default.nfilter.subset = 3
  ))

  features_dir <- tempfile("radiomics-asset")
  dir.create(features_dir)
  arrow::write_parquet(
    data.frame(
      sample_id = c("case001", "case002", "case003"),
      original_firstorder_Mean = c(1.1, 2.2, 3.3)
    ),
    file.path(features_dir, "radiomics_features.parquet")
  )
  metadata_path <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    sample_id = c("case001", "case002", "case003"),
    patient_id = c("patient001", "patient002", "patient003")),
    metadata_path, row.names = FALSE)
  assign_test_imaging_handle(metadata_path)

  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  asset_id <- dsImaging:::.asset_register(
    db,
    dataset_id = "lung",
    kind = "radiomics_collection",
    path_or_root = features_dir,
    derivation_hash = "load-feature-test",
    visibility = "global",
    collection_seal = strrep("a", 64)
  )

  df <- dsImaging:::imagingLoadAssetDS("img", asset_id)
  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 3L)
  expect_equal(df$original_firstorder_Mean, c(1.1, 2.2, 3.3))

  columns <- c("sample_id", "original_firstorder_Mean")
  subset <- dsImaging:::imagingLoadAssetDS("img", asset_id, columns = columns)
  expect_equal(names(subset), c("sample_id", "original_firstorder_Mean"))
  expect_error(imagingLoadAssetDS(
    "img", asset_id, include_metadata = TRUE),
    "metadata is unavailable")
})

test_that("imagingLoadAssetDS can join dataset metadata", {
  testthat::skip_if_not_installed("arrow")
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    nfilter.subset = 3,
    default.nfilter.subset = 3
  ))

  features_dir <- tempfile("radiomics-asset")
  dir.create(features_dir)
  arrow::write_parquet(
    data.frame(
      sample_id = c("case001", "case002", "case003"),
      original_firstorder_Energy = c(10, 20, 30)
    ),
    file.path(features_dir, "radiomics_features.parquet")
  )

  metadata_path <- tempfile(fileext = ".parquet")
  arrow::write_parquet(
    data.frame(
      sample_id = c("case001", "case002", "case003"),
      patient_id = c("patient001", "patient002", "patient003"),
      age = c(68, 71, 55),
      deadstatus_event = c(1L, 0L, 1L)
    ),
    metadata_path
  )

  assign_test_imaging_handle(metadata_path,
    label_col = "deadstatus_event")

  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  asset_id <- dsImaging:::.asset_register(
    db,
    dataset_id = "lung",
    kind = "radiomics_collection",
    path_or_root = features_dir,
    derivation_hash = "load-feature-metadata-test",
    visibility = "global",
    collection_seal = strrep("a", 64)
  )

  df <- dsImaging:::imagingLoadAssetDS(
    "img", asset_id,
    include_metadata = TRUE
  )
  expect_equal(names(df), c(
    "sample_id", "original_firstorder_Energy", "deadstatus_event"
  ))
  expect_false(any(c("patient_id", "age") %in% names(df)))

  columns <- c("sample_id", "deadstatus_event")
  subset <- dsImaging:::imagingLoadAssetDS(
    "img", asset_id, columns = columns, include_metadata = TRUE)
  expect_equal(names(subset), c("sample_id", "deadstatus_event"))

  repaired <- dsImaging:::imagingLoadAssetDS(
    "img", asset_id,
    include_metadata = TRUE,
    syntactic_names = TRUE
  )
  expect_equal(names(repaired), c(
    "sample_id", "original_firstorder_Energy", "deadstatus_event"
  ))
})

test_that("imagingLoadAssetDS can repair non-syntactic radiomics names", {
  testthat::skip_if_not_installed("arrow")
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    nfilter.subset = 3,
    default.nfilter.subset = 3
  ))

  features_dir <- tempfile("radiomics-asset")
  dir.create(features_dir)
  arrow::write_parquet(
    data.frame(
      sample_id = c("case001", "case002", "case003"),
      check.names = FALSE,
      "wavelet-HLH_glrlm_RunLengthNonUniformity" = c(1, 2, 3)
    ),
    file.path(features_dir, "radiomics_features.parquet")
  )
  metadata_path <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    sample_id = c("case001", "case002", "case003"),
    patient_id = c("patient001", "patient002", "patient003")),
    metadata_path, row.names = FALSE)
  assign_test_imaging_handle(metadata_path)

  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  asset_id <- dsImaging:::.asset_register(
    db,
    dataset_id = "lung",
    kind = "radiomics_collection",
    path_or_root = features_dir,
    derivation_hash = "load-feature-syntactic-test",
    visibility = "global",
    collection_seal = strrep("a", 64)
  )

  df <- dsImaging:::imagingLoadAssetDS(
    "img", asset_id,
    syntactic_names = TRUE
  )
  expect_true("wavelet.HLH_glrlm_RunLengthNonUniformity" %in% names(df))
})

test_that("feature loading rejects arbitrary subsets and cross-dataset assets", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    dsimaging.nfilter.subset = 3L
  ))
  metadata_path <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    sample_id = paste0("case00", 1:4),
    patient_id = paste0("patient00", 1:4)),
    metadata_path, row.names = FALSE)
  assign_test_imaging_handle(metadata_path)

  subset_path <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    sample_id = paste0("case00", 1:3), feature = 1:3),
    subset_path, row.names = FALSE)
  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  subset_id <- dsImaging:::.asset_register(
    db, "lung", "feature_table", subset_path,
    visibility = "global", collection_seal = strrep("a", 64))
  other_id <- dsImaging:::.asset_register(
    db, "different.dataset", "feature_table", subset_path,
    visibility = "global", collection_seal = strrep("a", 64))

  expect_error(imagingLoadAssetDS("img", subset_id),
               "complete admitted collection")
  expect_error(imagingLoadAssetDS("img", other_id),
               "not found for dataset")
})

test_that("feature asset roots resolve one confined non-symlinked table", {
  root <- withr::local_tempdir()
  first <- file.path(root, "first.csv")
  second <- file.path(root, "second.csv")
  utils::write.csv(data.frame(sample_id = letters[1:3]), first,
                   row.names = FALSE)
  expect_identical(
    dsImaging:::.resolve_feature_asset_file(root, "dataset"),
    normalizePath(first, winslash = "/", mustWork = TRUE))

  utils::write.csv(data.frame(sample_id = letters[1:3]), second,
                   row.names = FALSE)
  expect_error(
    dsImaging:::.resolve_feature_asset_file(root, "dataset"),
    "exactly one table")

  unlink(second)
  outside <- tempfile(fileext = ".csv")
  on.exit(unlink(outside), add = TRUE)
  utils::write.csv(data.frame(sample_id = letters[1:3]), outside,
                   row.names = FALSE)
  unlink(first)
  linked <- suppressWarnings(file.symlink(outside, first))
  if (isTRUE(linked)) {
    expect_error(
      dsImaging:::.resolve_feature_asset_file(root, "dataset"),
      "symbolic link")
  }
})

test_that("feature loading rejects duplicate rows even with the full roster", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    dsimaging.nfilter.subset = 3L
  ))
  metadata_path <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    sample_id = paste0("case00", 1:4),
    patient_id = paste0("patient00", 1:4)),
    metadata_path, row.names = FALSE)
  assign_test_imaging_handle(metadata_path)

  duplicate_path <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    sample_id = c(paste0("case00", 1:4), "case001"),
    feature = 1:5), duplicate_path, row.names = FALSE)
  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  asset_id <- dsImaging:::.asset_register(
    db, "lung", "feature_table", duplicate_path,
    visibility = "global", collection_seal = strrep("a", 64))

  expect_error(imagingLoadAssetDS("img", asset_id),
               "complete admitted collection")
})

test_that("inactive feature assets cannot be loaded by raw ID", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    dsimaging.nfilter.subset = 3L
  ))
  metadata_path <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    sample_id = paste0("case00", 1:3),
    patient_id = paste0("patient00", 1:3)),
    metadata_path, row.names = FALSE)
  assign_test_imaging_handle(metadata_path)

  feature_path <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    sample_id = paste0("case00", 1:3), feature = 1:3),
    feature_path, row.names = FALSE)
  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  asset_id <- dsImaging:::.asset_register(
    db, "lung", "feature_table", feature_path,
    visibility = "global", collection_seal = strrep("a", 64))
  DBI::dbExecute(db, "UPDATE assets SET status='deprecated' WHERE asset_id=?",
                 params = list(asset_id))

  expect_error(imagingLoadAssetDS("img", asset_id),
               "not found for dataset")
})

test_that("private assets require a session grant even for the same OS user", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_envvar(USER = "shared-node-user")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    dsimaging.nfilter.subset = 3L
  ))
  metadata_path <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    sample_id = paste0("case00", 1:3),
    patient_id = paste0("patient00", 1:3)),
    metadata_path, row.names = FALSE)
  session <- new.env(parent = globalenv())
  other_session <- new.env(parent = globalenv())
  assign_test_imaging_handle(metadata_path, env = session)
  assign_test_imaging_handle(metadata_path, env = other_session)

  feature_path <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    sample_id = paste0("case00", 1:3), feature = 1:3),
    feature_path, row.names = FALSE)
  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  asset_id <- dsImaging:::.asset_register(
    db, "lung", "feature_table", feature_path,
    created_by = "shared-node-user", collection_seal = strrep("a", 64))

  load_call <- call("imagingLoadAssetDS", "img", asset_id)
  expect_error(eval(load_call, envir = session), "not found")
  workflow <- dsImaging:::.register_imaging_workflow(list(
    action = "published", asset_id = asset_id, dataset_id = "lung"), session)
  assign("workflow", workflow, envir = session)
  status <- eval(quote(imagingCollectionStatusDS("workflow")),
                 envir = session)
  expect_equal(status$asset_id, asset_id)
  loaded <- eval(load_call, envir = session)
  expect_equal(loaded$feature, 1:3)
  expect_error(eval(load_call, envir = other_session), "not found")
})

test_that("asset catalogs and loads are isolated by immutable collection seal", {
  db_path <- tempfile(fileext = ".sqlite")
  metadata_path <- tempfile(fileext = ".csv")
  feature_path <- tempfile(fileext = ".csv")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    dsimaging.nfilter.subset = 3L
  ))
  utils::write.csv(data.frame(
    sample_id = paste0("case00", 1:3),
    patient_id = paste0("patient00", 1:3)),
    metadata_path, row.names = FALSE)
  utils::write.csv(data.frame(
    sample_id = paste0("case00", 1:3), feature = c(101, 202, 303)),
    feature_path, row.names = FALSE)
  collection_a <- new.env(parent = globalenv())
  collection_b <- new.env(parent = globalenv())
  assign_test_imaging_handle(metadata_path, env = collection_a,
    collection_seal = strrep("a", 64))
  assign_test_imaging_handle(metadata_path, env = collection_b,
    collection_seal = strrep("b", 64))

  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  asset_id <- dsImaging:::.asset_register(
    db, "lung", "feature_table", feature_path, visibility = "global",
    collection_seal = strrep("a", 64))

  catalog_a <- eval(quote(imagingAssetCatalogDS("img")), envir = collection_a)
  catalog_b <- eval(quote(imagingAssetCatalogDS("img")), envir = collection_b)
  expect_identical(catalog_a$asset_id, asset_id)
  expect_equal(nrow(catalog_b), 0L)
  expect_equal(eval(call("imagingLoadAssetDS", "img", asset_id),
    envir = collection_a)$feature, c(101, 202, 303))
  expect_error(eval(call("imagingLoadAssetDS", "img", asset_id),
    envir = collection_b), "not found")
})

test_that("feature schemas and catalog kinds cannot carry private strings", {
  db_path <- tempfile(fileext = ".sqlite")
  metadata_path <- tempfile(fileext = ".csv")
  feature_path <- tempfile(fileext = ".csv")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    dsimaging.nfilter.subset = 3L
  ))
  utils::write.csv(data.frame(
    sample_id = paste0("case00", 1:3),
    patient_id = paste0("patient00", 1:3)),
    metadata_path, row.names = FALSE)
  unsafe <- data.frame(sample_id = paste0("case00", 1:3), check.names = FALSE)
  unsafe[["/srv/private/cohort-A/patient-007.dcm"]] <- 1:3
  utils::write.csv(unsafe, feature_path, row.names = FALSE)
  assign_test_imaging_handle(metadata_path)
  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  asset_id <- dsImaging:::.asset_register(
    db, "lung", "feature_table", feature_path, visibility = "global",
    collection_seal = strrep("a", 64))

  expect_error(imagingLoadAssetDS("img", asset_id),
    "schema is unavailable", fixed = TRUE)
  expect_error(dsImaging:::.asset_register(
    db, "lung", "/srv/private/cohort-A", feature_path,
    collection_seal = strrep("a", 64)),
    "kind is not supported", fixed = TRUE)

  DBI::dbExecute(db, "UPDATE assets SET kind = ? WHERE asset_id = ?",
    params = list("/srv/private/cohort-A", asset_id))
  catalog <- imagingAssetCatalogDS("img")
  expect_identical(catalog$kind, "unknown")
  error <- tryCatch(imagingLoadAssetDS("img", asset_id), error = identity)
  expect_identical(conditionMessage(error),
    "Requested imaging asset is not a loadable feature table.")
})

test_that("metadata loading exposes only the manifest-declared label", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    dsimaging.nfilter.subset = 3L
  ))
  metadata_path <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    sample_id = paste0("case00", 1:3),
    patient_id = paste0("patient00", 1:3),
    declared_label = c("a", "b", "a"),
    undeclared_outcome = c(1L, 0L, 1L),
    direct_identifier = c("name-a", "name-b", "name-c")),
    metadata_path, row.names = FALSE)
  assign_test_imaging_handle(metadata_path, label_col = "declared_label")

  feature_path <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    sample_id = paste0("case00", 1:3), feature = 1:3),
    feature_path, row.names = FALSE)
  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  asset_id <- dsImaging:::.asset_register(
    db, "lung", "feature_table", feature_path,
    visibility = "global", collection_seal = strrep("a", 64))

  loaded <- imagingLoadAssetDS("img", asset_id, include_metadata = TRUE)
  expect_named(loaded, c("sample_id", "feature", "declared_label"))
  columns <- c("sample_id", "undeclared_outcome")
  expect_error(imagingLoadAssetDS("img", asset_id,
    columns = columns,
    include_metadata = TRUE), "Requested columns not found")

  malicious_path <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    sample_id = paste0("case00", 1:3), feature = 1:3,
    undeclared_outcome = c(1L, 0L, 1L)),
    malicious_path, row.names = FALSE)
  malicious_id <- dsImaging:::.asset_register(
    db, "lung", "feature_table", malicious_path,
    visibility = "global", collection_seal = strrep("a", 64))
  expect_error(imagingLoadAssetDS("img", malicious_id),
               "undeclared clinical columns")

  forged_label_path <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    sample_id = paste0("case00", 1:3), feature = 1:3,
    declared_label = c("b", "b", "b")),
    forged_label_path, row.names = FALSE)
  forged_label_id <- dsImaging:::.asset_register(
    db, "lung", "feature_table", forged_label_path,
    visibility = "global", collection_seal = strrep("a", 64))
  expect_error(imagingLoadAssetDS("img", forged_label_id),
               "label does not match")
})

test_that("the analyst catalog discovers only global assets", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    dsimaging.nfilter.subset = 3L
  ))
  metadata_path <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    sample_id = paste0("case00", 1:3),
    patient_id = paste0("patient00", 1:3)),
    metadata_path, row.names = FALSE)
  assign_test_imaging_handle(metadata_path)

  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  private_id <- dsImaging:::.asset_register(
    db, "lung", "feature_table", "/private/table.csv")
  global_id <- dsImaging:::.asset_register(
    db, "lung", "feature_table", "/global/table.csv",
    visibility = "global", collection_seal = strrep("a", 64))

  catalog <- imagingAssetCatalogDS("img")
  expect_true(global_id %in% catalog$asset_id)
  expect_false(private_id %in% catalog$asset_id)
  expect_named(catalog, c("asset_id", "kind", "modality"))
})
