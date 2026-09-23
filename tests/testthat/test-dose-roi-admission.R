dose_roi_config <- function() {
  list(mask_asset = "masks", roi_labels = "tumour,organ_at_risk,absent",
    mask_labels = "1,2,7")
}

dose_roi_rows <- function(ids) {
  rows <- data.frame(sample_id = rep(ids, each = 3L),
    roi_label = rep(c("tumour", "organ_at_risk", "absent"), length(ids)),
    dose_min = 1, dose_max = 3, dose_mean = 2, dose_std = 0.5,
    dose_voxels = 8, n_beams = 1, n_fraction_groups = 1,
    n_fractions = 20, stringsAsFactors = FALSE)
  missing <- rows$roi_label == "absent"
  rows[missing, c("dose_min", "dose_max", "dose_mean", "dose_std")] <- NA_real_
  rows$dose_voxels[missing] <- 0
  rows
}

test_that("RT config admits public segment and dose selections without ambiguity", {
  config <- dsImaging:::.imaging_runner_config
  expect_identical(config("rt_convert", list(rt_asset = "rt_seg",
    segment_numbers = c(1, 3)))$segment_numbers, "1,3")
  expect_error(config("rt_convert", list(rois = "tumour", segment_numbers = 1)),
    "mutually exclusive")
  expect_error(config("rt_convert", list(segment_numbers = c(1, 1))), "unique")
  expect_error(config("rt_convert", list(rois = "tumour,tumour")), "unique")
  for (value in list(0, 65536, NA, "1,", "1,,2", 1.5, "/private/seg")) {
    expect_error(config("rt_convert", list(segment_numbers = value)))
  }
  base <- dose_roi_config()
  expect_identical(config("rt_dose_plan", base)$roi_labels, base$roi_labels)
  masks <- list(mask_assets = c("tumour_masks", "oar_masks"),
    roi_labels = c("tumour", "organ_at_risk"), mask_labels = c(1, 1))
  expected <- config("rt_dose_plan", masks)
  expect_identical(expected$mask_labels, "1,1")
  expect_identical(expected$mask_assets, "tumour_masks,oar_masks")
  expect_setequal(dsImaging:::.imaging_collect_resource_names(list(config = expected)),
    c("rt_dose", "rt_plan", "tumour_masks", "oar_masks"))
  invalid <- list(
    modifyList(base, list(roi_labels = "tumour,tumour,absent")),
    modifyList(base, list(mask_labels = "1,1,7")),
    modifyList(base, list(mask_labels = "1,2")),
    modifyList(base, list(mask_labels = "1,2,0")),
    modifyList(base, list(mask_labels = "1,2,2147483648")),
    modifyList(base, list(roi_labels = "tumour,/private/path,absent")),
    modifyList(base, list(roi_labels = "tumour,organ_at_risk,")),
    modifyList(base, list(roi_labels = NULL)),
    modifyList(base, list(mask_asset = NULL)),
    c(base, list(mask_assets = "masks,masks,masks")),
    modifyList(masks, list(mask_assets = "tumour_masks")),
    modifyList(masks, list(mask_assets = "tumour_masks,../private")))
  for (value in invalid) expect_error(config("rt_dose_plan", value))
})

test_that("labelled dose publication binds a fixed public cross-product", {
  fixture <- admission_publication_fixture(withr::local_tempdir())
  testthat::local_mocked_bindings(resolve_dataset = function(dataset_id) {
    list(manifest_uri = fixture$manifest_path, backend = storage_backend("file"))
  }, .package = "dsImaging")
  rows <- dose_roi_rows(fixture$ids)
  config <- c(list(dataset_id = fixture$context_id), dose_roi_config())
  validate <- function(data, cfg = config) {
    utils::write.csv(data, file.path(fixture$output, "rt_dose_metrics.csv"), row.names = FALSE, na = "")
    dsImaging:::.assert_publishable_imaging_feature_asset(
      fixture$output, "dose_table", cfg, "rt_dose_plan")
  }
  expect_invisible(validate(rows))
  literal_na <- rows
  literal_na$roi_label[literal_na$roi_label == "tumour"] <- "NA"
  expect_invisible(validate(literal_na,
    modifyList(config, list(roi_labels = "NA,organ_at_risk,absent"))))
  absent <- rows
  absent[, c("dose_min", "dose_max", "dose_mean", "dose_std")] <- NA_real_
  absent$dose_voxels <- 0
  expect_invisible(validate(absent))
  expect_error(validate(rows, list(dataset_id = fixture$context_id)), "schema")
  expect_error(validate(rows[-1, ]), "complete admitted collection")
  expect_error(validate(rows[rows$roi_label != "absent", ]), "complete admitted collection")
  expect_error(validate(rbind(rows, rows[1, ])), "schema")
  changed <- rows
  changed$roi_label[1] <- "private-discovered-label"
  expect_error(validate(changed), "schema")
  changed <- rows
  changed$sample_id[1] <- "wrong-patient-sample"
  expect_error(validate(changed), "complete admitted collection")
  for (field in c("dose_min", "dose_max", "dose_mean", "dose_std")) {
    changed <- rows
    changed[[field]][3] <- 0
    expect_error(validate(changed), "schema")
    changed <- rows
    changed[[field]][1] <- NA_real_
    expect_error(validate(changed), "schema")
  }
  changed <- rows
  changed$dose_voxels[1] <- 0.5
  expect_error(validate(changed), "values")
  changed <- rows
  changed$dose_mean[1] <- 4
  expect_error(validate(changed), "values")
  changed <- rows
  changed$patient_id <- "private"
  expect_error(validate(changed), "schema")
  expect_error(validate(rows, modifyList(config, list(roi_labels = "tumour,oar,absent"))),
    "schema")
  withr::local_options(list(nfilter.subset = 4))
  expect_error(validate(rows), "privacy|patient|minimum|threshold")
})

test_that("DSLite ASSIGN retains labelled dose missing rows and checks public provenance", {
  skip_if_not_installed("DSLite")
  skip_if_not_installed("DSI")
  root <- withr::local_tempdir()
  fixture <- admission_publication_fixture(root)
  withr::local_options(list(dsimaging.asset_db = file.path(root, "assets.sqlite")))
  server <- DSLite::newDSLiteServer(config = list(), strict = TRUE)
  description <- packageDescription("dsImaging")
  for (method in trimws(strsplit(description$AssignMethods, ",", fixed = TRUE)[[1]])) {
    server$assignMethod(method, paste0("dsImaging::", method))
  }
  for (method in trimws(strsplit(description$AggregateMethods, ",", fixed = TRUE)[[1]])) {
    server$aggregateMethod(method, paste0("dsImaging::", method))
  }
  server_name <- paste0("dsimaging_labelled_dose_", Sys.getpid())
  assign(server_name, server, envir = .GlobalEnv)
  withr::defer(rm(list = server_name, envir = .GlobalEnv))
  connection <- DSI::dsConnect(DSLite::DSLite(), name = "site", url = server_name)
  withr::defer(DSI::dsDisconnect(connection))
  session <- server$getSession(connection@sid)
  assign_test_imaging_handle(fixture$metadata_path, label_col = "diagnosis", env = session)
  rows <- dose_roi_rows(fixture$ids)
  path <- file.path(fixture$output, "rt_dose_metrics.csv")
  utils::write.csv(rows, path, row.names = FALSE, na = "")
  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  register <- function(provenance) dsImaging:::.asset_register(db, "lung", "dose_table",
    fixture$output, visibility = "global", collection_seal = strrep("a", 64),
    provenance = provenance)
  asset_id <- register(list(runner = "rt_dose_plan", config = dose_roi_config()))
  expression <- call("imagingLoadAssetDS", "img", asset_id, include_metadata = TRUE)
  expect_no_error(DSI::dsAssignExpr(connection, "dose", expression, async = FALSE))
  expected <- rows
  expected$diagnosis <- rep(c(0L, 1L, 0L), each = 3L)
  expect_equal(server$getSessionData(connection@sid, "dose"), expected)
  expect_false("patient_id" %in% names(server$getSessionData(connection@sid, "dose")))
  expect_error(DSI::dsFetch(DSI::dsAggregate(connection, expression, async = FALSE)),
    "not allowed|does not allow|not defined")
  public <- DSI::dsFetch(DSI::dsAggregate(connection,
    call("imagingAssetCatalogDS", "img"), async = FALSE))
  expect_named(public, c("asset_id", "kind", "modality"))
  encoded <- jsonlite::toJSON(public)
  for (value in c("roi_label", "tumour", "absent", "dose_mean", "scan-1", path)) {
    expect_false(grepl(value, encoded, fixed = TRUE))
  }
  undeclared <- register(NULL)
  expect_error(DSI::dsAssignExpr(connection, "bad_dose",
    call("imagingLoadAssetDS", "img", undeclared), async = FALSE), "schema")
  changed <- rows
  changed$roi_label[1] <- "private_label"
  utils::write.csv(changed, path, row.names = FALSE, na = "")
  expect_error(DSI::dsAssignExpr(connection, "bad_dose", expression, async = FALSE), "schema")
})
