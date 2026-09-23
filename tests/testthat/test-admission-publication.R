admission_publication_fixture <- function(root) {
  ids <- paste0("scan-", 1:3)
  metadata_path <- file.path(root, "samples.csv")
  utils::write.csv(data.frame(sample_id = ids,
    patient_id = paste0("patient-", 1:3), diagnosis = c(0L, 1L, 0L)),
    metadata_path, row.names = FALSE)
  manifest <- test_privacy_manifest(metadata_path, label_col = "diagnosis")
  admission <- dsImaging:::.imaging_privacy_admission(manifest)
  manifest$.dsimaging_privacy_roster <- admission$roster
  manifest$.dsimaging_collection_seal <- strrep("a", 64)
  path <- file.path(root, "manifest.yaml")
  yaml::write_yaml(manifest, path)
  output <- file.path(root, "output")
  dir.create(output)
  list(ids = ids, metadata_path = metadata_path, manifest_path = path,
    output = output, context_id = paste0("dsctx_", strrep("a", 64)),
    authorized = list(dataset_id = "lung", manifest = manifest,
      backend = NULL, privacy = admission$contract,
      privacy_roster = admission$roster))
}

admission_output_sample <- function(root, sample_id, primary, files = primary) {
  list(sample_id = sample_id, primary = primary, files = as.list(files),
    file_integrity = lapply(files, function(path) list(path = path,
      size = file.info(file.path(root, path))$size,
      sha256 = digest::digest(file.path(root, path), algo = "sha256",
        file = TRUE))))
}

admission_write_output_map <- function(root, type, samples) {
  jsonlite::write_json(list(schema_version = 1L, artifact_type = type,
    samples = samples), file.path(root, "dsimaging_output_manifest.json"),
    auto_unbox = TRUE, null = "null")
}

admission_dose_rows <- function(ids) {
  data.frame(sample_id = rep(ids, each = 2),
    roi = rep(c("whole_grid", "mask"), length(ids)),
    dose_min = 1, dose_max = 3, dose_mean = 2, dose_std = 0.5,
    dose_voxels = 8L, n_beams = 1L, n_fraction_groups = 1L,
    n_fractions = 20L, stringsAsFactors = FALSE)
}

test_that("dose publication requires complete unique per-ROI sample mapping", {
  fixture <- admission_publication_fixture(withr::local_tempdir())
  testthat::local_mocked_bindings(resolve_dataset = function(dataset_id) {
    list(manifest_uri = fixture$manifest_path, backend = storage_backend("file"))
  }, .package = "dsImaging")
  path <- file.path(fixture$output, "rt_dose_metrics.csv")
  rows <- admission_dose_rows(fixture$ids)
  config <- list(dataset_id = fixture$context_id, mask_asset = "masks")
  validate <- function(data, cfg = config) {
    utils::write.csv(data, path, row.names = FALSE)
    dsImaging:::.assert_publishable_imaging_feature_asset(
      fixture$output, "dose_table", cfg, "rt_dose_plan")
  }
  expect_invisible(validate(rows))
  expect_error(validate(rows[-1, ]), "complete admitted collection")
  expect_error(validate(rbind(rows, rows[1, ])), "schema is unavailable")
  expect_error(validate(rows[-2, ]), "complete admitted collection")
  changed <- rows
  changed$sample_id[1] <- "other-sample"
  expect_error(validate(changed), "complete admitted collection")
  changed <- rows
  changed$roi[1] <- "/private/roi-name"
  expect_error(validate(changed), "schema is unavailable")
  changed <- rows
  changed$dose_mean[1] <- Inf
  expect_error(validate(changed), "schema is unavailable")
  changed <- rows
  changed$patient_id <- "patient-1"
  expect_error(validate(changed), "schema is unavailable")
  expect_error(validate(rows, list(dataset_id = fixture$context_id)),
    "ROI mapping is unavailable")
  expect_invisible(validate(rows[rows$roi == "whole_grid", ],
    list(dataset_id = fixture$context_id)))
  utils::write.csv(rows, path, row.names = FALSE)
  expect_error(dsImaging:::.assert_publishable_imaging_feature_asset(
    fixture$output, "feature_table", config), "complete admitted collection")

  metadata <- utils::read.csv(fixture$metadata_path)
  metadata$patient_id <- "one-patient"
  utils::write.csv(metadata, fixture$metadata_path, row.names = FALSE)
  expect_error(dsImaging:::.assert_dose_asset_privacy(rows,
    fixture$authorized), "privacy roster|fewer than 3")
})

test_that("dose rows are assigned privately and tile bodies never load as tables", {
  root <- withr::local_tempdir()
  fixture <- admission_publication_fixture(root)
  withr::local_options(list(dsimaging.asset_db = file.path(root, "assets.sqlite"),
    dsimaging.privacy_profile = "clinical_default"))
  assign_test_imaging_handle(fixture$metadata_path, label_col = "diagnosis")
  rows <- admission_dose_rows(fixture$ids)
  path <- file.path(fixture$output, "rt_dose_metrics.csv")
  utils::write.csv(rows, path, row.names = FALSE)
  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  dose_id <- dsImaging:::.asset_register(db, "lung", "dose_table", fixture$output,
    visibility = "global", collection_seal = strrep("a", 64),
    provenance = list(n_roi_rows = 6L, local_path = path))
  tile_id <- dsImaging:::.asset_register(db, "lung", "wsi_tile_root", fixture$output,
    visibility = "global", collection_seal = strrep("a", 64),
    provenance = list(n_tiles = 123L, per_slide = list("scan-1" = 17L),
      tile_file = "/private/tiles/body.png"))
  assigned <- imagingLoadAssetDS("img", dose_id, include_metadata = TRUE)
  expect_equal(nrow(assigned), 6L)
  expect_equal(assigned$diagnosis, rep(c(0L, 1L, 0L), each = 2L))
  expect_false("patient_id" %in% names(assigned))
  expect_error(imagingLoadAssetDS("img", tile_id), "not a loadable feature table")
  expect_error(imagingAssetDetailDS("img", dose_id), "not available")
  catalog <- imagingAssetCatalogDS("img")
  expect_named(catalog, c("asset_id", "kind", "modality"))
  expect_setequal(catalog$kind, c("dose_table", "wsi_tile_root"))
  public <- jsonlite::toJSON(list(catalog = catalog,
    metadata = imagingMetadataDS("img"), assets = imagingAssetsDS("img")),
    auto_unbox = TRUE)
  for (private in c("dose_mean", "whole_grid", "scan-1", "patient-1",
                    "n_tiles", "per_slide", "n_roi_rows", path, "body.png")) {
    expect_false(grepl(private, public, fixed = TRUE))
  }
  description <- read.dcf(system.file("DESCRIPTION", package = "dsImaging"))
  expect_match(description[1, "AssignMethods"], "imagingLoadAssetDS")
  expect_false(grepl("imagingLoadAssetDS|imagingAssetDetailDS",
    description[1, "AggregateMethods"]))
  utils::write.csv(rows[-1, ], path, row.names = FALSE)
  expect_error(imagingLoadAssetDS("img", dose_id), "complete admitted collection")
})

test_that("dose CSV loading preserves numeric-looking canonical identifiers", {
  path <- file.path(withr::local_tempdir(), "dose.csv")
  rows <- admission_dose_rows(c("001", "002", "003"))
  utils::write.csv(rows, path, row.names = FALSE)
  loaded <- dsImaging:::.read_feature_asset(
    list(path_or_root = path, kind = "dose_table"), "lung")
  expect_identical(loaded$sample_id, rows$sample_id)
})

test_that("DSLite assigns dose rows but refuses their aggregate extraction", {
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
  server_name <- paste0("dsimaging_dose_boundary_", Sys.getpid())
  assign(server_name, server, envir = .GlobalEnv)
  withr::defer(rm(list = server_name, envir = .GlobalEnv))
  connection <- DSI::dsConnect(DSLite::DSLite(), name = "site", url = server_name)
  withr::defer(DSI::dsDisconnect(connection))
  session <- server$getSession(connection@sid)
  assign_test_imaging_handle(fixture$metadata_path, label_col = "diagnosis", env = session)
  rows <- admission_dose_rows(fixture$ids)
  utils::write.csv(rows, file.path(fixture$output, "rt_dose_metrics.csv"), row.names = FALSE)
  db <- dsImaging:::.asset_db_connect()
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  asset_id <- dsImaging:::.asset_register(db, "lung", "dose_table", fixture$output,
    visibility = "global", collection_seal = strrep("a", 64))
  expression <- call("imagingLoadAssetDS", "img", asset_id)
  expect_no_error(DSI::dsAssignExpr(connection, "dose", expression, async = FALSE))
  expect_equal(server$getSessionData(connection@sid, "dose"), rows)
  expect_error(DSI::dsFetch(DSI::dsAggregate(connection, expression, async = FALSE)),
    "not allowed|does not allow|not defined")
  public <- DSI::dsFetch(DSI::dsAggregate(connection,
    call("imagingAssetCatalogDS", "img"), async = FALSE))
  expect_named(public, c("asset_id", "kind", "modality"))
  expect_false(any(c("roi", "dose_mean", "path", "n_rows") %in% names(public)))
})

test_that("WSI publication binds private slide manifests, counts and tile bodies", {
  fixture <- admission_publication_fixture(withr::local_tempdir())
  root <- fixture$output
  samples <- lapply(seq_along(fixture$ids), function(i) {
    tile <- paste0("tile-", i, ".png")
    manifest <- paste0("slide-", i, ".json")
    tiles <- if (i == 1L) list() else list(list(tile_file = tile,
      x = 0L, y = 0L, tile_size = 16L, tissue_fraction = 0.5))
    if (i != 1L) writeBin(charToRaw("synthetic-tile"), file.path(root, tile))
    jsonlite::write_json(list(sample_id = fixture$ids[[i]], n_tiles = length(tiles),
      tiles = tiles), file.path(root, manifest), auto_unbox = TRUE)
    admission_output_sample(root, fixture$ids[[i]], manifest,
      c(manifest, if (i != 1L) tile))
  })
  validate <- function(values = samples) {
    admission_write_output_map(root, "wsi_tile_root", values)
    dsImaging:::.assert_mapped_imaging_output(root, "wsi_tile_root",
      fixture$authorized$privacy_roster)
  }
  expect_invisible(validate())
  expect_invisible(dsImaging:::.assert_mapped_imaging_output(
    file.path(root, "."), "wsi_tile_root", fixture$authorized$privacy_roster))
  expect_error(validate(samples[-1]), "complete admitted collection")
  crossed <- samples
  crossed[[1]] <- samples[[2]]
  crossed[[1]]$sample_id <- fixture$ids[[1]]
  expect_error(validate(crossed), "tile mapping is invalid")
  writeBin(charToRaw("unmapped"), file.path(root, "unmapped.png"))
  expect_error(validate(), "unmapped sample artifacts")
  unlink(file.path(root, "unmapped.png"))
  writeBin(charToRaw("unmapped"), file.path(root, "unmapped.jpg"))
  expect_error(validate(), "unmapped sample artifacts")
  unlink(file.path(root, "unmapped.jpg"))
  writeBin(charToRaw("corrupt"), file.path(root, "tile-2.png"))
  expect_error(validate(), "integrity verification")
  writeBin(charToRaw("synthetic-tile"), file.path(root, "tile-2.png"))
  expect_true(file.symlink(file.path(root, "tile-2.png"),
    file.path(root, "linked.png")))
  expect_error(validate(), "symbolic link")
  unlink(file.path(root, "linked.png"))
  bad <- jsonlite::read_json(file.path(root, "slide-2.json"))
  bad$n_tiles <- 100L
  jsonlite::write_json(bad, file.path(root, "slide-2.json"), auto_unbox = TRUE)
  samples[[2]] <- admission_output_sample(root, fixture$ids[[2]], "slide-2.json",
    c("slide-2.json", "tile-2.png"))
  expect_error(validate(), "tile mapping is invalid")
})

test_that("QC publication retains the complete roster when capped", {
  fixture <- admission_publication_fixture(withr::local_tempdir())
  root <- fixture$output
  file <- "case_0001_0123456789ab.png"
  writeBin(charToRaw("thumbnail"), file.path(root, file))
  samples <- list(admission_output_sample(root, fixture$ids[[1]], file))
  samples <- c(samples, lapply(fixture$ids[-1], function(id) list(
    sample_id = id, status = "omitted_by_cap", primary = NULL,
    files = list(), file_integrity = list())))
  table <- data.frame(sample_id = fixture$ids[[1]],
    qc_id = sub("[.]png$", "", file), file = file, has_mask = FALSE,
    width_px = 192L)
  utils::write.csv(table, file.path(root, "qc_visual_manifest.csv"), row.names = FALSE)
  validate <- function(values = samples, cap = 1L) {
    admission_write_output_map(root, "qc_visual_asset", values)
    dsImaging:::.assert_mapped_imaging_output(root, "qc_visual_asset",
      fixture$authorized$privacy_roster, list(max_tiles = cap))
  }
  expect_invisible(validate())
  expect_invisible(validate(rev(samples)))
  expect_error(validate(cap = 2L), "cap mapping is invalid")
  expect_error(validate(samples[-3]), "complete admitted collection")
  changed <- samples
  changed[[2]]$status <- "failed"
  expect_error(validate(changed), "sample status is invalid")
  table$sample_id <- fixture$ids[[2]]
  utils::write.csv(table, file.path(root, "qc_visual_manifest.csv"), row.names = FALSE)
  expect_error(validate(), "no exact sample table")
})

test_that("WSI manifest-only publication keeps fan-out confined to private JSON", {
  fixture <- admission_publication_fixture(withr::local_tempdir())
  root <- fixture$output
  samples <- lapply(seq_along(fixture$ids), function(i) {
    manifest <- paste0("slide-", i, ".json")
    jsonlite::write_json(list(sample_id = fixture$ids[[i]], n_tiles = 1L,
      tiles = list(list(tile_file = "", x = 0L, y = 0L, tile_size = 16L,
        tissue_fraction = 0.5))), file.path(root, manifest), auto_unbox = TRUE)
    admission_output_sample(root, fixture$ids[[i]], manifest)
  })
  admission_write_output_map(root, "wsi_tile_root", samples)
  expect_invisible(dsImaging:::.assert_mapped_imaging_output(root, "wsi_tile_root",
    fixture$authorized$privacy_roster, list(write_tiles = FALSE)))
  expect_error(dsImaging:::.assert_mapped_imaging_output(root, "wsi_tile_root",
    fixture$authorized$privacy_roster), "tile mapping is invalid")
  jsonlite::write_json(list(n_tiles = 1L,
    tiles = list(list(tile_file = "/private/tile-body.png"))),
    file.path(root, "wsi_tiling_summary.json"), auto_unbox = TRUE)
  provenance <- dsImaging:::.imaging_output_metadata(root)
  expect_equal(provenance$summaries$wsi_tiling_summary$n_tiles, 1L)
  expect_null(provenance$summaries$wsi_tiling_summary$tiles)
})

test_that("RT and MONAI publishers require exactly one mask for each sample", {
  fixture <- admission_publication_fixture(withr::local_tempdir())
  root <- fixture$output
  samples <- lapply(seq_along(fixture$ids), function(i) {
    files <- paste0("mask-", i, "-", 1:2, ".nii.gz")
    for (file in files) writeBin(charToRaw("mask"), file.path(root, file))
    admission_output_sample(root, fixture$ids[[i]], files[[1]], files)
  })
  admission_write_output_map(root, "mask_root", samples)
  for (runner in c("rt_convert", "monai_bundle_infer")) {
    expect_error(dsImaging:::.assert_mapped_imaging_output(root, "mask_root",
      fixture$authorized$privacy_roster, runner = runner), "one mask per sample")
  }
  expect_invisible(dsImaging:::.assert_mapped_imaging_output(root, "mask_root",
    fixture$authorized$privacy_roster, runner = "totalsegmentator_infer"))
  writeBin(charToRaw("extra mask"), file.path(root, ".unmapped.nii.gz"))
  expect_error(dsImaging:::.assert_mapped_imaging_output(root, "mask_root",
    fixture$authorized$privacy_roster, runner = "totalsegmentator_infer"),
    "unmapped sample artifacts")
})
