radiomics_export_fixture <- function(env) {
  rows <- data.frame(sample_id = c("scan-z", "scan-a", "scan-2", "scan-10"),
    patient_id = c("p1", "p1", "p2", "p3"),
    diagnosis = c("case", "case", "control", "control"),
    stringsAsFactors = FALSE)
  metadata <- tempfile(fileext = ".csv")
  utils::write.csv(rows, metadata, row.names = FALSE)
  assign_test_imaging_handle(metadata, env = env, label_col = "diagnosis")
  features <- tempfile(fileext = ".csv")
  utils::write.csv(data.frame(sample_id = rows$sample_id,
    radiomics_mean = c(1.5, 3.5, 2.5, 4.5)), features, row.names = FALSE)
  db <- dsImaging:::.asset_db_connect()
  asset <- dsImaging:::.asset_register(db, "lung", "feature_table", features,
    visibility = "global", collection_seal = strrep("a", 64))
  dsImaging:::.asset_db_close(db)
  assign("asset_id", asset, env)
  exported <- evalq(dsImaging::imagingLoadAssetDS(
    "img", asset_id, include_metadata = TRUE), env)
  assign("rad", exported, env)
  list(rows = rows, raw = exported, metadata = metadata,
       authorized = dsImaging:::.authorized_imaging_dataset("img", owner_env = env))
}

local_export_policy <- function(.local_envir = parent.frame()) {
  withr::local_options(list(dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.nfilter.subset = 3L, nfilter.subset = 3L,
    default.nfilter.subset = 3L), .local_envir = .local_envir)
}

test_that("complete exported frames and Arrow containers ignore only row order", {
  local_export_policy()
  env <- new.env(parent = globalenv())
  fixture <- radiomics_export_fixture(env)
  resolve <- function(symbol = "rad", capability = NULL) {
    dsImaging:::.resolve_imaging_feature_view_for_consumer(symbol, capability, env)
  }
  first <- resolve()
  expected_ids <- sort(fixture$rows$sample_id, method = "radix")
  expect_identical(first$data$sample_id, expected_ids)
  expect_identical(first$data$patient_id,
    fixture$rows$patient_id[match(expected_ids, fixture$rows$sample_id)])
  expect_identical(first$privacy_roster$sample_count, 4L)
  expect_identical(first$privacy_roster$privacy_unit_count, 3L)
  reversed <- fixture$raw[c(4, 2, 1, 3), , drop = FALSE]
  rownames(reversed) <- c("arbitrary", "row", "names", "ignored")
  path <- tempfile(fileext = ".parquet")
  arrow::write_parquet(reversed, path)
  containers <- list(reversed, arrow::Table$create(reversed),
    arrow::RecordBatch$create(reversed),
    arrow::read_parquet(path, as_data_frame = FALSE))
  for (candidate in containers) {
    assign("renamed", candidate, env)
    current <- resolve("renamed")
    expect_identical(current$data, first$data)
    expect_identical(current$feature_view_capability, first$feature_view_capability)
    expect_identical(resolve("renamed", first$feature_view_capability), current)
  }
  state <- dsImaging:::.imaging_session_state(env, create = FALSE)
  before <- ls(state$feature_views, all.names = TRUE)
  registered <- dsImaging:::.register_imaging_feature_table_export(
    reversed, fixture$authorized, "img", env)
  expect_identical(registered, first$feature_view_capability)
  expect_identical(ls(state$feature_views, all.names = TRUE), before)
  expect_identical(get("rad", env), fixture$raw)
})

test_that("export order normalization rejects subsets duplicates and changed cells", {
  local_export_policy()
  env <- new.env(parent = globalenv())
  fixture <- radiomics_export_fixture(env)
  raw <- fixture$raw
  value <- raw; value$radiomics_mean[[1L]] <- value$radiomics_mean[[1L]] + 1
  label <- raw; label$diagnosis[[1L]] <- "other"
  key <- raw; key$sample_id[[1L]] <- "unadmitted-sample"
  key_alias <- raw; key_alias$sample_id[[1L]] <- paste0(" ", key_alias$sample_id[[1L]])
  type <- raw; type$radiomics_mean <- as.character(type$radiomics_mean)
  missing <- raw; missing$sample_id[[1L]] <- NA_character_
  variants <- list(subset = raw[-1L, ], duplicate = raw[c(1, 1, 3, 4), ],
    added = rbind(raw, raw[1L, ]), missing = missing, value = value, label = label,
    key = key, key_alias = key_alias, type = type,
    missing_key = raw[setdiff(names(raw), "sample_id")],
    missing_label = raw[setdiff(names(raw), "diagnosis")],
    columns = raw[rev(names(raw))])
  for (name in names(variants)) {
    for (arrow in c(FALSE, TRUE)) {
      candidate <- variants[[name]]
      if (arrow) candidate <- arrow::Table$create(candidate)
      assign("changed", candidate, env)
      expect_error(dsImaging:::.resolve_imaging_feature_view_for_consumer(
        "changed", owner_env = env), "Unknown, stale, or cross-session", info = name)
    }
  }
  other <- new.env(parent = globalenv())
  assign("rad", raw[nrow(raw):1L, ], other)
  expect_error(dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "rad", owner_env = other), "Unknown, stale, or cross-session")
})

test_that("row permutations cannot disambiguate different source authorities", {
  local_export_policy()
  env <- new.env(parent = globalenv())
  fixture <- radiomics_export_fixture(env)
  first <- dsImaging:::.resolve_imaging_feature_view_for_consumer("rad", owner_env = env)
  assign_test_imaging_handle(fixture$metadata, symbol = "img2", env = env,
                            label_col = "diagnosis")
  second <- dsImaging:::.authorized_imaging_dataset("img2", owner_env = env)
  dsImaging:::.register_imaging_feature_table_export(
    fixture$raw[4:1, ], second, "img2", env)
  for (candidate in list(fixture$raw, fixture$raw[4:1, ],
                         arrow::Table$create(fixture$raw[4:1, ]))) {
    assign("ambiguous", candidate, env)
    expect_error(dsImaging:::.resolve_imaging_feature_view_for_consumer(
      "ambiguous", owner_env = env), "Unknown, stale, or cross-session")
    pinned <- dsImaging:::.resolve_imaging_feature_view_for_consumer(
      "ambiguous", first$feature_view_capability, env)
    expect_identical(pinned$data, first$data)
  }
  evalq(dsImaging::imagingDestroyDS("img"), env)
  expect_error(dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "rad", first$feature_view_capability, env), "Unknown, stale, or cross-session")
})

test_that("canonical export lookup preserves source roster and private-state guards", {
  local_export_policy()
  env <- new.env(parent = globalenv())
  fixture <- radiomics_export_fixture(env)
  first <- dsImaging:::.resolve_imaging_feature_view_for_consumer("rad", owner_env = env)
  state <- dsImaging:::.imaging_session_state(env, create = FALSE)
  views <- state$feature_views
  cap <- first$feature_view_capability
  original <- views[[cap]]
  changed <- original; changed$data$radiomics_mean[[1L]] <- -10
  seal <- original; seal$collection_seal <- strrep("b", 64)
  roster <- original
  roster$privacy_roster <- dsImaging:::.new_imaging_privacy_roster(
    original$privacy_roster$sample_ids, rev(original$privacy_roster$privacy_ids))
  mapping <- original; mapping$data$patient_id <- rev(mapping$data$patient_id)
  mapping$data_sha256 <- digest::digest(mapping$data, algo = "sha256", serialize = TRUE)
  for (entry in list(changed, seal, roster, mapping)) {
    views[[cap]] <- entry
    expect_error(dsImaging:::.resolve_imaging_feature_view_for_consumer(
      "rad", cap, env), "Unknown, stale, or cross-session")
  }
  views[[cap]] <- original
  expect_identical(dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "rad", cap, env)$data, first$data)
})

test_that("caller attributes cannot authorize changed tables or cross-session references", {
  local_export_policy()
  env <- new.env(parent = globalenv())
  fixture <- radiomics_export_fixture(env)
  first <- dsImaging:::.resolve_imaging_feature_view_for_consumer("rad", owner_env = env)
  modified <- fixture$raw
  modified$radiomics_mean[[1L]] <- modified$radiomics_mean[[1L]] + 1
  attr(modified, "feature_view_capability") <- first$feature_view_capability
  attr(modified, "privacy_roster") <- first$privacy_roster
  attr(modified, "collection_seal") <- first$collection_seal
  class(modified) <- c("dsimaging_feature_view_ref", "data.frame")
  assign("forged", modified, env)
  expect_error(dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "forged", first$feature_view_capability, env), "Unknown, stale, or cross-session")
  expect_identical(dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "rad", owner_env = env)$data, first$data)

  reference <- structure(list(capability = first$feature_view_capability),
                         class = "dsimaging_feature_view_ref")
  other <- new.env(parent = globalenv())
  assign("copied", reference, other)
  expect_error(dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "copied", owner_env = other), "Unknown, stale, or cross-session")
})

test_that("active export bindings and rebound source handles cannot regain authority", {
  local_export_policy()
  env <- new.env(parent = globalenv())
  fixture <- radiomics_export_fixture(env)
  first <- dsImaging:::.resolve_imaging_feature_view_for_consumer("rad", owner_env = env)
  touched <- FALSE
  makeActiveBinding("active", function(value) {
    touched <<- TRUE
    fixture$raw
  }, env)
  expect_error(dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "active", owner_env = env), "Unknown, stale, or cross-session")
  expect_false(touched)

  assign_test_imaging_handle(fixture$metadata, symbol = "replacement", env = env,
                            label_col = "diagnosis")
  assign("img", get("replacement", env), env)
  expect_error(dsImaging:::.resolve_imaging_feature_view_for_consumer(
    "rad", first$feature_view_capability, env), "Unknown, stale, or cross-session")
})
