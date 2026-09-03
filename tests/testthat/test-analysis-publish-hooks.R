test_that("publisher provenance captures runner summary versions", {
  out <- tempfile("runner-output-")
  dir.create(out)
  writeLines(jsonlite::toJSON(list(
    n_samples = 1L,
    n_features = 107L,
    columns = paste0("f", seq_len(200)),
    versions = list(python = "3.11.14", radiomics = "v3.0.1")
  ), auto_unbox = TRUE, pretty = TRUE),
  file.path(out, "extraction_summary.json"))

  meta <- dsImaging:::.imaging_output_metadata(out)

  expect_equal(meta$summaries$extraction_summary$n_samples, 1L)
  expect_null(meta$summaries$extraction_summary$columns)
  expect_equal(meta$runner_versions$extraction_summary$radiomics, "v3.0.1")
})

test_that("feature publishers reject duplicate or partial collection rows", {
  root <- withr::local_tempdir()
  metadata_path <- file.path(root, "samples.csv")
  utils::write.csv(data.frame(
    sample_id = paste0("case00", 1:3),
    patient_id = paste0("patient00", 1:3)),
    metadata_path, row.names = FALSE)
  manifest <- test_privacy_manifest(metadata_path)
  admission <- dsImaging:::.imaging_privacy_admission(manifest)
  manifest$.dsimaging_privacy_roster <- admission$roster
  manifest$.dsimaging_collection_seal <- strrep("a", 64)
  manifest_path <- file.path(root, "manifest.yaml")
  yaml::write_yaml(manifest, manifest_path)
  context_id <- paste0("dsctx_", strrep("a", 64))

  testthat::local_mocked_bindings(
    resolve_dataset = function(dataset_id) list(
      manifest_uri = manifest_path,
      backend = storage_backend("file")),
    .package = "dsImaging"
  )

  output <- file.path(root, "output")
  dir.create(output)
  feature_path <- file.path(output, "features.csv")
  utils::write.csv(data.frame(
    sample_id = paste0("case00", 1:3), feature = 1:3),
    feature_path, row.names = FALSE)
  expect_invisible(dsImaging:::.assert_publishable_imaging_feature_asset(
    output, "feature_table", list(dataset_id = context_id)))

  utils::write.csv(data.frame(
    sample_id = c(paste0("case00", 1:3), "case001"), feature = 1:4),
    feature_path, row.names = FALSE)
  expect_error(dsImaging:::.assert_publishable_imaging_feature_asset(
    output, "feature_table", list(dataset_id = context_id)),
    "complete admitted collection")
})

test_that("non-tabular publishers require an exact confined sample map", {
  root <- withr::local_tempdir()
  metadata_path <- file.path(root, "samples.csv")
  ids <- paste0("case00", 1:3)
  utils::write.csv(data.frame(
    sample_id = ids, patient_id = paste0("patient00", 1:3)),
    metadata_path, row.names = FALSE)
  manifest <- test_privacy_manifest(metadata_path)
  admission <- dsImaging:::.imaging_privacy_admission(manifest)
  manifest$.dsimaging_privacy_roster <- admission$roster
  manifest$.dsimaging_collection_seal <- strrep("b", 64)
  manifest_path <- file.path(root, "manifest.yaml")
  yaml::write_yaml(manifest, manifest_path)
  context_id <- paste0("dsctx_", strrep("f", 64))
  testthat::local_mocked_bindings(
    resolve_dataset = function(dataset_id) list(
      manifest_uri = manifest_path,
      backend = storage_backend("file")),
    .package = "dsImaging")

  output <- file.path(root, "output")
  dir.create(output)
  files <- paste0("mask-", seq_along(ids), ".nii.gz")
  for (path in file.path(output, files)) writeBin(charToRaw("mask"), path)
  write_output_manifest <- function(samples) jsonlite::write_json(list(
    schema_version = 1L, artifact_type = "mask_root", samples = samples),
    file.path(output, "dsimaging_output_manifest.json"),
    auto_unbox = TRUE, pretty = TRUE)
  exact <- lapply(seq_along(ids), function(i) list(
    sample_id = ids[[i]], primary = files[[i]], files = list(files[[i]]),
    file_integrity = list(list(
      path = files[[i]],
      size = file.info(file.path(output, files[[i]]))$size,
      sha256 = digest::digest(file.path(output, files[[i]]),
                              algo = "sha256", file = TRUE)))))
  write_output_manifest(exact)
  expect_invisible(dsImaging:::.assert_publishable_imaging_feature_asset(
    output, "mask_root", list(dataset_id = context_id)))

  write_output_manifest(exact[-1L])
  expect_error(dsImaging:::.assert_publishable_imaging_feature_asset(
    output, "mask_root", list(dataset_id = context_id)),
    "complete admitted collection")

  duplicated <- exact
  duplicated[[2L]]$primary <- files[[1L]]
  duplicated[[2L]]$files <- list(files[[1L]])
  duplicated[[2L]]$file_integrity <- duplicated[[1L]]$file_integrity
  write_output_manifest(duplicated)
  expect_error(dsImaging:::.assert_publishable_imaging_feature_asset(
    output, "mask_root", list(dataset_id = context_id)),
    "more than once")

  write_output_manifest(exact)
  writeBin(charToRaw("changed"), file.path(output, files[[1L]]))
  expect_error(dsImaging:::.assert_publishable_imaging_feature_asset(
    output, "mask_root", list(dataset_id = context_id)),
    "integrity verification")
  writeBin(charToRaw("mask"), file.path(output, files[[1L]]))
  writeBin(charToRaw("unmapped"), file.path(output, "extra.nii.gz"))
  expect_error(dsImaging:::.assert_publishable_imaging_feature_asset(
    output, "mask_root", list(dataset_id = context_id)),
    "unmapped sample artifacts")
})

test_that("feature publishers reject ambiguous multiple output tables", {
  root <- withr::local_tempdir()
  metadata_path <- file.path(root, "samples.csv")
  ids <- paste0("case00", 1:3)
  utils::write.csv(data.frame(
    sample_id = ids, patient_id = paste0("patient00", 1:3)),
    metadata_path, row.names = FALSE)
  manifest <- test_privacy_manifest(metadata_path)
  admission <- dsImaging:::.imaging_privacy_admission(manifest)
  manifest$.dsimaging_privacy_roster <- admission$roster
  manifest$.dsimaging_collection_seal <- strrep("c", 64)
  manifest_path <- file.path(root, "manifest.yaml")
  yaml::write_yaml(manifest, manifest_path)
  context_id <- paste0("dsctx_", strrep("b", 64))
  testthat::local_mocked_bindings(
    resolve_dataset = function(dataset_id) list(
      manifest_uri = manifest_path,
      backend = storage_backend("file")),
    .package = "dsImaging")

  output <- file.path(root, "output")
  dir.create(output)
  table <- data.frame(sample_id = ids, feature = seq_along(ids))
  utils::write.csv(table, file.path(output, "features.csv"), row.names = FALSE)
  utils::write.csv(table, file.path(output, "other.csv"), row.names = FALSE)
  expect_error(dsImaging:::.assert_publishable_imaging_feature_asset(
    output, "feature_table", list(dataset_id = context_id)),
    "output is unavailable")
})

test_that("drip feed resolves only the context sealed into its generation", {
  generation_id <- paste0("gen_", strrep("a", 32))
  root <- withr::local_tempdir()
  images <- file.path(root, "images")
  masks <- file.path(root, "masks")
  dir.create(images)
  dir.create(masks)
  generation_spec <- list(
    segmenter = list(provider = "existing_mask_asset", mask_asset = "masks"),
    profile = list(name = "demo_ct_firstorder_v1", bin_width = 25),
    profile_signature = "sealed-profile",
    dataset_context = list(sealed = TRUE))
  generation <- list(
    generation_id = generation_id, dataset_id = "lung", state = "RUNNING",
    spec_json = as.character(jsonlite::toJSON(
      generation_spec, auto_unbox = TRUE)))
  used_generation_context <- FALSE

  testthat::local_mocked_bindings(
    get_generation = function(id) generation,
    .sync_active_jobs = function(id) invisible(NULL),
    requeue_stale_claimed_items = function(id) invisible(NULL),
    .imaging_max_inflight = function() 4L,
    .resolve_ds = function(...) stop("global registry was consulted"),
    .resolve_ds_from_generation = function(id, dataset_id = NULL) {
      used_generation_context <<- TRUE
      list(dataset_id = "lung", backend = storage_backend("file"),
        manifest = list(dataset_id = "lung", assets = list(
          images = list(uri = images), masks = list(uri = masks))))
    },
    claim_pending_items = function(...) character(0),
    .package = "dsImaging"
  )
  testthat::local_mocked_bindings(
    count_active_jobs = function(...) 0L,
    .package = "dsHPC"
  )

  expect_invisible(dsImaging:::.drip_feed_next_batch(generation_id, "lung"))
  expect_true(used_generation_context)
  expect_error(
    dsImaging:::.drip_feed_next_batch(generation_id, "other"),
    "does not authorize", fixed = TRUE)
})

test_that("generation contexts require exact dataset and privacy-roster binding", {
  generation_id <- paste0("gen_", strrep("d", 32))
  roster <- dsImaging:::.new_imaging_privacy_roster(
    c("scan1", "scan2", "scan3"), c("patient1", "patient2", "patient3"))
  make_generation <- function(context_name = "dataset_context",
                              manifest_dataset = "lung",
                              context_roster = roster) {
    context <- list(
      manifest = list(dataset_id = manifest_dataset,
        .dsimaging_privacy_roster = context_roster),
      backend = list(type = "file", config = list()),
      collection_seal = strrep("a", 64))
    spec <- list(privacy_roster = roster)
    spec[[context_name]] <- context
    list(generation_id = generation_id, dataset_id = "lung", state = "RUNNING",
      spec_json = as.character(jsonlite::toJSON(spec, auto_unbox = TRUE)))
  }
  generation <- make_generation()
  testthat::local_mocked_bindings(
    get_generation = function(id) generation,
    .imaging_privacy_admission = function(...) list(),
    .new_imaging_collection_snapshot = function(...) list(
      seal = strrep("a", 64)),
    .package = "dsImaging"
  )

  expect_equal(
    dsImaging:::.resolve_ds_from_generation(generation_id, "lung")$dataset_id,
    "lung")
  generation <- make_generation(manifest_dataset = "other")
  expect_null(dsImaging:::.resolve_ds_from_generation(generation_id, "lung"))
  altered_roster <- dsImaging:::.new_imaging_privacy_roster(
    c("scan1", "scan2", "scan3"), c("patient1", "patient1", "patient3"))
  generation <- make_generation(context_roster = altered_roster)
  expect_null(dsImaging:::.resolve_ds_from_generation(generation_id, "lung"))
  generation <- make_generation(context_name = "dataset")
  expect_null(dsImaging:::.resolve_ds_from_generation(generation_id, "lung"))
})

test_that("opaque job tags recover exactly one private sample", {
  generation_id <- paste0("gen_", strrep("a", 32))
  private_id <- "PHI_PATIENT_701_SCAN_A"
  token_a <- strrep("1", 32)
  token_b <- strrep("2", 32)
  candidates <- data.frame(
    generation_id = generation_id,
    sample_id = c(private_id, "scan-b"),
    job_token = c(token_a, token_b),
    stringsAsFactors = FALSE
  )
  tags <- dsImaging:::.per_image_job_tags(
    "dataset", token_a, generation_id)

  expect_false(any(grepl(private_id, tags, fixed = TRUE)))
  expect_identical(dsImaging:::.sample_id_from_tags(
    tags, candidates), private_id)
  expect_null(dsImaging:::.sample_id_from_tags(
    c("per_image", "dataset", strrep("3", 32), generation_id), candidates))
  expect_null(dsImaging:::.sample_id_from_tags(
    c("other", "dataset", token_a, generation_id), candidates))
})
