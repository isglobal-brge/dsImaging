test_that("image resolution uses only the exact snapshot record", {
  root <- withr::local_tempdir()
  collection <- write_test_imaging_collection(
    root, "exact.images",
    data.frame(sample_id = c("S1", "S2", "S10"),
      patient_id = c("P1", "P2", "P10"), stringsAsFactors = FALSE))
  backend <- storage_backend("file")
  admission <- dsImaging:::.imaging_privacy_admission(
    collection$manifest, backend)
  snapshot <- dsImaging:::.new_imaging_collection_snapshot(
    collection$manifest, backend, admission)

  s1 <- file.path(collection$image_root, "S1.png")
  s10 <- file.path(collection$image_root, "S10.png")
  expect_identical(dsImaging:::.resolve_sample_image(
    collection$image_root, "S1", backend, snapshot),
    s1)
  expect_identical(dsImaging:::.resolve_sample_image(
    collection$image_root, "S10", backend, snapshot),
    s10)

  alternative <- file.path(collection$image_root, "S1.nii.gz")
  file.create(alternative)
  unlink(s1)
  expect_error(dsImaging:::.resolve_sample_image(
    collection$image_root, "S1", backend, snapshot), "integrity")
})

test_that("image resolution rejects cross-root records and symbolic links", {
  root <- withr::local_tempdir()
  collection <- write_test_imaging_collection(
    root, "confined.images",
    data.frame(sample_id = c("S1", "S2", "S10"),
      patient_id = c("P1", "P2", "P10"), stringsAsFactors = FALSE))
  backend <- storage_backend("file")
  admission <- dsImaging:::.imaging_privacy_admission(
    collection$manifest, backend)
  snapshot <- dsImaging:::.new_imaging_collection_snapshot(
    collection$manifest, backend, admission)

  outside_root <- withr::local_tempdir()
  outside <- file.path(outside_root, "S1.png")
  file.create(outside)
  redirected <- snapshot
  redirected$records[[1L]]$uri <- outside
  expect_error(dsImaging:::.resolve_sample_image(
    collection$image_root, "S1", backend, redirected), "integrity")

  pinned <- file.path(collection$image_root, "S1.png")
  unlink(pinned)
  linked <- suppressWarnings(file.symlink(outside, pinned))
  if (!isTRUE(linked)) skip("Symbolic links are unavailable on this platform")
  expect_error(dsImaging:::.resolve_sample_image(
    collection$image_root, "S1", backend, snapshot), "integrity")
})

test_that("image snapshot records are unique and bytes remain pinned", {
  root <- withr::local_tempdir()
  collection <- write_test_imaging_collection(
    root, "sealed.images",
    data.frame(sample_id = c("S1", "S2", "S10"),
      patient_id = c("P1", "P2", "P10"), stringsAsFactors = FALSE))
  backend <- storage_backend("file")
  admission <- dsImaging:::.imaging_privacy_admission(
    collection$manifest, backend)
  snapshot <- dsImaging:::.new_imaging_collection_snapshot(
    collection$manifest, backend, admission)

  duplicated <- snapshot
  duplicated$records[[length(duplicated$records) + 1L]] <-
    duplicated$records[[1L]]
  expect_error(dsImaging:::.resolve_sample_image(
    collection$image_root, "S1", backend, duplicated), "integrity")

  pinned <- file.path(collection$image_root, "S1.png")
  size <- file.info(pinned)$size
  writeBin(charToRaw(strrep("x", size)), pinned)
  uri <- dsImaging:::.resolve_sample_image(
    collection$image_root, "S1", backend, snapshot)
  expect_error(dsImaging:::.stage_image_for_job(
    uri, "S1", "sealed.images", backend,
    image_root = collection$image_root, snapshot = snapshot), "integrity")
})

test_that("collection scans use snapshot IDs and persist the execution unit", {
  root <- withr::local_tempdir()
  dataset_id <- "opaque.image.names"
  ids <- c("sample-A", "sample-B", "sample-C")
  collection <- write_test_imaging_collection(
    root, dataset_id,
    data.frame(sample_id = ids, patient_id = paste0("patient-", 1:3),
      stringsAsFactors = FALSE))

  sample_manifest_path <- collection$manifest$sample_manifests$uri
  index_path <- collection$manifest$content_hash_index$uri
  sample_manifest <- utils::read.csv(sample_manifest_path,
    stringsAsFactors = FALSE, check.names = FALSE)
  index <- utils::read.csv(index_path, stringsAsFactors = FALSE,
    check.names = FALSE)
  opaque_names <- paste0("object-", seq_along(ids), ".png")
  for (i in seq_along(ids)) {
    target <- file.path(collection$image_root, opaque_names[[i]])
    expect_true(file.rename(index$uri[[i]], target))
    index$uri[[i]] <- target
    sample_manifest$primary_uri[[i]] <- opaque_names[[i]]
    sample_manifest$files_json[[i]] <- jsonlite::toJSON(
      list(list(path = opaque_names[[i]], role = "primary")),
      auto_unbox = TRUE)
  }
  utils::write.csv(sample_manifest, sample_manifest_path, row.names = FALSE)
  utils::write.csv(index, index_path, row.names = FALSE)

  backend <- storage_backend("file")
  admission <- dsImaging:::.imaging_privacy_admission(
    collection$manifest, backend)
  snapshot <- dsImaging:::.new_imaging_collection_snapshot(
    collection$manifest, backend, admission)
  withr::local_options(list(
    dsimaging.asset_db = tempfile(fileext = ".sqlite"),
    dsimaging.analysis.home = file.path(root, "analysis"),
    dsimaging.nfilter.subset = 3L))
  unit_selection <- list(
    schema_version = 1L, source = "resource", unit_id = "unit_alpha",
    resource_pool_id = "unit_alpha", type = "external",
    config_seal = strrep("b", 64), config = list(),
    allowed_labels = list("dsImaging"), allowed_runners = list())
  scan <- dsImaging:::.imaging_radiomics_scan_collection(
    dataset_id,
    list(provider = "ct_lung_threshold"),
    list(name = "demo_ct_firstorder_v1", bin_width = 25),
    "private",
    resolved_override = list(
      manifest = collection$manifest,
      backend = backend,
      collection_snapshot = snapshot),
    unit_selection = unit_selection)

  expect_identical(scan$pending_ids, sort(ids, method = "radix"))
  expect_setequal(names(scan$content_hashes), ids)
  expect_false(any(opaque_names %in% scan$pending_ids))
  generation <- get_generation(scan$generation_id)
  contract <- jsonlite::fromJSON(generation$spec_json,
    simplifyVector = FALSE)
  expect_identical(contract$dshpc_unit, unit_selection)

  submitted_units <- list()
  testthat::local_mocked_bindings(
    hpcSubmitInternal = function(spec_encoded, session_env = NULL,
                                 unit_selection = NULL) {
      submitted_units[[length(submitted_units) + 1L]] <<- unit_selection
      list(job_id = paste0("job_", length(submitted_units)))
    },
    count_active_jobs = function(...) 0L,
    .package = "dsHPC")
  testthat::local_mocked_bindings(
    .sync_active_jobs = function(...) invisible(NULL),
    requeue_stale_claimed_items = function(...) invisible(NULL),
    .package = "dsImaging")

  first <- scan$pending_ids[[1L]]
  initial <- dsImaging:::.imaging_radiomics_submit_batch(
    scan$generation_id, first, dataset_id, list(),
    scan$content_hashes[first])
  expect_identical(initial$submitted, 1L)
  expect_invisible(dsImaging:::.drip_feed_next_batch(
    scan$generation_id, dataset_id))
  expect_length(submitted_units, length(ids))
  expect_true(all(vapply(submitted_units, identical, logical(1),
    unit_selection)))
})

test_that("collection derivations bind hashes to their exact sample IDs", {
  left <- dsImaging:::.ordered_sample_hashes(c(
    sample.A = strrep("a", 64), sample.B = strrep("b", 64)))
  swapped <- dsImaging:::.ordered_sample_hashes(c(
    sample.A = strrep("b", 64), sample.B = strrep("a", 64)))
  reordered <- dsImaging:::.ordered_sample_hashes(c(
    sample.B = strrep("b", 64), sample.A = strrep("a", 64)))

  expect_identical(left, reordered)
  expect_false(identical(
    compute_derivation_hash(image_hashes = left),
    compute_derivation_hash(image_hashes = swapped)))
})

test_that("per-image derivations bind reusable artifacts to an execution unit", {
  base <- list(source = "resource", type = "external",
    resource_pool_id = "cluster", config_seal = strrep("a", 64))
  changed <- base
  changed$config_seal <- strrep("b", 64)
  hash <- function(unit) compute_image_derivation_hash(
    content_hash = strrep("c", 64), processor = "radiomics",
    params = list(execution_unit = unit))

  expect_false(identical(hash(base), hash(changed)))
})

test_that("generation profile and mask inputs cannot change between batches", {
  profile <- list(name = "demo_ct_firstorder_v1", bin_width = 25L)
  signature <- dsImaging:::.radiomics_profile_signature(profile)
  hashes <- list(
    sample.B = strrep("b", 64), sample.A = strrep("a", 64))
  spec <- list(profile_signature = signature,
    mask_hashes = dsImaging:::.ordered_sample_hashes(hashes))

  expect_identical(dsImaging:::.generation_profile_signature(spec, profile),
    signature)
  expect_identical(dsImaging:::.generation_mask_hashes(spec, rev(hashes)),
    spec$mask_hashes)

  changed_profile <- profile
  changed_profile$bin_width <- 50L
  expect_error(dsImaging:::.generation_profile_signature(
    spec, changed_profile), "no longer matches")
  changed_hashes <- hashes
  changed_hashes$sample.A <- strrep("c", 64)
  expect_error(dsImaging:::.generation_mask_hashes(
    spec, changed_hashes), "no longer match")
})

test_that("generation resolution revalidates the pinned collection seal", {
  root <- withr::local_tempdir()
  collection <- write_test_imaging_collection(
    root, "sealed.generation",
    data.frame(sample_id = c("S1", "S2", "S10"),
      patient_id = c("P1", "P2", "P10"), stringsAsFactors = FALSE))
  backend <- storage_backend("file")
  admission <- dsImaging:::.imaging_privacy_admission(
    collection$manifest, backend)
  snapshot <- dsImaging:::.new_imaging_collection_snapshot(
    collection$manifest, backend, admission)
  resolved <- list(backend = backend, collection_snapshot = snapshot)
  context <- dsImaging:::.dataset_context_from_resolved(
    resolved, collection$manifest, admission$roster)
  generation_id <- paste0("gen_", strrep("a", 32))
  generation <- list(
    generation_id = generation_id, dataset_id = "sealed.generation",
    spec_json = as.character(jsonlite::toJSON(list(
      privacy_roster = admission$roster, dataset_context = context),
      auto_unbox = TRUE)))
  testthat::local_mocked_bindings(
    get_generation = function(id) generation,
    .package = "dsImaging")

  current <- dsImaging:::.resolve_ds_from_generation(
    generation_id, "sealed.generation")
  expect_identical(current$collection_snapshot$seal, snapshot$seal)

  index_path <- collection$manifest$content_hash_index$uri
  index <- utils::read.csv(index_path, stringsAsFactors = FALSE)
  index$size[[1L]] <- index$size[[1L]] + 1L
  utils::write.csv(index, index_path, row.names = FALSE)
  expect_null(dsImaging:::.resolve_ds_from_generation(
    generation_id, "sealed.generation"))
})

test_that("artifact mask fallback uses exact stems with strict cardinality", {
  artifact <- withr::local_tempdir()
  s10 <- file.path(artifact, "S10_mask.nii.gz")
  file.create(s10)
  expect_null(dsImaging:::.find_mask_in_artifact(artifact, "S1"))

  ignored <- file.path(artifact, "S1_mask.txt")
  exact <- file.path(artifact, "S1_mask.png")
  file.create(ignored)
  file.create(exact)
  expect_identical(dsImaging:::.find_mask_in_artifact(artifact, "S1"),
    normalizePath(exact, winslash = "/"))

  second <- file.path(artifact, "S1_seg.nii.gz")
  file.create(second)
  expect_error(dsImaging:::.find_mask_in_artifact(artifact, "S1"),
    "ambiguous")
})

test_that("artifact mask manifests are exact, authoritative, and confined", {
  root <- withr::local_tempdir()
  artifact <- file.path(root, "artifact")
  outside_root <- file.path(root, "outside")
  dir.create(artifact)
  dir.create(outside_root)
  fallback <- file.path(artifact, "S1_mask.nii.gz")
  outside <- file.path(outside_root, "S1_mask.nii.gz")
  file.create(fallback)
  file.create(outside)
  manifest_path <- file.path(artifact, "seg_manifest.json")

  jsonlite::write_json(list(samples = list(S10 = list(
    primary_mask = "S10_mask.nii.gz"))), manifest_path, auto_unbox = TRUE)
  expect_null(dsImaging:::.find_mask_in_artifact(artifact, "S1"))

  jsonlite::write_json(list(samples = list(S1 = list(
    primary_mask = outside))), manifest_path, auto_unbox = TRUE)
  expect_null(dsImaging:::.find_mask_in_artifact(artifact, "S1"))

  link <- file.path(artifact, "S1_seg.nii.gz")
  linked <- suppressWarnings(file.symlink(outside, link))
  if (isTRUE(linked)) {
    jsonlite::write_json(list(samples = list(S1 = list(
      primary_mask = basename(link)))), manifest_path, auto_unbox = TRUE)
    expect_null(dsImaging:::.find_mask_in_artifact(artifact, "S1"))
  }

  jsonlite::write_json(list(samples = list(S1 = list(
    primary_mask = basename(fallback)))), manifest_path, auto_unbox = TRUE)
  expect_identical(dsImaging:::.find_mask_in_artifact(artifact, "S1"),
    normalizePath(fallback, winslash = "/"))
})
