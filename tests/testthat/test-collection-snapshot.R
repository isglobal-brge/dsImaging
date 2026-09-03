test_that("collection snapshots pin metadata, routes, and image bytes", {
  root <- withr::local_tempdir()
  rows <- data.frame(
    sample_id = paste0("scan", 1:3),
    patient_id = paste0("patient", 1:3),
    diagnosis = c("case", "control", "case"),
    stringsAsFactors = FALSE)
  collection <- write_test_imaging_collection(
    root, "snapshot.test", rows, label_col = "diagnosis")
  backend <- storage_backend("file")
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(collection$manifest), "img",
    backend = backend, manifest_uri = collection$manifest_path,
    require_snapshot = TRUE)
  env <- new.env(parent = globalenv())
  reference <- dsImaging:::.register_imaging_handle(handle, env)
  assign("img", reference, env)

  expect_no_error(eval(
    quote(dsImaging:::.resolve_imaging_handle_for_consumer("img")), env))

  metadata_path <- collection$manifest$metadata$uri
  metadata <- utils::read.csv(metadata_path, stringsAsFactors = FALSE)
  metadata$diagnosis[[1L]] <- "changed"
  utils::write.csv(metadata, metadata_path, row.names = FALSE)
  expect_error(eval(
    quote(dsImaging:::.resolve_imaging_handle_for_consumer("img")), env),
    "collection snapshot")
})

test_that("snapshot materialization verifies every object hash and size", {
  root <- withr::local_tempdir()
  collection <- write_test_imaging_collection(
    root, "snapshot.bytes",
    data.frame(sample_id = paste0("scan", 1:3),
      patient_id = paste0("patient", 1:3)))
  backend <- storage_backend("file")
  admission <- dsImaging:::.imaging_privacy_admission(collection$manifest,
                                                       backend)
  snapshot <- dsImaging:::.new_imaging_collection_snapshot(
    collection$manifest, backend, admission)
  destination <- file.path(root, "materialized")
  materialized <- dsImaging:::.materialize_imaging_snapshot(
    snapshot, backend, destination)
  expect_equal(length(list.files(materialized$root, recursive = TRUE)), 3L)

  unlink(destination, recursive = TRUE)
  first <- snapshot$records[[1L]]$uri
  original_size <- file.info(first)$size
  writeBin(charToRaw(strrep("x", original_size)), first)
  expect_error(
    dsImaging:::.materialize_imaging_snapshot(
      snapshot, backend, destination),
    "integrity")
  expect_false(dir.exists(destination))
})

test_that("collection snapshots require exact artifact rosters", {
  root <- withr::local_tempdir()
  collection <- write_test_imaging_collection(
    root, "snapshot.roster",
    data.frame(sample_id = paste0("scan", 1:3),
      patient_id = paste0("patient", 1:3)))
  index <- utils::read.csv(
    collection$manifest$content_hash_index$uri, stringsAsFactors = FALSE)
  utils::write.csv(index[-1L, ], collection$manifest$content_hash_index$uri,
                   row.names = FALSE)
  expect_error(
    dsImaging:::.make_imaging_handle(
      imaging_dataset_descriptor(collection$manifest), "img",
      backend = storage_backend("file"),
      manifest_uri = collection$manifest_path, require_snapshot = TRUE),
    "integrity")
})

test_that("worker contexts carry only the admitted image sample map", {
  root <- withr::local_tempdir()
  collection <- write_test_imaging_collection(
    root, "snapshot.worker-map",
    data.frame(sample_id = c("scan-A", "scan-B", "scan-C"),
      patient_id = c("patient-A", "patient-B", "patient-C")))
  backend <- storage_backend("file")
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(collection$manifest), "img",
    backend = backend, manifest_uri = collection$manifest_path,
    require_snapshot = TRUE)
  context_root <- file.path(root, "contexts")
  withr::local_options(list(
    dsimaging.registry_path = file.path(root, "registry.yaml"),
    dsimaging.worker_context_dir = context_root))

  context_id <- dsImaging:::.register_imaging_worker_context(list(
    handle = handle, dataset_id = handle$dataset_id,
    manifest = handle$manifest, backend = handle$backend,
    privacy_roster = handle$privacy_roster,
    collection_snapshot = handle$collection_snapshot),
    asset_names = "images")
  context <- yaml::read_yaml(
    dsImaging:::.imaging_worker_context_file(context_id))

  expect_identical(unlist(context$collection_map$asset_names,
                          use.names = FALSE), "images")
  expect_identical(
    vapply(context$collection_map$records, `[[`, character(1), "sample_id"),
    handle$privacy_roster$sample_ids)
  expect_identical(
    vapply(context$collection_map$records, `[[`, character(1), "content_hash"),
    vapply(handle$collection_snapshot$records, `[[`, character(1),
           "content_hash"))
  expect_null(context$collection_map$records[[1L]]$files)
})
