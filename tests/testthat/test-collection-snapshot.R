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

test_that("S3 snapshot materialization fetches the admitted object version", {
  root <- withr::local_tempdir()
  payload <- charToRaw("immutable-image")
  requests <- list()
  snapshot <- list(
    version = 1L,
    seal = strrep("a", 64L),
    artifacts = list(),
    records = list(list(
      source_kind = "single_file",
      relative_path = "scan-1.nii.gz",
      uri = "s3://imaging/datasets/study/source/images/scan-1.nii.gz",
      content_hash = digest::digest(payload, algo = "sha256", serialize = FALSE),
      size = length(payload),
      version_id = "immutable-version-1"
    ))
  )
  backend <- structure(list(type = "s3", config = list()),
                       class = "dsimaging_backend")
  testthat::local_mocked_bindings(
    backend_get_file = function(backend, uri, dest, overwrite = FALSE,
                                version_id = NULL) {
      requests[[length(requests) + 1L]] <<- list(
        uri = uri, version_id = version_id)
      writeBin(payload, dest)
      invisible(dest)
    },
    .package = "dsImaging"
  )

  materialized <- dsImaging:::.materialize_imaging_snapshot(
    snapshot, backend, file.path(root, "materialized"))

  expect_true(file.exists(file.path(materialized$root, "scan-1.nii.gz")))
  expect_identical(requests, list(list(
    uri = snapshot$records[[1L]]$uri,
    version_id = "immutable-version-1"
  )))
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

test_that("same-URI mask roots use their exact validated mask index", {
  skip_if_not_installed("arrow")
  root <- withr::local_tempdir()
  ids <- c("scan-A", "scan-B", "scan-C")
  collection <- write_test_imaging_collection(
    root, "snapshot.same-uri-masks",
    data.frame(sample_id = ids,
      patient_id = c("patient-A", "patient-B", "patient-C")))
  mask_paths <- file.path(
    collection$image_root, paste0("opaque-mask-", seq_along(ids), ".nii.gz"))
  for (i in seq_along(mask_paths)) {
    writeBin(charToRaw(paste0("test-mask-", ids[[i]])), mask_paths[[i]])
  }
  mask_index <- file.path(root, "indexes", "masks.parquet")
  reverse <- rev(seq_along(ids))
  mask_rows <- data.frame(
    sample_id = ids[reverse],
    uri = mask_paths[reverse],
    content_hash = vapply(mask_paths[reverse], digest::digest, character(1),
      algo = "sha256", file = TRUE),
    size = as.numeric(file.info(mask_paths[reverse])$size),
    source_kind = "mask_file",
    stringsAsFactors = FALSE)
  arrow::write_parquet(mask_rows, mask_index)
  collection$manifest$assets$masks <- list(
    kind = "mask_root", uri = collection$image_root,
    content_hash_index = mask_index)
  yaml::write_yaml(collection$manifest, collection$manifest_path)

  backend <- storage_backend("file")
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(collection$manifest), "img",
    backend = backend, manifest_uri = collection$manifest_path,
    require_snapshot = TRUE)
  authorized <- list(
    handle = handle, dataset_id = handle$dataset_id,
    manifest = handle$manifest, backend = handle$backend,
    privacy_roster = handle$privacy_roster,
    collection_snapshot = handle$collection_snapshot)
  worker_manifest <- dsImaging:::.imaging_worker_manifest(
    authorized, c("images", "masks"))
  collection_map <- dsImaging:::.imaging_worker_collection_map(
    authorized, worker_manifest)

  expect_identical(
    vapply(collection_map$records_by_asset$masks, `[[`, character(1),
           "sample_id"), ids)
  expect_identical(
    vapply(collection_map$records_by_asset$masks, `[[`, character(1),
           "content_hash"),
    unname(vapply(mask_paths, digest::digest, character(1),
                  algo = "sha256", file = TRUE)))

  arrow::write_parquet(mask_rows[-1L, ], mask_index)
  expect_error(dsImaging:::.imaging_worker_collection_map(
    authorized, worker_manifest), "complete admitted collection", fixed = TRUE)
})
