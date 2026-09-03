test_privacy_manifest <- function(metadata_path, dataset_id = "lung",
                                  format = NULL,
                                  label_col = NULL,
                                  assets = list(images = list(
                                    type = "image_root", uri = dirname(metadata_path)))) {
  if (is.null(format)) {
    format <- if (grepl("[.]parquet$", metadata_path,
                        ignore.case = TRUE)) "parquet" else "csv"
  }
  list(
    schema_version = 1L,
    dataset_id = dataset_id,
    modality = "image",
    metadata = list(
      uri = metadata_path,
      file = metadata_path,
      format = format,
      id_col = "sample_id",
      privacy_unit = "patient",
      privacy_unit_col = "patient_id",
      privacy_unit_canonicalization = "trim-utf8-v2",
      label_col = label_col
    ),
    assets = assets
  )
}

assign_test_imaging_handle <- function(metadata_path, dataset_id = "lung",
                                       symbol = "img", env = parent.frame(),
                                       assets = NULL, label_col = NULL,
                                       collection_seal = strrep("a", 64)) {
  if (is.null(assets)) {
    assets <- list(images = list(
      type = "image_root", uri = dirname(metadata_path)))
  }
  manifest <- test_privacy_manifest(
    metadata_path, dataset_id, label_col = label_col, assets = assets)
  admission <- dsImaging:::.imaging_privacy_admission(manifest)
  handle <- list(
    dataset_id = dataset_id,
    manifest = manifest,
    backend = NULL,
    manifest_uri = NULL,
    privacy = admission$contract,
    n_privacy_units = admission$n_privacy_units,
    privacy_roster = admission$roster,
    collection_seal = collection_seal
  )
  reference <- dsImaging:::.register_imaging_handle(handle, env)
  assign(symbol, reference, envir = env)
  invisible(reference)
}

write_test_imaging_collection <- function(root, dataset_id, rows,
                                          label_col = NULL) {
  image_root <- file.path(root, "source", "images")
  metadata_root <- file.path(root, "metadata")
  index_root <- file.path(root, "indexes")
  dir.create(image_root, recursive = TRUE)
  dir.create(metadata_root, recursive = TRUE)
  dir.create(index_root, recursive = TRUE)

  sample_ids <- as.character(rows$sample_id)
  image_paths <- file.path(image_root, paste0(sample_ids, ".png"))
  for (i in seq_along(image_paths)) {
    writeBin(charToRaw(paste0("test-image-", sample_ids[[i]])), image_paths[[i]])
  }
  hashes <- vapply(image_paths, digest::digest, character(1),
                   algo = "sha256", file = TRUE)
  sizes <- as.numeric(file.info(image_paths)$size)
  source_kind <- rep("single_file", length(sample_ids))
  n_files <- rep(1L, length(sample_ids))
  metadata <- rows
  metadata$source_kind <- source_kind
  metadata$n_files <- n_files
  metadata_path <- file.path(metadata_root, "samples.csv")
  utils::write.csv(metadata, metadata_path, row.names = FALSE)

  relative <- paste0(sample_ids, ".png")
  sample_manifest_path <- file.path(metadata_root, "sample_manifests.csv")
  utils::write.csv(data.frame(
    sample_id = sample_ids,
    source_kind = source_kind,
    primary_uri = relative,
    files_json = vapply(relative, function(path) jsonlite::toJSON(
      list(list(path = path, role = "primary")), auto_unbox = TRUE),
      character(1)),
    content_hash = hashes,
    n_files = n_files,
    stringsAsFactors = FALSE
  ), sample_manifest_path, row.names = FALSE)
  index_path <- file.path(index_root, "content_hash_index.csv")
  utils::write.csv(data.frame(
    sample_id = sample_ids,
    uri = image_paths,
    content_hash = hashes,
    size = sizes,
    source_kind = source_kind,
    stringsAsFactors = FALSE
  ), index_path, row.names = FALSE)

  manifest <- list(
    schema_version = 1L,
    dataset_id = dataset_id,
    modality = "image",
    metadata = list(
      uri = metadata_path, format = "csv", id_col = "sample_id",
      privacy_unit = "patient", privacy_unit_col = "patient_id",
      privacy_unit_canonicalization = "trim-utf8-v2",
      label_col = label_col),
    assets = list(images = list(kind = "image_root", uri = image_root)),
    sample_manifests = list(uri = sample_manifest_path, format = "csv"),
    content_hash_index = list(uri = index_path, format = "csv")
  )
  manifest$metadata <- manifest$metadata[!vapply(
    manifest$metadata, is.null, logical(1))]
  manifest_path <- file.path(root, "manifest.yaml")
  yaml::write_yaml(manifest, manifest_path)
  list(manifest = manifest, manifest_path = manifest_path,
       image_root = image_root)
}
