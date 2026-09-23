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

