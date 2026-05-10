# Tests for R/registry.R

test_that(".load_registry errors when registry file not found", {
  withr::with_options(
    list(dsimaging.registry_path = "/nonexistent/path/registry.yaml",
         default.dsimaging.registry_path = NULL), {
    expect_error(.load_registry(), "not found")
  })
})

test_that("resolve_dataset errors for missing dataset", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    schema_version = 1,
    "existing.dataset" = list(manifest_uri = "/tmp/m.yml", enabled = TRUE)
  ), tmp)

  withr::with_options(list(dsimaging.registry_path = tmp), {
    expect_error(resolve_dataset("nonexistent"), "not found in registry")
  })
})

test_that("resolve_dataset errors for disabled dataset", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    schema_version = 1,
    "test.dataset" = list(manifest_uri = "/tmp/m.yml", enabled = FALSE)
  ), tmp)

  withr::with_options(list(dsimaging.registry_path = tmp), {
    expect_error(resolve_dataset("test.dataset"), "disabled")
  })
})

test_that("list_datasets returns only enabled entries", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    schema_version = 1,
    "ds.enabled" = list(manifest_uri = "/tmp/a.yml", enabled = TRUE),
    "ds.disabled" = list(manifest_uri = "/tmp/b.yml", enabled = FALSE)
  ), tmp)

  withr::with_options(list(dsimaging.registry_path = tmp), {
    result <- list_datasets()
    expect_equal(length(result), 1)
    expect_true("ds.enabled" %in% names(result))
  })
})

test_that("imagingListDatasetsDS reads manifest_uri registry entries", {
  manifest_path <- tempfile(fileext = ".yml")
  registry_path <- tempfile(fileext = ".yml")
  on.exit(unlink(c(manifest_path, registry_path)))

  yaml::write_yaml(list(
    schema_version = 1,
    dataset_id = "ds.enabled",
    title = "Enabled imaging dataset",
    modality = "ct",
    metadata = list(uri = "/tmp/samples.csv", format = "csv")
  ), manifest_path)

  yaml::write_yaml(list(
    schema_version = 1,
    "ds.enabled" = list(
      manifest_uri = manifest_path,
      enabled = TRUE
    )
  ), registry_path)

  withr::with_options(list(dsimaging.registry_path = registry_path), {
    result <- imagingListDatasetsDS()
    expect_equal(result$dataset_id, "ds.enabled")
    expect_equal(result$title, "Enabled imaging dataset")
    expect_equal(result$modality, "ct")
    expect_true(result$enabled)
  })
})
