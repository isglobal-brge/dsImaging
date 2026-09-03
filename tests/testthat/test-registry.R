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

test_that("admin dataset listing reads manifest_uri registry entries", {
  manifest_path <- tempfile(fileext = ".yml")
  registry_path <- tempfile(fileext = ".yml")
  on.exit(unlink(c(manifest_path, registry_path)))

  yaml::write_yaml(list(
    schema_version = 1,
    dataset_id = "ds.enabled",
    title = "Enabled imaging dataset",
    modality = "ct",
    metadata = list(uri = "/tmp/samples.csv", format = "csv",
      id_col = "sample_id", privacy_unit = "patient",
      privacy_unit_col = "patient_id",
      privacy_unit_canonicalization = "trim-utf8-v2")
  ), manifest_path)

  yaml::write_yaml(list(
    schema_version = 1,
    "ds.enabled" = list(
      manifest_uri = manifest_path,
      enabled = TRUE
    )
  ), registry_path)

  withr::with_options(list(dsimaging.registry_path = registry_path), {
    result <- dsImaging:::.list_imaging_datasets_admin()
    expect_equal(result$dataset_id, "ds.enabled")
    expect_equal(result$title, "Enabled imaging dataset")
    expect_equal(result$modality, "ct")
    expect_true(result$enabled)
  })
})

test_that("resolve_dataset exposes an explicit NULL manifest element", {
  # Regression: without an exact `manifest` element, `resolved$manifest`
  # partial-matched `manifest_uri` and returned the URI string, crashing the
  # worker-side drip feed with "$ operator is invalid for atomic vectors".
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    schema_version = 1,
    "test.dataset" = list(manifest_uri = "/tmp/m.yml", enabled = TRUE,
                          backend = "file")
  ), tmp)

  withr::with_options(list(dsimaging.registry_path = tmp), {
    res <- resolve_dataset("test.dataset")
    expect_true("manifest" %in% names(res))
    expect_null(res$manifest)
    expect_identical(res$manifest_uri, "/tmp/m.yml")
  })
})
