# Tests for R/registry.R

test_that(".load_registry errors when option not set", {
  withr::with_options(
    list(dsimaging.registry_path = NULL,
         default.dsimaging.registry_path = NULL), {
    expect_error(.load_registry(), "registry_path option is not configured")
  })
})

test_that("resolve_dataset errors for missing dataset", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    "existing.dataset" = list(manifest = "/tmp/m.yml", enabled = TRUE)
  ), tmp)

  withr::with_options(list(dsimaging.registry_path = tmp), {
    expect_error(resolve_dataset("nonexistent"), "not found in registry")
  })
})

test_that("resolve_dataset errors for disabled dataset", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    "test.dataset" = list(manifest = "/tmp/m.yml", enabled = FALSE)
  ), tmp)

  withr::with_options(list(dsimaging.registry_path = tmp), {
    expect_error(resolve_dataset("test.dataset"), "disabled")
  })
})

test_that("list_datasets returns only enabled entries", {
  tmp <- tempfile(fileext = ".yml")
  on.exit(unlink(tmp))

  yaml::write_yaml(list(
    "ds.enabled" = list(manifest = "/tmp/a.yml", enabled = TRUE),
    "ds.disabled" = list(manifest = "/tmp/b.yml", enabled = FALSE)
  ), tmp)

  withr::with_options(list(dsimaging.registry_path = tmp), {
    result <- list_datasets()
    expect_equal(length(result), 1)
    expect_true("ds.enabled" %in% names(result))
  })
})
