write_synthetic_model_bundle <- function(root, recipe = FALSE) {
  directory <- file.path(root, "lungmask", "R231")
  dir.create(file.path(directory, "weights"), recursive = TRUE)
  weight <- file.path(directory, "weights", "model.pth")
  writeBin(charToRaw("synthetic model bytes"), weight)
  item <- list(path = "weights/model.pth", size = unname(file.info(weight)$size),
    sha256 = digest::digest(weight, algo = "sha256", file = TRUE))
  manifest <- list(schema_version = 1L, provider = "lungmask", task = "R231",
    upstream_version = "0.0.1", licence = "Synthetic fixture only",
    source_url = "https://example.invalid/model", download_date = "2026-09-23T00:00:00Z",
    files = list(item), runtime = list(model_path = "weights/model.pth"))
  jsonlite::write_json(manifest, file.path(directory, "manifest.json"),
    auto_unbox = TRUE, pretty = TRUE)
  if (recipe) {
    manifest$download_date <- NULL
    manifest$files[[1]]$url <- paste0("file://", normalizePath(weight))
    return(manifest)
  }
  directory
}

test_that("registration accepts only complete canonical bundles and pins their manifest", {
  skip_if(!nzchar(Sys.which("python3")), "python3 is unavailable")
  root <- withr::local_tempdir()
  withr::local_options(list(dsimaging.analysis.models_dir = root))
  expect_equal(nrow(list_segmentation_models()), 0L)
  expect_error(register_segmentation_model("test", "nnunetv2", "test", "/external/model"),
    "canonical")
  directory <- write_synthetic_model_bundle(root)
  entry <- register_segmentation_model("synthetic", "lungmask", "R231", directory)
  expect_true(file.exists(entry))
  models <- list_segmentation_models()
  expect_equal(nrow(models), 1L)
  expect_identical(models$name, "synthetic")
  expect_identical(models$provider, "lungmask")
  expect_true(models$ready)
  sha <- digest::digest(file.path(directory, "manifest.json"), algo = "sha256", file = TRUE)
  expect_identical(models$manifest_sha256, sha)
  expect_identical(dsImaging:::.get_model_config("synthetic")$manifest_sha256, sha)
  expect_null(dsImaging:::.get_model_config("missing"))

  public <- imagingCapabilitiesDS()$models
  expect_named(public, c("provider", "task", "ready", "manifest_sha256"))
  expect_identical(public$manifest_sha256, sha)
  expect_true(public$ready)
  expect_false(grepl(root, jsonlite::toJSON(public), fixed = TRUE))
  expect_identical(imagingListModelsDS(), public)

  writeBin(charToRaw("tampered model bytes!"), file.path(directory, "weights", "model.pth"))
  damaged <- imagingListModelsDS()
  expect_false(any(damaged$ready))
  expect_null(dsImaging:::.get_model_config("synthetic"))
  expect_error(register_segmentation_model("synthetic", "lungmask", "R231", directory), "mismatch")
})

test_that("legacy installed markers and path-only records do not establish readiness", {
  root <- withr::local_tempdir()
  withr::local_options(list(dsimaging.analysis.models_dir = root))
  directory <- file.path(root, "lungmask", "R231")
  dir.create(directory, recursive = TRUE)
  writeLines("installed", file.path(directory, ".installed"))
  jsonlite::write_json(list(provider = "lungmask", task = "R231", path = directory),
    file.path(root, "legacy.json"), auto_unbox = TRUE)
  expect_equal(nrow(list_installed_models()), 0L)
  expect_equal(nrow(imagingListModelsDS()), 0L)
  expect_error(install_model("lungmask", "../R231"), "Invalid model bundle task")
  expect_error(register_segmentation_model("../escape", "lungmask", "R231", directory),
    "Invalid model bundle name")
  expect_error(register_segmentation_model("x", "lungmask", "R231", directory,
    extra = list(path = "/external")), "verified bundle manifest")
})

test_that("admin installer downloads once, reports a digest, and fails closed on broken bytes", {
  skip_if(!nzchar(Sys.which("python3")), "python3 is unavailable")
  root <- withr::local_tempdir()
  source <- file.path(root, "source")
  models <- file.path(root, "models")
  recipe <- write_synthetic_model_bundle(source, recipe = TRUE)
  dir.create(file.path(models, "sources", "lungmask"), recursive = TRUE)
  jsonlite::write_json(recipe, file.path(models, "sources", "lungmask", "R231.json"),
    auto_unbox = TRUE, pretty = TRUE)
  withr::local_options(list(dsimaging.analysis.models_dir = models,
    dshpc.admin_key = "bundle-admin-key"))
  expect_error(imagingInstallModelDS("invalid", "lungmask", "R231"), "Admin access denied")
  expect_false(dir.exists(file.path(models, "lungmask")))
  key <- dsImaging:::.dsr_encode(list(.admin_key = "bundle-admin-key"))
  result <- imagingInstallModelDS(key, "lungmask", "R231")
  expect_identical(result$status, "installed")
  expect_match(result$manifest_sha256, "^[0-9a-f]{64}$")
  expect_identical(result$manifest_sha256, imagingListModelsDS()$manifest_sha256)
  expect_false(file.exists(file.path(models, "lungmask", "R231", ".installed")))
  unlink(source, recursive = TRUE)
  expect_message(second <- install_model("lungmask", "R231"), "Verified model bundle")
  expect_identical(second$manifest_sha256, result$manifest_sha256)
  unlink(file.path(models, "lungmask", "R231", "weights", "model.pth"))
  refused <- imagingInstallModelDS(key, "lungmask", "R231")
  expect_identical(refused$status, "failed")
  expect_identical(refused$error, "installation_failed")
  expect_false(grepl(root, jsonlite::toJSON(refused), fixed = TRUE))
  expect_false(any(imagingListModelsDS()$ready))
})

test_that("bundle roots are administrator settings and analyst selectors accept no paths", {
  root <- withr::local_tempdir()
  withr::local_options(list(dsimaging.analysis.models_dir = NULL,
    default.dsimaging.analysis.models_dir = NULL, dsimaging.models_dir = NULL,
    default.dsimaging.models_dir = NULL))
  withr::local_envvar(c(DSIMAGING_MODELS = file.path(root, "models"),
    DSIMAGING_MODEL_REGISTRY = file.path(root, "registry"),
    DSIMAGING_MODEL_SOURCES = file.path(root, "sources"),
    DSIMAGING_ANALYSIS_MODELS_DIR = NA_character_, DSIMAGING_MODELS_DIR = NA_character_))
  expect_identical(dsImaging:::.models_dir(), file.path(root, "models"))
  expect_identical(dsImaging:::.model_registry_path(), file.path(root, "registry"))
  expect_identical(dsImaging:::.model_sources_dir(), file.path(root, "sources"))
  expect_false(dir.exists(file.path(root, "models")))
  for (provider in c("monai", "lungmask", "nnunetv2", "totalsegmentator")) {
    expect_error(dsImaging:::.imaging_segmenter_spec(list(provider = provider,
      model_path = "/external/model")), "unsupported|Unsupported|not allowed")
  }
})
