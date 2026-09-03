test_that("resource clients and imaging handles retain their storage context", {
  root <- withr::local_tempdir()
  image_root <- file.path(root, "images")
  dir.create(image_root)
  metadata_path <- file.path(root, "samples.csv")
  utils::write.csv(
    data.frame(sample_id = paste0("case", 1:3), label = c(0L, 1L, 0L)),
    metadata_path, row.names = FALSE)
  manifest_path <- file.path(root, "manifest.yaml")
  yaml::write_yaml(list(
    schema_version = 1L,
    dataset_id = "imaging.contract",
    modality = "pathology",
    metadata = list(
      uri = metadata_path, file = metadata_path, format = "csv"),
    assets = list(images = list(kind = "image_root", uri = image_root))
  ), manifest_path)
  registry_path <- file.path(root, "registry.yaml")
  yaml::write_yaml(list(
    schema_version = 1L,
    "imaging.contract" = list(
      enabled = TRUE, backend = "file", manifest_uri = manifest_path)
  ), registry_path)

  withr::local_options(list(dsimaging.registry_path = registry_path))
  resource <- resourcer::newResource(
    name = "PROJECT.images", url = "imaging+dataset://imaging.contract")
  client <- ImagingDatasetResourceClient$new(resource)

  expect_equal(client$getDatasetId(), "imaging.contract")
  expect_identical(client$getManifestUri(), manifest_path)
  expect_identical(client$getBackend()$type, "file")
  expect_equal(client$getManifest()$dataset_id, "imaging.contract")

  env <- new.env(parent = globalenv())
  assign("img", client, envir = env)
  assign("imagingInitDS", dsImaging::imagingInitDS, envir = env)
  handle <- eval(quote(imagingInitDS("img")), envir = env)
  withr::defer(rm(list = "imaging_img", envir = dsImaging:::.dsimaging_env))

  expect_s3_class(handle$descriptor, "ImagingDatasetDescriptor")
  expect_false(inherits(handle$descriptor, "FlowerDatasetDescriptor"))
  expect_identical(handle$backend$type, "file")
  expect_identical(handle$manifest_uri, manifest_path)
  expect_equal(handle$n_samples, 3L)
})

test_that("S3 resource clients retain the Resource without copying credentials", {
  manifest <- list(
    schema_version = 1L,
    dataset_id = "pathmnist.site",
    metadata = list(
      uri = "s3://imaging-e2e/datasets/pathmnist.site/metadata/samples.parquet",
      format = "parquet"),
    assets = list(images = list(
      kind = "image_root",
      uri = "s3://imaging-e2e/datasets/pathmnist.site/source/images/"))
  )
  observed <- new.env(parent = emptyenv())
  local_mocked_bindings(
    parse_manifest = function(uri, backend) {
      observed$uri <- uri
      observed$backend <- backend
      manifest
    },
    .package = "dsImaging"
  )
  resource <- resourcer::newResource(
    name = "PROJECT.images",
    url = paste0(
      "imaging+dataset://127.0.0.1:19000/imaging-e2e/",
      "datasets/pathmnist.site"),
    identity = "test-access-key",
    secret = "test-secret-key"
  )

  client <- ImagingDatasetResourceClient$new(resource)
  backend <- client$getBackend()

  expect_identical(backend$type, "s3")
  expect_identical(backend$config$resource, resource)
  expect_identical(backend$config$endpoint, "http://127.0.0.1:19000")
  expect_identical(
    client$getManifestUri(),
    "s3://imaging-e2e/datasets/pathmnist.site/manifest.yaml")
  expect_identical(observed$uri, client$getManifestUri())
  expect_identical(observed$backend, backend)
  expect_false(any(c("access_key", "secret_key", "identity", "secret") %in%
                   names(backend$config)))
})
