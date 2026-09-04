test_that("resource clients and imaging handles retain their storage context", {
  root <- withr::local_tempdir()
  collection <- write_test_imaging_collection(
    root, "imaging.contract",
    data.frame(sample_id = paste0("case", 1:3),
      patient_id = paste0("patient", 1:3), label = c(0L, 1L, 0L)),
    label_col = "label")
  image_root <- collection$image_root
  manifest_path <- collection$manifest_path
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
  expect_error(
    resourcer::as.resource.data.frame(client),
    "must be initialized with imagingInitDS",
    fixed = TRUE
  )

  env <- new.env(parent = globalenv())
  assign("img", client, envir = env)
  assign("imagingInitDS", dsImaging::imagingInitDS, envir = env)
  reference <- eval(quote(imagingInitDS("img")), envir = env)
  assign("imaging_handle", reference, envir = env)
  handle <- eval(quote(dsImaging:::.getImagingHandle("imaging_handle")),
                 envir = env)

  expect_s3_class(handle$descriptor, "ImagingDatasetDescriptor")
  expect_false(inherits(handle$descriptor, "FlowerDatasetDescriptor"))
  expect_identical(handle$backend$type, "file")
  expect_identical(handle$manifest_uri, manifest_path)
  expect_equal(handle$n_privacy_units, 3L)
  expect_true(dir.exists(image_root))

  raw_env <- new.env(parent = globalenv())
  assign("img_resource", resource, envir = raw_env)
  assign("imagingInitDS", dsImaging::imagingInitDS, envir = raw_env)
  expect_error(eval(quote(imagingInitDS("img_resource")), envir = raw_env),
    "could not be initialized", fixed = TRUE)

  mismatched_class <- R6::R6Class(
    "MismatchedImagingDatasetResourceClient",
    inherit = ImagingDatasetResourceClient,
    public = list(getDatasetId = function() "other.dataset"))
  assign("mismatched", mismatched_class$new(resource), envir = raw_env)
  expect_error(eval(quote(imagingInitDS("mismatched")), envir = raw_env),
               "could not be initialized", fixed = TRUE)
})

test_that("Armadillo format locators use only the server registry", {
  root <- withr::local_tempdir()
  collection <- write_test_imaging_collection(
    root, "imaging.contract",
    data.frame(sample_id = paste0("case", 1:3),
      patient_id = paste0("patient", 1:3), label = c(0L, 1L, 0L)),
    label_col = "label")
  registry_path <- file.path(root, "registry.yaml")
  yaml::write_yaml(list(
    schema_version = 1L,
    "imaging.contract" = list(
      enabled = TRUE, backend = "file",
      manifest_uri = collection$manifest_path)
  ), registry_path)
  withr::local_options(list(dsimaging.registry_path = registry_path))

  uploaded <- resourcer::newResource(
    name = "imaging.contract",
    url = paste0("https://armadillo.test/storage/projects/imaging/",
      "objects/markers%2Fimaging_contract.parquet"),
    format = "dsimaging-dataset:imaging.contract")
  # This is the exact descriptor shape created by current Armadillo servers.
  reconstructed <- resourcer::newResource(
    name = uploaded$name,
    url = gsub("/objects/", "/rawfiles/", uploaded$url),
    format = uploaded$format,
    secret = "temporary-armadillo-jwt")

  resolver <- ImagingDatasetResourceResolver$new()
  expect_true(resolver$isFor(reconstructed))
  client <- resourcer::newResourceClient(reconstructed)
  expect_s3_class(client, "ImagingDatasetResourceClient")
  expect_identical(client$getDatasetId(), "imaging.contract")
  expect_identical(client$getManifestUri(), collection$manifest_path)
  expect_identical(client$getBackend()$type, "file")

  retained <- client$getResource()
  expect_identical(retained$name, "dsImaging dataset")
  expect_identical(retained$url, "imaging+dataset://imaging.contract")
  expect_null(retained$identity)
  expect_null(retained$secret)
  expect_null(retained$format)
  expect_false(grepl("armadillo.test", retained$url, fixed = TRUE))
  expect_false(grepl("temporary-armadillo-jwt",
    paste(unlist(retained), collapse = ""), fixed = TRUE))

  # Even if a non-Armadillo caller attaches credential-shaped fields, a
  # format locator must not switch to the direct S3 Resource path.
  credential_bait <- resourcer::newResource(
    name = uploaded$name, url = reconstructed$url,
    identity = "must-not-be-used", secret = "must-not-be-used",
    format = uploaded$format)
  bait_client <- ImagingDatasetResourceClient$new(credential_bait)
  expect_identical(bait_client$getBackend()$type, "file")
  expect_null(bait_client$getResource()$identity)
  expect_null(bait_client$getResource()$secret)

  direct_with_unrelated_format <- resourcer::newResource(
    "PROJECT.images", "imaging+dataset://imaging.contract",
    format = "parquet")
  direct_client <- ImagingDatasetResourceClient$new(
    direct_with_unrelated_format)
  expect_identical(direct_client$getDatasetId(), "imaging.contract")
  expect_identical(direct_client$getResource()$format, "parquet")
})

test_that("imaging Resource locators reject malformed and conflicting forms", {
  marker_url <- paste0(
    "https://armadillo.test/storage/projects/imaging/rawfiles/",
    "markers%2Fimaging_contract.parquet")
  malformed <- list(
    NULL,
    "dsimaging-dataset:",
    "dsimaging-dataset",
    "dsimaging-dataset:../imaging.contract",
    "dsimaging-dataset:Imaging.Contract",
    "dsimaging-dataset:imaging.contract?endpoint=other",
    "unrelated-format",
    c("dsimaging-dataset:imaging.contract", "extra"),
    1L)
  for (format in malformed) {
    resource <- resourcer::newResource(
      "PROJECT.images", marker_url, format = format)
    expect_error(ImagingDatasetResourceClient$new(resource),
      "Invalid imaging dataset resource", fixed = TRUE)
  }

  conflicting <- resourcer::newResource(
    "PROJECT.images", "imaging+dataset://imaging.contract",
    format = "dsimaging-dataset:other.dataset")
  expect_error(ImagingDatasetResourceClient$new(conflicting),
    "Invalid imaging dataset resource", fixed = TRUE)

  vector_locator <- resourcer::newResource(
    "PROJECT.images", "imaging+dataset://imaging.contract",
    format = c("dsimaging-dataset:imaging.contract", "parquet"))
  expect_error(ImagingDatasetResourceClient$new(vector_locator),
    "Invalid imaging dataset resource", fixed = TRUE)

  direct_with_unrelated_format <- resourcer::newResource(
    "PROJECT.images", "imaging+dataset://imaging.contract",
    format = "parquet")
  selector <- dsImaging:::.imaging_resource_selector(
    direct_with_unrelated_format)
  expect_identical(selector$transport, "direct")
  expect_identical(selector$dataset_id, "imaging.contract")

  partial_format <- structure(list(
    name = "PROJECT.images",
    url = "imaging+dataset://imaging.contract",
    identity = NULL,
    secret = NULL,
    formatLocator = "dsimaging-dataset:other.dataset"),
    class = "resource")
  partial_selector <- dsImaging:::.imaging_resource_selector(partial_format)
  expect_identical(partial_selector$transport, "direct")
  expect_identical(partial_selector$dataset_id, "imaging.contract")

  partial_only <- structure(list(
    name = "PROJECT.images", urlLocator = marker_url,
    formatLocator = "dsimaging-dataset:imaging.contract"),
    class = "resource")
  expect_false(ImagingDatasetResourceResolver$new()$isFor(partial_only))
  expect_error(ImagingDatasetResourceClient$new(partial_only),
    "Invalid imaging dataset resource", fixed = TRUE)

  oversized_secret <- resourcer::newResource(
    "PROJECT.images", "imaging+dataset://imaging.contract",
    secret = strrep("x", 65537L))
  expect_error(ImagingDatasetResourceClient$new(oversized_secret),
    "Invalid imaging dataset resource", fixed = TRUE)

  oversized_identity <- resourcer::newResource(
    "PROJECT.images", "imaging+dataset://imaging.contract",
    identity = strrep("x", 4097L))
  expect_error(ImagingDatasetResourceClient$new(oversized_identity),
    "Invalid imaging dataset resource", fixed = TRUE)

  oversized_format <- resourcer::newResource(
    "PROJECT.images", "imaging+dataset://imaging.contract",
    format = strrep("x", 257L))
  expect_error(ImagingDatasetResourceClient$new(oversized_format),
    "Invalid imaging dataset resource", fixed = TRUE)
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

test_that("imagingInitDS rejects forged lists and in-session descriptors", {
  env <- new.env(parent = globalenv())
  assign("forged", list(url = "imaging+dataset://private.dataset"), envir = env)
  assign("imagingInitDS", dsImaging::imagingInitDS, envir = env)
  expect_error(eval(quote(imagingInitDS("forged")), envir = env),
               "could not be initialized", fixed = TRUE)

  assign("descriptor", structure(list(dataset_id = "private.dataset"),
    class = c("ImagingDatasetDescriptor", "list")), envir = env)
  expect_error(eval(quote(imagingInitDS("descriptor")), envir = env),
               "could not be initialized", fixed = TRUE)
})

test_that("resource URL location fields are validated before backend access", {
  bad_urls <- c(
    paste0("imaging+dataset://dataset.one?endpoint=",
      utils::URLencode("https://objects.example/private", reserved = TRUE),
      "&bucket=imaging-data&prefix=datasets/dataset.one"),
    paste0("imaging+dataset://dataset.one?endpoint=",
      utils::URLencode("https://objects.example", reserved = TRUE),
      "&bucket=imaging-data&prefix=../dataset.one"),
    paste0("imaging+dataset://dataset.one?endpoint=",
      utils::URLencode("https://objects.example", reserved = TRUE),
      "&bucket=imaging-data&prefix=datasets/other.dataset"),
    "imaging+dataset://dataset.one?bucket=ok&bucket=duplicate"
  )
  for (url in bad_urls) {
    resource <- resourcer::newResource("PROJECT.images", url,
      identity = "access", secret = "secret")
    expect_error(ImagingDatasetResourceClient$new(resource),
                 "Invalid imaging dataset resource URL", fixed = TRUE)
  }
})

test_that("resource manifest must match the URL dataset exactly", {
  root <- withr::local_tempdir()
  metadata_path <- file.path(root, "samples.csv")
  utils::write.csv(data.frame(
    sample_id = paste0("case", 1:3), patient_id = paste0("patient", 1:3)),
    metadata_path, row.names = FALSE)
  manifest_path <- file.path(root, "manifest.yaml")
  yaml::write_yaml(list(
    schema_version = 1L,
    dataset_id = "other.dataset",
    metadata = list(uri = metadata_path, file = metadata_path, format = "csv",
      id_col = "sample_id", privacy_unit = "patient",
      privacy_unit_col = "patient_id",
      privacy_unit_canonicalization = "trim-utf8-v2"),
    assets = list()), manifest_path)
  registry_path <- file.path(root, "registry.yaml")
  yaml::write_yaml(list(schema_version = 1L,
    requested.dataset = list(enabled = TRUE, backend = "file",
      manifest_uri = manifest_path)), registry_path)
  withr::local_options(list(dsimaging.registry_path = registry_path))

  resource <- resourcer::newResource(
    "PROJECT.images", "imaging+dataset://requested.dataset")
  expect_error(ImagingDatasetResourceClient$new(resource),
               "does not match its dataset", fixed = TRUE)

})

test_that("S3 resource manifests cannot reference another collection prefix", {
  collection <- "s3://imaging-e2e/datasets/pathmnist.site"
  other <- "s3://imaging-e2e/datasets/private.site"
  manifest <- list(
    schema_version = 1L,
    dataset_id = "pathmnist.site",
    metadata = list(uri = paste0(collection, "/metadata/samples.csv"),
      format = "csv"),
    assets = list(images = list(kind = "image_root",
      uri = paste0(collection, "/source/images/"))),
    labels = list(list(name = "diagnosis",
      uri = paste0(collection, "/metadata/labels.csv"))),
    sample_manifests = list(
      uri = paste0(collection, "/metadata/sample_manifests.csv")),
    content_hash_index = list(
      uri = paste0(collection, "/indexes/content_hash_index.csv"))
  )
  local_mocked_bindings(
    parse_manifest = function(uri, backend) manifest,
    .package = "dsImaging"
  )
  resource <- resourcer::newResource(
    "PROJECT.images",
    paste0("imaging+dataset://127.0.0.1:19000/imaging-e2e/",
      "datasets/pathmnist.site"),
    identity = "access", secret = "secret")

  mutations <- list(
    function(x) { x$metadata$uri <- paste0(other, "/metadata/samples.csv"); x },
    function(x) { x$metadata$file <- "/srv/private/samples.csv"; x },
    function(x) { x$assets$images$uri <- paste0(other, "/source/images/"); x },
    function(x) { x$labels[[1]]$uri <- paste0(other, "/labels.csv"); x },
    function(x) {
      x$sample_manifests$uri <- paste0(other, "/sample_manifests.csv"); x
    },
    function(x) {
      x$content_hash_index$uri <- paste0(other, "/content_hash_index.csv"); x
    },
    function(x) {
      x$assets$images$content_hash_index <-
        paste0(other, "/images_content_hash_index.csv"); x
    }
  )
  original <- manifest
  for (mutate in mutations) {
    manifest <- mutate(original)
    expect_error(ImagingDatasetResourceClient$new(resource),
                 "outside its collection scope", fixed = TRUE)
  }
})

test_that("local resource manifests are confined to their collection root", {
  store <- withr::local_tempdir()
  collection <- file.path(store, "datasets", "public.site")
  other <- file.path(store, "datasets", "private.site")
  dir.create(collection, recursive = TRUE)
  dir.create(other, recursive = TRUE)
  public_metadata <- file.path(collection, "samples.csv")
  private_metadata <- file.path(other, "samples.csv")
  rows <- data.frame(sample_id = paste0("case", 1:3),
    patient_id = paste0("patient", 1:3))
  utils::write.csv(rows, public_metadata, row.names = FALSE)
  utils::write.csv(rows, private_metadata, row.names = FALSE)
  manifest_path <- file.path(collection, "manifest.yaml")
  manifest <- list(
    schema_version = 1L,
    dataset_id = "public.site",
    metadata = list(uri = public_metadata, file = private_metadata,
      format = "csv", id_col = "sample_id", privacy_unit = "patient",
      privacy_unit_col = "patient_id",
      privacy_unit_canonicalization = "trim-utf8-v2"),
    assets = list(images = list(kind = "image_root",
      uri = file.path(collection, "images")))
  )
  yaml::write_yaml(manifest, manifest_path)
  registry_path <- file.path(store, "registry.yaml")
  yaml::write_yaml(list(schema_version = 1L,
    public.site = list(enabled = TRUE, backend = "file",
      manifest_uri = manifest_path)), registry_path)
  withr::local_options(list(dsimaging.registry_path = registry_path))
  resource <- resourcer::newResource(
    "PROJECT.images", "imaging+dataset://public.site")

  expect_error(ImagingDatasetResourceClient$new(resource),
               "outside its collection scope", fixed = TRUE)

  manifest$metadata$file <- public_metadata
  manifest$assets$images$uri <- file.path(other, "images")
  yaml::write_yaml(manifest, manifest_path)
  expect_error(ImagingDatasetResourceClient$new(resource),
               "outside its collection scope", fixed = TRUE)
})
