test_that("MinIO webhook events upsert resource content hashes", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    dsimagingstore.resource_mapper = function(bucket, object_key) {
      paste(bucket, object_key, sep = ":")
    }
  ))

  payload <- jsonlite::toJSON(list(Records = list(list(
    s3 = list(
      bucket = list(name = "imaging-objects"),
      object = list(
        key = "lung1/indexes/content_hash_index.parquet",
        eTag = "0123456789abcdef0123456789abcdef"
      )
    )
  ))), auto_unbox = TRUE)

  handled <- dsImaging:::.content_hash_handle_minio_event(payload)
  expect_equal(nrow(handled), 1L)
  expect_equal(handled$source, "etag_md5")

  row <- dsImaging:::.content_hash_get(
    "imaging-objects:lung1/indexes/content_hash_index.parquet")
  expect_equal(row$content_hash, "0123456789abcdef0123456789abcdef")
  expect_equal(row$source, "etag_md5")
})

test_that("internal content hashing returns stable indexed values", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(dsimaging.asset_db = db_path))

  dsImaging:::.content_hash_upsert("lung1", "sha256:abc", "checksum_sha256",
    bucket = "imaging-objects", object_key = "lung1/object.nii.gz")

  first <- dsImaging:::.content_hash_for_resource("lung1")
  second <- dsImaging:::.content_hash_for_resource("lung1")
  expect_equal(first$content_hash, second$content_hash)
  expect_equal(first$source, "checksum_sha256")
})

test_that("mutating an object changes the indexed content hash", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    dsimagingstore.resource_mapper = function(bucket, object_key) "lung1"
  ))

  make_payload <- function(etag) {
    jsonlite::toJSON(list(Records = list(list(
      s3 = list(bucket = list(name = "imaging-objects"),
        object = list(key = "lung1/object.nii.gz", eTag = etag))
    ))), auto_unbox = TRUE)
  }

  dsImaging:::.content_hash_handle_minio_event(make_payload(
    "11111111111111111111111111111111"))
  before <- dsImaging:::.content_hash_for_resource("lung1")
  dsImaging:::.content_hash_handle_minio_event(make_payload(
    "22222222222222222222222222222222"))
  after <- dsImaging:::.content_hash_for_resource("lung1")

  expect_equal(before$content_hash, "11111111111111111111111111111111")
  expect_equal(after$content_hash, "22222222222222222222222222222222")
})

test_that("internal content hashing rejects direct S3 object probes", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    dsimagingstore.object_head = function(bucket, object_key, locator = NULL) {
      list(etag = '"abcdefabcdefabcdefabcdefabcdefab"')
    }
  ))

  expect_error(
    dsImaging:::.content_hash_for_resource(
      "s3://imaging-objects/lung1/object.nii.gz"),
    "registered dataset or asset")
  expect_null(dsImaging:::.content_hash_get(
    "s3://imaging-objects/lung1/object.nii.gz"))
})

test_that("hash computation prefers checksum and streams multipart ETags", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    dsimagingstore.object_reader = function(bucket, object_key, locator = NULL) {
      charToRaw("payload-v1")
    }
  ))

  checksum <- dsImaging:::.content_hash_compute("bucket", "object",
    checksum_sha256 = "sha256-from-header", etag = "not-used")
  expect_equal(checksum$content_hash, "sha256-from-header")
  expect_equal(checksum$source, "checksum_sha256")

  streamed <- dsImaging:::.content_hash_compute("bucket", "object",
    etag = "0123456789abcdef0123456789abcdef-2")
  expected_file <- tempfile()
  writeBin(charToRaw("payload-v1"), expected_file)
  on.exit(unlink(expected_file), add = TRUE)
  expect_equal(streamed$source, "stream")
  expect_equal(streamed$content_hash,
    digest::digest(file = expected_file, algo = "sha256"))
})

test_that("unsupported resources return non-error NA metadata", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(
    dsimaging.asset_db = db_path,
    dsimaging.registry_path = "/definitely/not/a/registry.yaml"
  ))

  result <- dsImaging:::.content_hash_for_resource("legacy_resource")
  expect_true(is.na(result$content_hash))
  expect_true(is.na(result$updated_at))
  expect_equal(result$source, "unsupported")
})
