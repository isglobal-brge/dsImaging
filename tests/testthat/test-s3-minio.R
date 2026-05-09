test_that("S3 backend works against a MinIO-compatible endpoint", {
  if (!identical(Sys.getenv("DSIMAGING_RUN_MINIO_TESTS", unset = ""), "1"))
    skip("set DSIMAGING_RUN_MINIO_TESTS=1 to run MinIO integration tests")

  docker <- Sys.which("docker")
  if (!nzchar(docker)) skip("docker CLI is not available")
  has_minio <- identical(system2(docker, c("image", "inspect", "minio/minio:latest"),
    stdout = FALSE, stderr = FALSE), 0L)
  if (!has_minio) skip("minio/minio:latest image is not available locally")

  container <- paste0("dsimaging-minio-smoke-", Sys.getpid())
  on.exit(system2(docker, c("rm", "-f", container), stdout = FALSE,
    stderr = FALSE), add = TRUE)

  started <- system2(docker, c(
    "run", "-d", "--name", container,
    "-e", "MINIO_ROOT_USER=minioadmin",
    "-e", "MINIO_ROOT_PASSWORD=minioadmin",
    "-p", "127.0.0.1::9000",
    "minio/minio:latest", "server", "/data"),
    stdout = TRUE, stderr = TRUE)
  expect_true(is.null(attr(started, "status")) || identical(as.integer(attr(started, "status")), 0L))

  port_line <- system2(docker, c("port", container, "9000/tcp"),
    stdout = TRUE, stderr = TRUE)[1]
  endpoint <- paste0("http://127.0.0.1:", sub(".*:", "", port_line))
  base_url <- sub("^https?://", "", endpoint)

  created <- FALSE
  for (i in seq_len(60)) {
    created <- tryCatch({
      aws.s3::put_bucket(
        bucket = "imaging-smoke",
        region = "",
        location_constraint = "",
        key = "minioadmin",
        secret = "minioadmin",
        base_url = base_url,
        use_https = FALSE)
      TRUE
    }, error = function(e) FALSE)
    if (isTRUE(created)) break
    Sys.sleep(1)
  }
  expect_true(created)

  withr::local_options(list(dsimaging.credentials = list(smoke = list(
    access_key = "minioadmin",
    secret_key = "minioadmin",
    endpoint = endpoint,
    region = ""
  ))))
  backend <- storage_backend("s3", list(endpoint = endpoint,
    credentials_ref = "smoke"))

  local <- tempfile("s3-smoke-")
  writeLines(c("id,value", "case001,42"), local)

  backend_put_file(backend, local,
    "s3://imaging-smoke/datasets/demo/metadata.csv")
  head <- backend_head(backend,
    "s3://imaging-smoke/datasets/demo/metadata.csv")
  keys <- backend_list(backend, "s3://imaging-smoke/datasets/demo/")
  out <- tempfile("s3-smoke-out-")
  backend_get_file(backend,
    "s3://imaging-smoke/datasets/demo/metadata.csv", out)

  expect_true(isTRUE(head$exists))
  expect_true("s3://imaging-smoke/datasets/demo/metadata.csv" %in% keys)
  expect_equal(readLines(out, warn = FALSE), c("id,value", "case001,42"))
})
