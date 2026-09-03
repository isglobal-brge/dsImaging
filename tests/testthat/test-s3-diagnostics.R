test_that("S3 wrappers discard backend diagnostics and preserve results", {
  private <- paste(
    "/srv/private/patient-42/scan.nii.gz",
    "s3://private-bucket/datasets/hidden/patient-42")
  messages <- character()
  warnings <- character()

  output <- capture.output(
    value <- withCallingHandlers(
      dsImaging:::.s3_call(function() {
        cat(private)
        message(private)
        warning(private)
        list(ok = TRUE)
      }),
      message = function(m) {
        messages <<- c(messages, conditionMessage(m))
        invokeRestart("muffleMessage")
      },
      warning = function(w) {
        warnings <<- c(warnings, conditionMessage(w))
        invokeRestart("muffleWarning")
      }))

  expect_identical(value, list(ok = TRUE))
  expect_length(output, 0L)
  expect_length(messages, 0L)
  expect_length(warnings, 0L)

  public <- capture.output(
    failure <- tryCatch(
      dsImaging:::.s3_call(function() {
        cat(private)
        message(private)
        stop(private, call. = FALSE)
      }),
      error = identity))
  expect_s3_class(failure, "error")
  expect_identical(conditionMessage(failure), "S3 operation failed.")
  expect_length(public, 0L)
  expect_false(grepl(private, conditionMessage(failure), fixed = TRUE))
})

test_that("missing S3 HEAD checks do not print object diagnostics", {
  private <- "Client error: (404) hidden/patient-42/scan.nii.gz"
  local_mocked_bindings(
    head_object = function(...) {
      message(private)
      cat(private)
      structure(FALSE, `content-length` = NA_character_)
    },
    .package = "aws.s3")

  messages <- character()
  output <- capture.output(
    result <- withCallingHandlers(
      dsImaging:::.s3_head(
        list(access_key = "unused", secret_key = "unused",
             endpoint = "127.0.0.1:9000", region = ""),
        "bucket", "missing"),
      message = function(m) {
        messages <<- c(messages, conditionMessage(m))
        invokeRestart("muffleMessage")
      }))

  expect_null(result)
  expect_length(output, 0L)
  expect_length(messages, 0L)
})

test_that("asset metadata joins hide storage and path diagnostics", {
  private_marker <- "/srv/private/imaging/patients.unsupported"
  authorized <- list(dataset_id = "private.collection")

  testthat::local_mocked_bindings(
    .asset_db_connect = function() structure(list(), class = "mock_db"),
    .asset_db_close = function(db) invisible(NULL),
    .resolve_asset_by_id_or_alias = function(...) list(kind = "feature_table"),
    .read_feature_asset = function(...) data.frame(sample_id = 1:3),
    .assert_feature_asset_privacy = function(...) invisible(TRUE),
    .join_feature_asset_metadata = function(...) stop(private_marker),
    .package = "dsImaging"
  )

  error <- tryCatch(
    dsImaging:::.imaging_load_asset(
      authorized, "features", include_metadata = TRUE),
    error = identity
  )
  expect_s3_class(error, "error")
  expect_identical(conditionMessage(error),
                   "Imaging dataset metadata is unavailable.")
  expect_false(grepl(private_marker, conditionMessage(error), fixed = TRUE))
})
