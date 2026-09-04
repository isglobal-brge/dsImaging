test_that("catalog connections wait for an active worker transaction", {
  path <- tempfile(fileext = ".sqlite")
  ready <- tempfile()
  withr::local_options(list(dsimaging.asset_db = path))

  initial <- dsImaging:::.asset_db_connect()
  dsImaging:::.asset_db_close(initial)

  child_code <- paste(
    "args <- commandArgs(TRUE)",
    "db <- DBI::dbConnect(RSQLite::SQLite(), args[[1L]], synchronous = NULL)",
    "DBI::dbExecute(db, 'PRAGMA locking_mode=EXCLUSIVE')",
    "DBI::dbExecute(db, 'BEGIN EXCLUSIVE')",
    "file.create(args[[2L]])",
    "Sys.sleep(1)",
    "DBI::dbExecute(db, 'COMMIT')",
    "DBI::dbDisconnect(db)",
    sep = "; "
  )
  child <- processx::process$new(
    file.path(R.home("bin"), "Rscript"),
    c("-e", child_code, path, ready),
    stdout = "|", stderr = "|")
  withr::defer(if (child$is_alive()) child$kill())

  deadline <- Sys.time() + 5
  while (!file.exists(ready) && child$is_alive() && Sys.time() < deadline) {
    Sys.sleep(0.02)
  }
  expect_true(file.exists(ready))

  expect_no_warning(db <- dsImaging:::.asset_db_connect())
  on.exit(dsImaging:::.asset_db_close(db), add = TRUE)
  expect_identical(
    DBI::dbGetQuery(db, "PRAGMA busy_timeout")$timeout[[1L]], 5000L)
  expect_identical(DBI::dbGetQuery(
    db, "SELECT count(*) AS n FROM assets")$n[[1L]], 0L)
  child$wait(timeout = 5000)
  expect_identical(child$get_exit_status(), 0L,
                   info = child$read_all_error())
})

test_that("legacy catalog lookup closes a partial connection on failure", {
  path <- tempfile(fileext = ".sqlite")
  file.create(path)
  withr::local_options(list(dsimaging.asset_db = path))
  disconnected <- FALSE
  fake_db <- structure(list(), class = "dsimaging_test_connection")
  testthat::local_mocked_bindings(
    dbConnect = function(...) fake_db,
    dbExecute = function(...) stop("simulated pragma failure"),
    dbIsValid = function(...) TRUE,
    dbDisconnect = function(...) {
      disconnected <<- TRUE
      TRUE
    },
    .package = "DBI")

  expect_null(dsImaging:::.get_asset_db())
  expect_true(disconnected)
})
