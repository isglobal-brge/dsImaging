.imaging_description_method_table <- function(field) {
  description <- packageDescription("dsImaging")
  methods <- trimws(strsplit(description[[field]], ",", fixed = TRUE)[[1]])
  data.frame(
    name = methods,
    value = paste0("dsImaging::", methods),
    package = "dsImaging",
    version = as.character(packageVersion("dsImaging")),
    type = if (identical(field, "AggregateMethods")) "aggregate" else "assign",
    class = "function",
    stringsAsFactors = FALSE
  )
}

.imaging_dslite_config <- function() {
  config <- DSLite::defaultDSConfiguration()
  config$AggregateMethods <- .imaging_description_method_table(
    "AggregateMethods")
  config$AssignMethods <- .imaging_description_method_table("AssignMethods")
  config
}

test_that("every registered dsImaging method validates lazy arguments first", {
  description <- packageDescription("dsImaging")
  methods <- unique(unlist(lapply(c("AggregateMethods", "AssignMethods"),
    function(field) trimws(strsplit(
      description[[field]], ",", fixed = TRUE)[[1]]))))

  for (method in methods) {
    fn <- get(method, envir = asNamespace("dsImaging"), inherits = FALSE)
    expect_identical(body(fn)[[2L]],
                     quote(.dsimaging_require_literal_arguments()),
                     info = method)
    if (length(formals(fn)) == 0L) next

    marker <- new.env(parent = emptyenv())
    marker$ran <- FALSE
    nested <- substitute({
      MARKER$ran <- TRUE
      NULL
    }, list(MARKER = marker))
    expression <- as.call(list(as.name(method), nested))
    expect_error(eval(expression, envir = asNamespace("dsImaging")),
                 "literal values or assigned server symbols", info = method)
    expect_false(marker$ran, info = method)
  }
})

test_that("DSLite nesting cannot invoke the dsImaging publisher ABI", {
  skip_if_not_installed("DSLite")
  skip_if_not_installed("DSI")

  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(dsimaging.asset_db = db_path))
  db <- dsImaging:::.asset_db_connect()
  before <- DBI::dbGetQuery(db, "SELECT COUNT(*) AS n FROM assets")$n[[1L]]
  dsImaging:::.asset_db_close(db)

  config <- .imaging_dslite_config()
  config$AggregateMethods <- rbind(config$AggregateMethods, data.frame(
    name = "identity", value = "base::identity", package = "base",
    version = as.character(getRversion()), type = "aggregate",
    class = "function", stringsAsFactors = FALSE))
  server <- DSLite::newDSLiteServer(config = config, strict = TRUE)
  server_name <- paste0("dsimaging_expression_server_", Sys.getpid())
  assign(server_name, server, envir = .GlobalEnv)
  withr::defer(rm(list = server_name, envir = .GlobalEnv))
  connection <- DSI::dsConnect(DSLite::DSLite(), name = "site",
                               url = server_name)
  withr::defer(DSI::dsDisconnect(connection))

  expression <- call("identity", call("register_derived_asset",
    "privacy.test", "feature_table", tempfile()))
  expect_error(
    DSI::dsFetch(DSI::dsAggregate(connection, expression, async = FALSE)),
    "publisher|does not allow expression")

  db <- dsImaging:::.asset_db_connect()
  after <- DBI::dbGetQuery(db, "SELECT COUNT(*) AS n FROM assets")$n[[1L]]
  dsImaging:::.asset_db_close(db)
  expect_identical(after, before)
})

test_that("publisher ABI accepts only a dsImaging namespace caller", {
  db_path <- tempfile(fileext = ".sqlite")
  withr::local_options(list(dsimaging.asset_db = db_path))
  artifact <- tempfile(fileext = ".csv")
  writeLines("sample_id,feature", artifact)

  expect_error(register_derived_asset(
    "privacy.test", "feature_table", artifact),
    "only to the dsImaging publisher")

  trusted_publisher_call <- function(...) register_derived_asset(...)
  environment(trusted_publisher_call) <- asNamespace("dsImaging")
  asset_id <- trusted_publisher_call(
    "privacy.test", "feature_table", artifact)
  expect_match(asset_id, "^asset_[0-9a-f]{32}$")
})

test_that("registered dsImaging outer calls reject nested calls in DSLite", {
  skip_if_not_installed("DSLite")
  skip_if_not_installed("DSI")

  config <- .imaging_dslite_config()
  server <- DSLite::newDSLiteServer(config = config, strict = TRUE)
  server_name <- paste0("dsimaging_outer_server_", Sys.getpid())
  assign(server_name, server, envir = .GlobalEnv)
  withr::defer(rm(list = server_name, envir = .GlobalEnv))
  connection <- DSI::dsConnect(DSLite::DSLite(), name = "site",
                               url = server_name)
  withr::defer(DSI::dsDisconnect(connection))

  expression <- call("imagingMetadataDS", call("imagingListModelsDS"))
  expect_error(
    DSI::dsFetch(DSI::dsAggregate(connection, expression, async = FALSE)),
    "literal values or assigned server symbols")
})
