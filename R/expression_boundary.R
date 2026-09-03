# Module: DataSHIELD Expression Boundary

#' Require side-effect-free DataSHIELD argument expressions
#'
#' DSLite and some server evaluators validate only the outer allowlisted call.
#' Inspect the still-lazy arguments before any public method forces them so an
#' unregistered nested call cannot execute as a promise.
#' @keywords internal
.dsimaging_require_literal_arguments <- function(call = sys.call(-1L)) {
  if (!is.call(call)) {
    stop("DataSHIELD method arguments could not be validated.",
         call. = FALSE)
  }
  arguments <- as.list(call)[-1L]
  safe <- vapply(arguments, function(expr) {
    is.symbol(expr) || is.atomic(expr) || is.null(expr)
  }, logical(1))
  if (any(!safe)) {
    stop("DataSHIELD method arguments must be literal values or assigned ",
         "server symbols.", call. = FALSE)
  }
  invisible(TRUE)
}

#' Require the dsHPC publisher hook to run through dsImaging server code
#'
#' `register_derived_asset()` is exported for the publisher plugin ABI, not as
#' an analyst operation. A direct or promise-nested DataSHIELD expression has
#' no dsImaging namespace caller before the protected function.
#' @keywords internal
.dsimaging_require_publisher_caller <- function() {
  deny <- function() {
    stop("Asset registration is available only to the dsImaging publisher.",
         call. = FALSE)
  }
  n <- sys.nframe()
  if (n <= 2L) deny()
  for (i in seq_len(n - 2L)) {
    fn <- tryCatch(sys.function(i), error = function(e) NULL)
    if (is.null(fn)) next
    env <- environment(fn)
    if (isNamespace(env) &&
        identical(as.character(getNamespaceName(env)), "dsImaging")) {
      return(invisible(TRUE))
    }
  }
  deny()
}
