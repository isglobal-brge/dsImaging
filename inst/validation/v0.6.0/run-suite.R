args <- commandArgs(trailingOnly = TRUE)
stopifnot(length(args) == 3L)
package <- args[[1L]]
source <- normalizePath(args[[2L]], mustWork = TRUE)
output <- args[[3L]]
result <- testthat::test_local(source, reporter = "summary", stop_on_failure = FALSE)
rows <- as.data.frame(result)
rows$result <- NULL
utils::write.csv(rows, output, row.names = FALSE)
counts <- c(tests = nrow(rows), passed = sum(rows$passed),
  failed = sum(rows$failed), errors = sum(rows$error),
  warnings = sum(rows$warning), skipped = sum(rows$skipped))
cat("\nVALIDATION ", package, " ", paste(names(counts), counts, sep = "=", collapse = " "), "\n", sep = "")
if (any(counts[c("failed", "errors")] != 0)) quit(status = 1L)
