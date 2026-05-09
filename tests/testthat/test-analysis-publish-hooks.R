test_that("publisher provenance captures runner summary versions", {
  out <- tempfile("runner-output-")
  dir.create(out)
  writeLines(jsonlite::toJSON(list(
    n_samples = 1L,
    n_features = 107L,
    columns = paste0("f", seq_len(200)),
    versions = list(python = "3.11.14", radiomics = "v3.0.1")
  ), auto_unbox = TRUE, pretty = TRUE),
  file.path(out, "extraction_summary.json"))

  meta <- dsImaging:::.imaging_output_metadata(out)

  expect_equal(meta$summaries$extraction_summary$n_samples, 1L)
  expect_null(meta$summaries$extraction_summary$columns)
  expect_equal(meta$runner_versions$extraction_summary$radiomics, "v3.0.1")
})
