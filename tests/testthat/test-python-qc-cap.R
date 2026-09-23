test_that("QC caps thumbnails while retaining and validating the complete roster", {
  python <- Sys.which("python3")
  skip_if(!nzchar(python), "python3 is unavailable")
  dependencies <- processx::run(python,
    c("-c", "import yaml, numpy, SimpleITK, PIL"), error_on_status = FALSE)
  skip_if(dependencies$status != 0L, "QC imaging dependencies are unavailable")
  python_dir <- system.file("python", package = "dsImaging")
  if (!nzchar(python_dir)) python_dir <- normalizePath(
    testthat::test_path("..", "..", "inst", "python"), mustWork = TRUE)
  root <- withr::local_tempdir()
  images <- file.path(root, "images")
  dir.create(images)
  ids <- sprintf("private-sample-%03d", 66:1)
  paths <- file.path(images, sprintf("opaque-%03d.mha", seq_along(ids)))
  generated <- processx::run(python, c("-c", paste(
    "import os, numpy as np, SimpleITK as sitk",
    "image = sitk.GetImageFromArray(np.arange(1024, dtype=np.uint16).reshape(32, 32))",
    "for path in os.environ['QC_IMAGES'].split(os.pathsep):",
    "    sitk.WriteImage(image, path)", sep = "\n")),
    env = c(QC_IMAGES = paste(paths, collapse = .Platform$path.sep)),
    error_on_status = FALSE)
  expect_equal(generated$status, 0L, info = generated$stderr)
  records <- unname(Map(function(path, sid) list(
    sample_id = sid, source_kind = "single_file", uri = path,
    relative_path = basename(path), n_files = 1L,
    content_hash = digest::digest(path, file = TRUE, algo = "sha256"),
    size = as.numeric(file.info(path)$size)), paths, ids))
  context_id <- paste0("dsctx_", strrep("c", 64))
  context_path <- file.path(root, paste0(context_id, ".context.yaml"))
  yaml::write_yaml(list(schema_version = 1L, context_id = context_id,
    manifest = list(dataset_id = "private.dataset",
      assets = list(images = list(uri = images)),
      .dsimaging_privacy_roster = list(sample_ids = as.list(ids))),
    backend = list(type = "file", config = list()),
    collection_map = list(version = 1L, seal = strrep("a", 64),
      asset_names = list("images"), records = records,
      records_by_asset = list(images = records))), context_path)
  env <- c(PYTHONPATH = paste(python_dir, Sys.getenv("PYTHONPATH"),
      sep = .Platform$path.sep), DSIMAGING_WORKER_CONTEXT_DIR = root,
    DSHPC_CFG_DATASET_ID = context_id, DSHPC_CFG_WORKER_CONTEXT = context_path,
    DSHPC_CFG_IMAGE_ASSET = "images", DSHPC_CFG_MASK_ASSET = "",
    DSHPC_CFG_MAX_SIZE = "16", DSHPC_CFG_MAX_TILES = "", DSHPC_INPUT_DIR = "")
  run_qc <- function(name, cap = "") processx::run(python, c(
    file.path(python_dir, "dsimaging_qc_visuals.py"),
    "--input", root, "--output", file.path(root, name)),
    env = replace(env, "DSHPC_CFG_MAX_TILES", cap), error_on_status = FALSE)
  result <- run_qc("capped", "2")
  expect_equal(result$status, 0L, info = result$stderr)
  output <- file.path(root, "capped")
  expect_length(list.files(output, pattern = "[.]png$"), 2L)
  table <- utils::read.csv(file.path(output, "qc_visual_manifest.csv"))
  expect_named(table, c("sample_id", "qc_id", "file", "has_mask", "width_px"))
  expect_equal(table$sample_id, sort(ids)[1:2])
  expect_true(all(grepl("^case_[0-9]{4}_[a-f0-9]{12}[.]png$", table$file)))
  mapping <- jsonlite::read_json(file.path(output, "dsimaging_output_manifest.json"))
  expect_setequal(vapply(mapping$samples, `[[`, character(1), "sample_id"), ids)
  omitted <- Filter(function(x) identical(x$status, "omitted_by_cap"), mapping$samples)
  expect_length(omitted, 64L)
  expect_true(all(vapply(omitted, function(x) is.null(x$primary) &&
    length(x$files) == 0L && length(x$file_integrity) == 0L, logical(1))))
  roster <- dsImaging:::.new_imaging_privacy_roster(ids, paste0("patient-", ids))
  expect_true(dsImaging:::.assert_mapped_imaging_output(output,
    "qc_visual_asset", roster, config = list(max_tiles = 2L)))
  dims <- processx::run(python, c("-c", paste(
    "import glob, os, json",
    "from PIL import Image",
    "print(json.dumps([Image.open(p).size for p in glob.glob(os.path.join(os.environ['QC_OUTPUT'], '*.png'))]))",
    sep = "\n")), env = c(QC_OUTPUT = output))
  expect_true(all(jsonlite::fromJSON(dims$stdout) <= 16L))
  default <- run_qc("default")
  expect_equal(default$status, 0L, info = default$stderr)
  expect_length(list.files(file.path(root, "default"), pattern = "[.]png$"), 64L)
  for (cap in c("0", "1025", "1.5", "invalid")) {
    invalid <- run_qc(paste0("invalid-", cap), cap)
    expect_gt(invalid$status, 0L)
    expect_match(invalid$stderr, "QC thumbnail bounds are invalid", fixed = TRUE)
  }
  # A capped-out sample must still pass the sealed input integrity checks.
  writeBin(charToRaw("tampered"), paths[[1]])
  tampered <- run_qc("tampered", "2")
  expect_gt(tampered$status, 0L)
  expect_match(tampered$stderr, "Admitted imaging inputs are unavailable", fixed = TRUE)
  expect_length(list.files(file.path(root, "tampered"), pattern = "[.]png$"), 0L)
})
