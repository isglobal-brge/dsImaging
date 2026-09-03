test_that("Python runners ignore global registries and arbitrary asset paths", {
  python <- Sys.which("python3")
  skip_if(!nzchar(python), "python3 is unavailable")
  yaml_check <- processx::run(python, c("-c", "import yaml"),
    error_on_status = FALSE)
  skip_if(yaml_check$status != 0L, "Python yaml module is unavailable")

  python_dir <- system.file("python", package = "dsImaging")
  if (!nzchar(python_dir)) {
    python_dir <- normalizePath(
      testthat::test_path("..", "..", "inst", "python"), mustWork = TRUE)
  }
  root <- withr::local_tempdir()
  authorized_asset <- file.path(root, "authorized-images")
  arbitrary_asset <- file.path(root, "other-dataset")
  dir.create(authorized_asset)
  dir.create(arbitrary_asset)
  context_id <- paste0("dsctx_", strrep("c", 64))
  context_path <- file.path(root, paste0(context_id, ".context.yaml"))
  jsonlite::write_json(list(
    schema_version = 1L,
    context_id = context_id,
    manifest = list(dataset_id = "private.dataset", assets = list(
      images = list(uri = authorized_asset))),
    backend = list(type = "file", config = list())
  ), context_path, auto_unbox = TRUE)

  outside_context <- file.path(dirname(root), paste0(context_id, ".yaml"))
  on.exit(unlink(outside_context), add = TRUE)
  jsonlite::write_json(list(
    schema_version = 1L,
    context_id = context_id,
    manifest = list(dataset_id = "other.dataset", assets = list(
      images = list(uri = arbitrary_asset))),
    backend = list(type = "file", config = list())
  ), outside_context, auto_unbox = TRUE)
  registry_path <- file.path(root, "registry.yaml")
  yaml::write_yaml(list(
    private.dataset = list(manifest = outside_context)), registry_path)

  base_env <- c(
    PYTHONPATH = paste(python_dir, Sys.getenv("PYTHONPATH"), sep = .Platform$path.sep),
    DSIMAGING_WORKER_CONTEXT_DIR = root,
    DSIMAGING_REGISTRY_PATH = registry_path,
    DSIMAGING_ASSET_DB = file.path(root, "global-assets.sqlite"),
    DSHPC_CFG_DATASET_ID = context_id,
    DSHPC_CFG_WORKER_CONTEXT = context_path,
    DSHPC_INPUT_DIR = "",
    DSHPC_CFG_INPUT_DIR = "",
    ARBITRARY_ASSET = arbitrary_asset
  )
  code <- paste(
    "import json, os",
    "from dsimaging_utils import resolve_asset_path",
    "print(json.dumps({",
    "  'authorized': resolve_asset_path('images', 'images', os.environ['ARBITRARY_ASSET']),",
    "  'unlisted': resolve_asset_path('secret', 'images', os.environ['ARBITRARY_ASSET'])",
    "}))",
    sep = "\n")
  result <- processx::run(python, c("-c", code), env = base_env,
    error_on_status = FALSE)
  expect_equal(result$status, 0L, info = result$stderr)
  resolved <- jsonlite::fromJSON(result$stdout)
  expect_identical(normalizePath(resolved$authorized),
                   normalizePath(authorized_asset))
  expect_null(resolved$unlisted)

  outside_env <- base_env
  outside_env[["DSHPC_CFG_WORKER_CONTEXT"]] <- outside_context
  result <- processx::run(python, c("-c", code), env = outside_env,
    error_on_status = FALSE)
  expect_equal(result$status, 0L, info = result$stderr)
  resolved <- jsonlite::fromJSON(result$stdout)
  expect_null(resolved$authorized)

  registry_env <- base_env
  registry_env[["DSHPC_CFG_DATASET_ID"]] <- "private.dataset"
  result <- processx::run(python, c("-c", code), env = registry_env,
    error_on_status = FALSE)
  expect_equal(result$status, 0L, info = result$stderr)
  resolved <- jsonlite::fromJSON(result$stdout)
  expect_null(resolved$authorized)
})

test_that("Python runners consume only the exact snapshot sample map", {
  python <- Sys.which("python3")
  skip_if(!nzchar(python), "python3 is unavailable")
  yaml_check <- processx::run(python, c("-c", "import yaml"),
    error_on_status = FALSE)
  skip_if(yaml_check$status != 0L, "Python yaml module is unavailable")

  python_dir <- system.file("python", package = "dsImaging")
  if (!nzchar(python_dir)) {
    python_dir <- normalizePath(
      testthat::test_path("..", "..", "inst", "python"), mustWork = TRUE)
  }
  root <- withr::local_tempdir()
  images <- file.path(root, "images")
  dir.create(file.path(images, "nested"), recursive = TRUE)
  admitted <- c(file.path(images, "unrelated-name.png"),
                file.path(images, "nested", "another-name.png"))
  writeBin(charToRaw("first"), admitted[[1L]])
  writeBin(charToRaw("second"), admitted[[2L]])
  writeBin(charToRaw("must-not-be-read"), file.path(images, "extra.png"))
  ids <- c("case-A", "case-B")
  hashes <- vapply(admitted, digest::digest, character(1),
                   algo = "sha256", file = TRUE)
  context_id <- paste0("dsctx_", strrep("d", 64))
  context_path <- file.path(root, paste0(context_id, ".context.yaml"))
  context <- list(
    schema_version = 1L,
    context_id = context_id,
    manifest = list(
      dataset_id = "private.dataset",
      assets = list(images = list(uri = images)),
      .dsimaging_privacy_roster = list(sample_ids = ids)),
    backend = list(type = "file", config = list()),
    collection_map = list(
      version = 1L, seal = strrep("a", 64), asset_names = list("images"),
      records = lapply(seq_along(ids), function(i) list(
        sample_id = ids[[i]], source_kind = "single_file",
        uri = admitted[[i]],
        relative_path = c("unrelated-name.png", "nested/another-name.png")[[i]],
        content_hash = hashes[[i]], size = file.info(admitted[[i]])$size,
        n_files = 1L))))
  yaml::write_yaml(context, context_path)
  env <- c(
    PYTHONPATH = paste(python_dir, Sys.getenv("PYTHONPATH"),
                       sep = .Platform$path.sep),
    DSIMAGING_WORKER_CONTEXT_DIR = root,
    DSHPC_CFG_DATASET_ID = context_id,
    DSHPC_CFG_WORKER_CONTEXT = context_path,
    DSHPC_INPUT_DIR = "")
  code <- paste(
    "import json",
    "from dsimaging_utils import collection_sample_files",
    "items = collection_sample_files('images')",
    "print(json.dumps([[sid, path] for path, sid in items]))",
    sep = "\n")
  result <- processx::run(python, c("-c", code), env = env,
                          error_on_status = FALSE)
  expect_equal(result$status, 0L, info = result$stderr)
  mapped <- jsonlite::fromJSON(result$stdout)
  expect_identical(mapped[, 1], ids)
  expect_false(any(grepl("extra.png", mapped[, 2], fixed = TRUE)))

  context$collection_map$records[[1L]]$content_hash <- strrep("0", 64)
  yaml::write_yaml(context, context_path)
  result <- processx::run(python, c("-c", code), env = env,
                          error_on_status = FALSE)
  expect_false(result$status == 0L)
  expect_match(result$stderr, "integrity verification")

  context$collection_map$records[[1L]]$content_hash <- hashes[[1L]]
  context$collection_map$records[[1L]]$source_kind <- "dicom_series"
  context$collection_map$records[[1L]]$n_files <- 2L
  yaml::write_yaml(context, context_path)
  result <- processx::run(python, c("-c", code), env = env,
                          error_on_status = FALSE)
  expect_false(result$status == 0L)
  expect_match(result$stderr, "Multi-file")
})

test_that("Python artifact maps reject subsets and cross-sample reuse", {
  python <- Sys.which("python3")
  skip_if(!nzchar(python), "python3 is unavailable")
  yaml_check <- processx::run(python, c("-c", "import yaml"),
    error_on_status = FALSE)
  skip_if(yaml_check$status != 0L, "Python yaml module is unavailable")

  python_dir <- system.file("python", package = "dsImaging")
  if (!nzchar(python_dir)) {
    python_dir <- normalizePath(
      testthat::test_path("..", "..", "inst", "python"), mustWork = TRUE)
  }
  root <- withr::local_tempdir()
  artifact <- file.path(root, "artifact")
  dir.create(artifact)
  writeBin(charToRaw("one"), file.path(artifact, "one.nii.gz"))
  writeBin(charToRaw("two"), file.path(artifact, "two.nii.gz"))
  ids <- c("case-A", "case-B")
  context_id <- paste0("dsctx_", strrep("e", 64))
  context_path <- file.path(root, paste0(context_id, ".context.yaml"))
  yaml::write_yaml(list(
    schema_version = 1L, context_id = context_id,
    manifest = list(dataset_id = "private.dataset", assets = list(),
      .dsimaging_privacy_roster = list(sample_ids = ids)),
    backend = list(type = "file", config = list())), context_path)
  output_manifest <- file.path(artifact, "dsimaging_output_manifest.json")
  write_manifest <- function(samples) jsonlite::write_json(list(
    schema_version = 1L, artifact_type = "mask_root", samples = samples),
    output_manifest, auto_unbox = TRUE, pretty = TRUE)
  mapped_sample <- function(sample_id, file) list(
    sample_id = sample_id, primary = file, files = list(file),
    file_integrity = list(list(
      path = file, size = file.info(file.path(artifact, file))$size,
      sha256 = digest::digest(file.path(artifact, file), algo = "sha256",
                              file = TRUE))))
  env <- c(
    PYTHONPATH = paste(python_dir, Sys.getenv("PYTHONPATH"),
                       sep = .Platform$path.sep),
    DSIMAGING_WORKER_CONTEXT_DIR = root,
    DSHPC_CFG_DATASET_ID = context_id,
    DSHPC_CFG_WORKER_CONTEXT = context_path,
    ARTIFACT_ROOT = artifact)
  code <- paste(
    "import os",
    "from dsimaging_utils import artifact_sample_files",
    "print(len(artifact_sample_files(os.environ['ARTIFACT_ROOT'], ('mask_root',))))",
    sep = "\n")

  write_manifest(list(
    mapped_sample(ids[[1L]], "one.nii.gz"),
    mapped_sample(ids[[2L]], "two.nii.gz")))
  result <- processx::run(python, c("-c", code), env = env,
                          error_on_status = FALSE)
  expect_equal(result$status, 0L, info = result$stderr)
  expect_identical(trimws(result$stdout), "2")

  write_manifest(list(mapped_sample(ids[[1L]], "one.nii.gz")))
  result <- processx::run(python, c("-c", code), env = env,
                          error_on_status = FALSE)
  expect_false(result$status == 0L)

  write_manifest(list(
    mapped_sample(ids[[1L]], "one.nii.gz"),
    mapped_sample(ids[[2L]], "one.nii.gz")))
  result <- processx::run(python, c("-c", code), env = env,
                          error_on_status = FALSE)
  expect_false(result$status == 0L)
  expect_match(result$stderr, "multiple samples")
})

test_that("Python operational names and logs do not expose private ids", {
  python <- Sys.which("python3")
  skip_if(!nzchar(python), "python3 is unavailable")

  python_dir <- system.file("python", package = "dsImaging")
  if (!nzchar(python_dir)) {
    python_dir <- normalizePath(
      testthat::test_path("..", "..", "inst", "python"), mustWork = TRUE)
  }
  root <- withr::local_tempdir()
  private_id <- "PHI_PATIENT_701_SCAN_A"
  env <- c(
    PYTHONPATH = paste(python_dir, Sys.getenv("PYTHONPATH"),
                       sep = .Platform$path.sep),
    PRIVATE_SAMPLE_ID = private_id)

  token_result <- processx::run(python, c(
    "-c",
    paste(
      "import os",
      "from dsimaging_utils import sample_token",
      "print(sample_token(os.environ['PRIVATE_SAMPLE_ID']))",
      sep = "\n")), env = env, error_on_status = FALSE)
  expect_equal(token_result$status, 0L, info = token_result$stderr)
  token <- trimws(token_result$stdout)
  expect_match(token, "^[0-9a-f]{64}$")
  expect_false(grepl(private_id, token, fixed = TRUE))
  expect_false(startsWith(token, "PHI"))

  output <- file.path(root, "output")
  result <- processx::run(python, c(
    file.path(python_dir, "dsimaging_extract.py"),
    "--input", root,
    "--output", output,
    "--image", file.path(root, paste0(private_id, ".nii.gz")),
    "--sample-id", private_id), env = env, error_on_status = FALSE)
  expect_false(result$status == 0L)
  expect_false(grepl(private_id, result$stdout, fixed = TRUE))
  expect_false(grepl(private_id, result$stderr, fixed = TRUE))
  expect_match(result$stderr, "No mask found for the admitted image")

  index_root <- file.path(root, private_id)
  dir.create(index_root)
  writeBin(charToRaw("image"), file.path(
    index_root, paste0(private_id, ".nii.gz")))
  index_result <- processx::run(python, c(
    file.path(python_dir, "build_hash_index.py"),
    "--image-root", index_root,
    "--output", file.path(root, "index.parquet")),
    env = env, error_on_status = FALSE)
  expect_equal(index_result$status, 0L, info = index_result$stderr)
  expect_false(grepl(private_id, index_result$stdout, fixed = TRUE))
  expect_false(grepl(private_id, index_result$stderr, fixed = TRUE))
})
