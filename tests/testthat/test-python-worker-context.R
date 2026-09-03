test_that("runner format gates admit TIFF and reject detached containers", {
  python <- Sys.which("python3")
  skip_if(!nzchar(python), "python3 is unavailable")
  python_dir <- system.file("python", package = "dsImaging")
  if (!nzchar(python_dir)) {
    python_dir <- normalizePath(
      testthat::test_path("..", "..", "inst", "python"), mustWork = TRUE)
  }
  root <- withr::local_tempdir()
  paths <- c(
    tif = file.path(root, "image.tif"),
    tiff = file.path(root, "mask.tiff"),
    nrrd = file.path(root, "detached.nrrd"),
    mhd = file.path(root, "detached.mhd"),
    mha = file.path(root, "detached.mha"),
    inline_nrrd = file.path(root, "inline.nrrd"),
    inline_mha = file.path(root, "inline.mha"))
  writeBin(charToRaw("raster"), paths[["tif"]])
  writeBin(charToRaw("mask"), paths[["tiff"]])
  writeBin(charToRaw(paste0(
    "NRRD0005\ntype: uchar\ndimension: 2\nsizes: 1 1\n",
    "encoding: raw\ndata file: private.raw\n\n")), paths[["nrrd"]])
  writeBin(charToRaw(
    "ObjectType = Image\nNDims = 2\nElementDataFile = private.raw\n"),
    paths[["mhd"]])
  writeBin(charToRaw(
    "ObjectType = Image\nNDims = 2\nElementDataFile = private.raw\n"),
    paths[["mha"]])
  writeBin(c(charToRaw(paste0(
    "NRRD0005\ntype: uchar\ndimension: 2\nsizes: 1 1\n",
    "encoding: raw\n\n")), as.raw(1L)), paths[["inline_nrrd"]])
  writeBin(c(charToRaw(paste0(
    "ObjectType = Image\nNDims = 2\nDimSize = 1 1\n",
    "ElementType = MET_UCHAR\nElementDataFile = LOCAL\n")), as.raw(1L)),
    paths[["inline_mha"]])
  env <- c(
    PYTHONPATH = paste(python_dir, Sys.getenv("PYTHONPATH"),
                       sep = .Platform$path.sep),
    FORMAT_PATHS = jsonlite::toJSON(as.list(paths), auto_unbox = TRUE))
  code <- paste(
    "import json, os",
    "from dsimaging_utils import (IMAGE_EXTS, MASK_EXTS, strip_extensions,",
    "                             validate_input_file)",
    "paths = json.loads(os.environ['FORMAT_PATHS'])",
    "def result(name):",
    "    try:",
    "        validate_input_file(paths[name], IMAGE_EXTS)",
    "        return 'accepted'",
    "    except RuntimeError as exc:",
    "        return str(exc)",
    "print(json.dumps({",
    "    'image_exts': IMAGE_EXTS, 'mask_exts': MASK_EXTS,",
    "    'stem': strip_extensions('scan.tiff'), 'tif': result('tif'),",
    "    'tiff': result('tiff'),",
    "    'nrrd': result('nrrd'), 'mhd': result('mhd'),",
    "    'mha': result('mha'), 'inline_nrrd': result('inline_nrrd'),",
    "    'inline_mha': result('inline_mha')}))",
    sep = "\n")
  result <- processx::run(python, c("-c", code), env = env,
                          error_on_status = FALSE)
  expect_equal(result$status, 0L, info = result$stderr)
  observed <- jsonlite::fromJSON(result$stdout)

  expect_true(all(c(".tif", ".tiff") %in% observed$image_exts))
  expect_true(all(c(".tif", ".tiff") %in% observed$mask_exts))
  expect_false(".mhd" %in% observed$image_exts)
  expect_false(".mhd" %in% observed$mask_exts)
  expect_identical(observed$stem, "scan")
  expect_identical(observed$tif, "accepted")
  expect_identical(observed$tiff, "accepted")
  expect_match(observed$nrrd, "Detached NRRD")
  expect_match(observed$mhd, "Detached MetaImage")
  expect_match(observed$mha, "Detached MetaImage")
  expect_identical(observed$inline_nrrd, "accepted")
  expect_identical(observed$inline_mha, "accepted")
})

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

test_that("worker contexts map source masks by the exact admitted roster", {
  skip_if_not_installed("arrow")
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
  ids <- c("case-A", "case-B", "case-C")
  collection <- write_test_imaging_collection(
    root, "private.dataset",
    data.frame(sample_id = ids, patient_id = paste0("patient-", 1:3)))
  masks <- file.path(root, "source", "masks")
  dir.create(masks)
  mask_paths <- file.path(masks, paste0("opaque-", 1:3, ".nii.gz"))
  for (i in seq_along(mask_paths)) {
    writeBin(charToRaw(paste0("test-mask-", ids[[i]])), mask_paths[[i]])
  }
  mask_index <- file.path(root, "indexes", "masks.parquet")
  reverse <- rev(seq_along(ids))
  arrow::write_parquet(data.frame(
    sample_id = ids[reverse],
    uri = mask_paths[reverse],
    content_hash = vapply(mask_paths[reverse], digest::digest, character(1),
      algo = "sha256", file = TRUE),
    size = as.numeric(file.info(mask_paths[reverse])$size),
    source_kind = "mask_file",
    stringsAsFactors = FALSE
  ), mask_index)
  collection$manifest$assets$masks <- list(
    kind = "mask_root", uri = masks, content_hash_index = mask_index)
  yaml::write_yaml(collection$manifest, collection$manifest_path)

  backend <- storage_backend("file")
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(collection$manifest), "img",
    backend = backend, manifest_uri = collection$manifest_path,
    require_snapshot = TRUE)
  context_root <- file.path(root, "contexts")
  withr::local_options(list(
    dsimaging.registry_path = file.path(root, "registry.yaml"),
    dsimaging.worker_context_dir = context_root))
  authorized <- list(
    handle = handle, dataset_id = handle$dataset_id,
    manifest = handle$manifest, backend = handle$backend,
    privacy_roster = handle$privacy_roster,
    collection_snapshot = handle$collection_snapshot)
  context_id <- dsImaging:::.register_imaging_worker_context(
    authorized, asset_names = c("images", "masks"))
  context_path <- dsImaging:::.imaging_worker_context_file(context_id)
  context <- yaml::read_yaml(context_path)

  expect_identical(unlist(context$collection_map$asset_names,
                          use.names = FALSE), c("images", "masks"))
  expect_identical(
    vapply(context$collection_map$records_by_asset$masks, `[[`,
           character(1), "sample_id"), ids)
  expect_identical(
    vapply(context$collection_map$records_by_asset$masks, `[[`,
           character(1), "content_hash"),
    unname(vapply(mask_paths, digest::digest, character(1),
                  algo = "sha256", file = TRUE)))

  env <- c(
    PYTHONPATH = paste(python_dir, Sys.getenv("PYTHONPATH"),
                       sep = .Platform$path.sep),
    DSIMAGING_WORKER_CONTEXT_DIR = context_root,
    DSHPC_CFG_DATASET_ID = context_id,
    DSHPC_CFG_WORKER_CONTEXT = context_path,
    DSHPC_INPUT_DIR = "")
  code <- paste(
    "import json",
    "from dsimaging_utils import mapped_sample_files, IMAGE_EXTS, MASK_EXTS",
    "images = mapped_sample_files('images', 'images', extensions=IMAGE_EXTS)",
    "masks = mapped_sample_files('masks', 'masks', extensions=MASK_EXTS)",
    "print(json.dumps({'images': [sid for _, sid in images],",
    "                  'masks': [sid for _, sid in masks]}))",
    sep = "\n")
  result <- processx::run(python, c("-c", code), env = env,
                          error_on_status = FALSE)
  expect_equal(result$status, 0L, info = result$stderr)
  mapped <- jsonlite::fromJSON(result$stdout)
  expect_identical(mapped$images, ids)
  expect_identical(mapped$masks, ids)

  original_size <- file.info(mask_paths[[1L]])$size
  writeBin(charToRaw(strrep("x", original_size)), mask_paths[[1L]])
  result <- processx::run(python, c("-c", code), env = env,
                          error_on_status = FALSE)
  expect_false(result$status == 0L)
  expect_match(result$stderr, "integrity verification")

  incomplete <- arrow::read_parquet(mask_index)
  arrow::write_parquet(incomplete[-1L, ], mask_index)
  expect_error(dsImaging:::.register_imaging_worker_context(
    authorized, asset_names = c("images", "masks")),
    "complete admitted collection", fixed = TRUE)
})

test_that("worker contexts resolve catalog mask aliases from output manifests", {
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
  ids <- c("case-A", "case-B", "case-C")
  collection <- write_test_imaging_collection(
    root, "catalog.mask.alias",
    data.frame(sample_id = ids, patient_id = paste0("patient-", 1:3)))
  artifact <- file.path(root, "derived-masks")
  dir.create(artifact)
  mask_paths <- file.path(artifact, paste0("opaque-", 1:3, ".nii.gz"))
  for (i in seq_along(mask_paths)) {
    writeBin(charToRaw(paste0("derived-mask-", ids[[i]])), mask_paths[[i]])
  }
  mapped_sample <- function(i) {
    relative <- basename(mask_paths[[i]])
    list(
      sample_id = ids[[i]], primary = relative, files = list(relative),
      file_integrity = list(list(
        path = relative, size = as.numeric(file.info(mask_paths[[i]])$size),
        sha256 = digest::digest(mask_paths[[i]], algo = "sha256", file = TRUE))))
  }
  jsonlite::write_json(list(
    schema_version = 1L, artifact_type = "mask_root",
    samples = lapply(rev(seq_along(ids)), mapped_sample)),
    file.path(artifact, "dsimaging_output_manifest.json"),
    auto_unbox = TRUE, pretty = TRUE)

  backend <- storage_backend("file")
  handle <- dsImaging:::.make_imaging_handle(
    imaging_dataset_descriptor(collection$manifest), "img",
    backend = backend, manifest_uri = collection$manifest_path,
    require_snapshot = TRUE)
  context_root <- file.path(root, "contexts")
  withr::local_options(list(
    dsimaging.asset_db = file.path(root, "assets.sqlite"),
    dsimaging.registry_path = file.path(root, "registry.yaml"),
    dsimaging.worker_context_dir = context_root))
  db <- dsImaging:::.asset_db_connect()
  asset_id <- dsImaging:::.asset_register(
    db, handle$dataset_id, "mask_root", artifact,
    derivation_hash = "catalog-mask-fallback", visibility = "global",
    collection_seal = handle$collection_seal)
  dsImaging:::.asset_set_alias(
    db, handle$dataset_id, "catalog_masks", asset_id)
  dsImaging:::.asset_db_close(db)

  authorized <- list(
    handle = handle, dataset_id = handle$dataset_id,
    manifest = handle$manifest, backend = handle$backend,
    privacy_roster = handle$privacy_roster,
    collection_snapshot = handle$collection_snapshot)
  context_id <- dsImaging:::.register_imaging_worker_context(
    authorized, asset_names = c("images", "catalog_masks"))
  context_path <- dsImaging:::.imaging_worker_context_file(context_id)
  context <- yaml::read_yaml(context_path)

  expect_false("catalog_masks" %in% unlist(
    context$collection_map$asset_names, use.names = FALSE))
  expect_identical(
    context$manifest$assets$catalog_masks$.dsimaging_asset_identity$asset_id,
    asset_id)

  env <- c(
    PYTHONPATH = paste(python_dir, Sys.getenv("PYTHONPATH"),
                       sep = .Platform$path.sep),
    DSIMAGING_WORKER_CONTEXT_DIR = context_root,
    DSHPC_CFG_DATASET_ID = context_id,
    DSHPC_CFG_WORKER_CONTEXT = context_path,
    DSHPC_INPUT_DIR = "")
  code <- paste(
    "import json",
    "from dsimaging_utils import mapped_sample_files, MASK_EXTS",
    "items = mapped_sample_files('catalog_masks', 'masks',",
    "    artifact_types=('mask_root',), extensions=MASK_EXTS)",
    "print(json.dumps([[sid, path] for path, sid in items]))",
    sep = "\n")
  result <- processx::run(python, c("-c", code), env = env,
                          error_on_status = FALSE)
  expect_equal(result$status, 0L, info = result$stderr)
  mapped <- jsonlite::fromJSON(result$stdout)
  expect_identical(mapped[, 1], ids)
  expect_identical(basename(mapped[, 2]), basename(mask_paths))
})

test_that("versioned S3 source maps use isolated cache entries", {
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
  context_id <- paste0("dsctx_", strrep("f", 64))
  context_path <- file.path(root, paste0(context_id, ".context.yaml"))
  payloads <- c("version-one", "version-two")
  records <- lapply(seq_along(payloads), function(i) list(
    sample_id = "case-A", source_kind = "single_file",
    uri = "s3://imaging-data/source/images/opaque.nii.gz",
    relative_path = "opaque.nii.gz",
    content_hash = digest::digest(charToRaw(payloads[[i]]),
                                  algo = "sha256", serialize = FALSE),
    size = nchar(payloads[[i]], type = "bytes"), n_files = 1L,
    version_id = paste0("version-", i)))
  context <- list(
    schema_version = 1L, context_id = context_id,
    manifest = list(
      dataset_id = "private.dataset",
      assets = list(images = list(
        uri = "s3://imaging-data/source/images/")),
      .dsimaging_privacy_roster = list(sample_ids = list("case-A"))),
    backend = list(type = "s3", config = list()),
    collection_map = list(
      version = 1L, seal = strrep("a", 64),
      asset_names = list("images"), records = list(records[[1L]]),
      records_by_asset = list(images = list(records[[1L]]))))
  yaml::write_yaml(context, context_path)
  env <- c(
    PYTHONPATH = paste(python_dir, Sys.getenv("PYTHONPATH"),
                       sep = .Platform$path.sep),
    DSIMAGING_WORKER_CONTEXT_DIR = root,
    DSIMAGING_CACHE_DIR = file.path(root, "cache"),
    DSHPC_CFG_DATASET_ID = context_id,
    DSHPC_CFG_WORKER_CONTEXT = context_path,
    PAYLOAD_ONE = payloads[[1L]], PAYLOAD_TWO = payloads[[2L]],
    HASH_TWO = records[[2L]]$content_hash)
  code <- paste(
    "import json, os, yaml",
    "import dsimaging_utils as utils",
    "payloads = {'version-1': os.environ['PAYLOAD_ONE'].encode(),",
    "            'version-2': os.environ['PAYLOAD_TWO'].encode()}",
    "class FakeS3:",
    "    def __init__(self): self.calls = []",
    "    def download_file(self, bucket, key, dest, ExtraArgs=None):",
    "        version = (ExtraArgs or {}).get('VersionId')",
    "        self.calls.append(version)",
    "        with open(dest, 'wb') as handle: handle.write(payloads[version])",
    "fake = FakeS3()",
    "utils.s3_client_for_entry = lambda entry: fake",
    "first = utils.collection_sample_files('images')[0][0]",
    "with open(os.environ['DSHPC_CFG_WORKER_CONTEXT']) as handle:",
    "    context = yaml.safe_load(handle)",
    "record = context['collection_map']['records_by_asset']['images'][0]",
    "record['version_id'] = 'version-2'",
    "record['content_hash'] = os.environ['HASH_TWO']",
    "record['size'] = len(payloads['version-2'])",
    "context['collection_map']['records'][0] = dict(record)",
    "with open(os.environ['DSHPC_CFG_WORKER_CONTEXT'], 'w') as handle:",
    "    yaml.safe_dump(context, handle)",
    "second = utils.collection_sample_files('images')[0][0]",
    "print(json.dumps({'calls': fake.calls, 'different': first != second,",
    "                  'payload': open(second, 'rb').read().decode()}))",
    sep = "\n")
  result <- processx::run(python, c("-c", code), env = env,
                          error_on_status = FALSE)
  expect_equal(result$status, 0L, info = result$stderr)
  observed <- jsonlite::fromJSON(result$stdout)
  expect_identical(observed$calls, c("version-1", "version-2"))
  expect_true(observed$different)
  expect_identical(observed$payload, payloads[[2L]])
})

test_that("PyRadiomics consumes exact source image and mask maps", {
  python <- Sys.which("python3")
  skip_if(!nzchar(python), "python3 is unavailable")
  dependencies <- processx::run(python, c("-c", paste(
    "import yaml, numpy, SimpleITK, radiomics, pandas, pyarrow",
    sep = "\n")), error_on_status = FALSE)
  skip_if(dependencies$status != 0L,
          "PyRadiomics integration dependencies are unavailable")

  python_dir <- system.file("python", package = "dsImaging")
  if (!nzchar(python_dir)) {
    python_dir <- normalizePath(
      testthat::test_path("..", "..", "inst", "python"), mustWork = TRUE)
  }
  profile <- system.file(
    "profiles", "demo_ct_firstorder_v1.yaml", package = "dsImaging")
  if (!nzchar(profile)) {
    profile <- normalizePath(testthat::test_path(
      "..", "..", "inst", "profiles", "demo_ct_firstorder_v1.yaml"),
      mustWork = TRUE)
  }
  root <- withr::local_tempdir()
  images <- file.path(root, "images")
  masks <- file.path(root, "masks")
  dir.create(images)
  dir.create(masks)
  ids <- c("case-NIFTI", "case-TIF", "case-TIFF")
  image_paths <- file.path(images, c(
    "opaque-image.nii.gz", "opaque-image.tif", "opaque-image.tiff"))
  mask_paths <- file.path(masks, c(
    "opaque-mask.nii.gz", "opaque-mask.tiff", "opaque-mask.tif"))
  generation <- paste(
    "import os, numpy as np, SimpleITK as sitk",
    "zz, yy, xx = np.indices((16, 16, 16))",
    "image = (zz + yy * 2 + xx * 3).astype(np.uint16)",
    "mask = np.zeros((16, 16, 16), dtype=np.uint8)",
    "mask[3:13, 3:13, 3:13] = 1",
    "for path in os.environ['IMAGES'].split(os.pathsep):",
    "    sitk.WriteImage(sitk.GetImageFromArray(image), path)",
    "for path in os.environ['MASKS'].split(os.pathsep):",
    "    sitk.WriteImage(sitk.GetImageFromArray(mask), path)",
    sep = "\n")
  generated <- processx::run(python, c("-c", generation),
    env = c(IMAGES = paste(image_paths, collapse = .Platform$path.sep),
            MASKS = paste(mask_paths, collapse = .Platform$path.sep)),
    error_on_status = FALSE)
  expect_equal(generated$status, 0L, info = generated$stderr)

  make_record <- function(path, sample_id, kind) list(
    sample_id = sample_id, source_kind = kind, uri = path,
    relative_path = basename(path),
    content_hash = digest::digest(path, algo = "sha256", file = TRUE),
    size = as.numeric(file.info(path)$size), n_files = 1L)
  image_records <- unname(Map(make_record, image_paths, ids,
                              MoreArgs = list(kind = "single_file")))
  mask_records <- unname(Map(make_record, mask_paths, ids,
                             MoreArgs = list(kind = "mask_file")))
  context_id <- paste0("dsctx_", strrep("b", 64))
  context_path <- file.path(root, paste0(context_id, ".context.yaml"))
  yaml::write_yaml(list(
    schema_version = 1L, context_id = context_id,
    manifest = list(
      dataset_id = "private.dataset",
      assets = list(images = list(uri = images), masks = list(uri = masks)),
      .dsimaging_privacy_roster = list(sample_ids = as.list(ids))),
    backend = list(type = "file", config = list()),
    collection_map = list(
      version = 1L, seal = strrep("a", 64),
      asset_names = list("images", "masks"),
      records = image_records,
      records_by_asset = list(
        images = image_records, masks = mask_records))),
    context_path)
  env <- c(
    PYTHONPATH = paste(python_dir, Sys.getenv("PYTHONPATH"),
                       sep = .Platform$path.sep),
    DSIMAGING_WORKER_CONTEXT_DIR = root,
    DSHPC_CFG_DATASET_ID = context_id,
    DSHPC_CFG_WORKER_CONTEXT = context_path,
    DSHPC_CFG_IMAGE_ASSET = "images",
    DSHPC_CFG_MASK_ASSET = "masks",
    DSHPC_INPUT_DIR = "")
  output <- file.path(root, "output")
  result <- processx::run(python, c(
    file.path(python_dir, "dsimaging_extract.py"),
    "--input", root, "--output", output, "--settings", profile),
    env = env, error_on_status = FALSE)
  expect_equal(result$status, 0L, info = result$stderr)
  summary <- jsonlite::read_json(
    file.path(output, "extraction_summary.json"), simplifyVector = TRUE)
  expect_identical(summary$n_samples, 3L)
  expect_gt(summary$n_features, 0L)
  expect_true(file.exists(file.path(output, "radiomics.parquet")))

  mask_size <- file.info(mask_paths[[2L]])$size
  writeBin(charToRaw(strrep("x", mask_size)), mask_paths[[2L]])
  failed <- processx::run(python, c(
    file.path(python_dir, "dsimaging_extract.py"),
    "--input", root, "--output", file.path(root, "tampered"),
    "--settings", profile), env = env, error_on_status = FALSE)
  expect_false(failed$status == 0L)
  expect_match(failed$stderr, "Admitted imaging inputs are unavailable")
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
