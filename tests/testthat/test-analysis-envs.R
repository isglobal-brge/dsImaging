test_that("radiomics envs are listed", {
  envs <- list_radiomics_envs()
  expect_true(is.data.frame(envs))
  expect_true(nrow(envs) >= 4)
  expect_true("radiomics" %in% envs$framework)
  expect_true("seg_totalseg" %in% envs$framework)
})

test_that("worker processes can recover administrator path configuration", {
  root <- withr::local_tempdir()
  venv_root <- file.path(root, "analysis-venvs")
  credentials_path <- file.path(root, "credentials.yaml")
  asset_db <- file.path(root, "assets.sqlite")
  withr::local_options(list(
    dsimaging.analysis.venv_root = NULL,
    default.dsimaging.analysis.venv_root = NULL,
    dsimaging.venv_root = NULL,
    default.dsimaging.venv_root = NULL,
    dsimaging.credentials_path = NULL,
    default.dsimaging.credentials_path = NULL,
    dsimaging.asset_db = NULL,
    default.dsimaging.asset_db = NULL
  ))
  withr::local_envvar(c(
    DSIMAGING_ANALYSIS_VENV_ROOT = venv_root,
    DSIMAGING_CREDENTIALS_PATH = credentials_path,
    DSIMAGING_ASSET_DB = asset_db
  ))

  expect_identical(
    dsImaging:::.imaging_analysis_option("venv_root"), venv_root)
  expect_identical(dsImaging:::.imaging_credentials_path(), credentials_path)
  expect_identical(dsImaging:::.asset_db_path(), asset_db)
})

test_that("runner registration pins environment paths for detached workers", {
  root <- withr::local_tempdir()
  dshpc_home <- file.path(root, "dshpc")
  imaging_home <- file.path(root, "imaging")
  venv_root <- file.path(root, "analysis-venvs")
  credentials_path <- file.path(root, "credentials.yaml")
  dir.create(file.path(dshpc_home, "runners"), recursive = TRUE,
             mode = "0770")
  withr::local_options(list(
    dshpc.home = dshpc_home,
    dsimaging.analysis.home = imaging_home,
    dsimaging.analysis.venv_root = NULL,
    default.dsimaging.analysis.venv_root = NULL,
    dsimaging.venv_root = NULL,
    default.dsimaging.venv_root = NULL,
    dsimaging.credentials_path = NULL,
    default.dsimaging.credentials_path = NULL
  ))
  withr::local_envvar(c(
    DSIMAGING_ANALYSIS_VENV_ROOT = venv_root,
    DSIMAGING_CREDENTIALS_PATH = credentials_path
  ))

  dsImaging:::.register_radiomics_runners()
  runner <- yaml::read_yaml(file.path(
    dshpc_home, "runners", "pyradiomics_extract.yml"))

  expect_identical(
    runner$python, file.path(venv_root, "radiomics", "bin", "python"))
  expect_identical(
    runner$env$DSIMAGING_CREDENTIALS_PATH, credentials_path)
})

test_that("runner health includes clinical imaging analysis runners", {
  runners <- dsImaging:::.radiomics_runner_health()
  expect_true("ct_lung_threshold" %in% runners$runner)
  expect_true("dicom_convert" %in% runners$runner)
  expect_true("dsimaging_image_preprocess" %in% runners$runner)
  expect_true("mask_ops" %in% runners$runner)
  expect_true("imaging_qc_metrics" %in% runners$runner)
  expect_true("rt_convert" %in% runners$runner)
  expect_true("rt_dose_plan" %in% runners$runner)
  expect_true("imaging_qc_visuals" %in% runners$runner)
  expect_true("image_spatial" %in% runners$runner)
  expect_true("wsi_tile" %in% runners$runner)
  expect_true("image_embeddings" %in% runners$runner)
})

test_that("radiomics runners declare scheduler resources", {
  home <- tempfile("dshpc_home")
  imaging_home <- tempfile("dsimaging_home")
  dir.create(file.path(home, "runners"), recursive = TRUE)
  withr::local_options(list(dshpc.home = home,
    dsimaging.analysis.home = imaging_home))
  on.exit(unlink(c(home, imaging_home), recursive = TRUE), add = TRUE)

  dsImaging:::.register_radiomics_runners()
  pyradiomics <- yaml::read_yaml(file.path(home, "runners", "pyradiomics_extract.yml"))
  lungmask <- yaml::read_yaml(file.path(home, "runners", "lungmask_infer.yml"))
  threshold <- yaml::read_yaml(file.path(home, "runners", "ct_lung_threshold.yml"))
  mask_ops <- yaml::read_yaml(file.path(home, "runners", "mask_ops.yml"))
  qc <- yaml::read_yaml(file.path(home, "runners", "imaging_qc_metrics.yml"))
  rt <- yaml::read_yaml(file.path(home, "runners", "rt_convert.yml"))
  embeddings <- yaml::read_yaml(file.path(home, "runners", "image_embeddings.yml"))

  expect_equal(pyradiomics$resources$memory_mb, 6144L)
  expect_equal(pyradiomics$resources$max_concurrent, 1L)
  expect_equal(pyradiomics$resources$oom_cooldown_secs, 120L)
  expect_match(pyradiomics$args_template[[1]],
    file.path(imaging_home, "runtime", "python"), fixed = TRUE)
  expect_false(grepl("00LOCK|site-library", pyradiomics$args_template[[1]]))
  expect_true(file.exists(pyradiomics$args_template[[1]]))
  expect_equal(lungmask$resources$max_concurrent, 1L)
  expect_equal(lungmask$resources$concurrency_group, "torch_cpu_heavy")
  expect_equal(threshold$resources$memory_mb, 1024L)
  expect_equal(mask_ops$resources$concurrency_group, "mask_ops")
  expect_equal(qc$resources$max_concurrent, 4L)
  expect_equal(rt$resources$concurrency_group, "imaging_rt")
  expect_equal(embeddings$resources$optional_gpus, 1L)
  expect_true("DSIMAGING_WORKER_CONTEXT_DIR" %in% names(pyradiomics$env))
  expect_true("DSIMAGING_CREDENTIALS_PATH" %in% names(pyradiomics$env))
  expect_false(any(c("DSIMAGING_ASSET_DB", "DSIMAGING_REGISTRY_PATH") %in%
                   names(pyradiomics$env)))

  permission_bits <- function(path) bitwAnd(
    as.integer(file.info(path)$mode), strtoi("0777", base = 8L))
  expect_identical(permission_bits(file.path(home, "runners")),
                   strtoi("0770", base = 8L))
  expect_identical(permission_bits(file.path(
    home, "runners", "pyradiomics_extract.yml")),
    strtoi("0660", base = 8L))
  expect_identical(permission_bits(dirname(pyradiomics$args_template[[1]])),
                   strtoi("0770", base = 8L))
})

test_that("runner registration rejects symlinked and world-writable configs", {
  skip_on_os("windows")
  root <- withr::local_tempdir()
  runners <- file.path(root, "runners")
  dir.create(runners, mode = "0770")
  Sys.chmod(runners, "0770", use_umask = FALSE)
  outside <- file.path(root, "outside.yml")
  writeLines("sentinel", outside)
  link <- file.path(runners, "linked.yml")
  if (!isTRUE(file.symlink(outside, link))) {
    skip("Symbolic links are unavailable on this platform")
  }

  expect_error(dsImaging:::.write_runner_yaml(
    runners, "linked", list(name = "linked")),
    "symbolic link", fixed = TRUE)
  expect_identical(readLines(outside), "sentinel")

  ordinary <- file.path(runners, "ordinary.yml")
  writeLines("name: ordinary", ordinary)
  Sys.chmod(ordinary, "0666", use_umask = FALSE)
  expect_error(dsImaging:::.assert_imaging_runner_permissions(
    ordinary, "0660", "Runner configuration"),
    "world-writable", fixed = TRUE)
})

test_that("radiomics runners can declare containerized execution metadata", {
  home <- tempfile("dshpc_home")
  imaging_home <- tempfile("dsimaging_home")
  dir.create(file.path(home, "runners"), recursive = TRUE)
  withr::local_options(list(
    dshpc.home = home,
    dsimaging.analysis.home = imaging_home,
    dsimaging.container_images = list(
      pyradiomics_extract = "ghcr.io/isglobal-brge/dsimaging-runner@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    ),
    dsimaging.container_runtime = "docker",
    dsimaging.container_pull = "never"
  ))
  on.exit(unlink(c(home, imaging_home), recursive = TRUE), add = TRUE)

  dsImaging:::.register_radiomics_runners()
  runner <- yaml::read_yaml(file.path(home, "runners", "pyradiomics_extract.yml"))

  expect_equal(runner$container$image,
    "ghcr.io/isglobal-brge/dsimaging-runner@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
  expect_equal(runner$container$runtime, "docker")
  expect_equal(runner$container$pull, "never")
  expect_equal(runner$container$command, "python")
  expect_equal(runner$container$args_template[[1]], "-m")
  expect_equal(runner$container$args_template[[2]], "dsimaging_extract")
})

test_that("clinical runner images must be immutable", {
  withr::local_options(list(
    dsimaging.container_images = list(
      pyradiomics_extract = "ghcr.io/isglobal-brge/dsimaging-runner:latest"
    )
  ))

  expect_error(
    dsImaging:::.imaging_container_config(
      "pyradiomics_extract", list("-m", "dsimaging_extract")),
    "immutable sha256"
  )
})

test_that("bundled profile paths are materialised outside the package tree", {
  imaging_home <- tempfile("dsimaging_home")
  withr::local_options(list(dsimaging.analysis.home = imaging_home))
  on.exit(unlink(imaging_home, recursive = TRUE), add = TRUE)

  path <- dsImaging:::.resolve_profile_path("aerts_signature_v1")

  expect_true(file.exists(path))
  expect_match(path, file.path(imaging_home, "runtime", "profiles"),
    fixed = TRUE)
  expect_false(grepl("00LOCK|site-library", path))
})
