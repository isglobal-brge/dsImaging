test_that("radiomics envs are listed", {
  envs <- list_radiomics_envs()
  expect_true(is.data.frame(envs))
  expect_true(nrow(envs) >= 4)
  expect_true("radiomics" %in% envs$framework)
  expect_true("seg_totalseg" %in% envs$framework)
})

test_that("runner health includes clinical imaging analysis runners", {
  runners <- dsImaging:::.radiomics_runner_health()
  expect_true("ct_lung_threshold" %in% runners$runner)
  expect_true("dicom_convert" %in% runners$runner)
  expect_true("image_preprocess" %in% runners$runner)
  expect_true("mask_ops" %in% runners$runner)
  expect_true("imaging_qc_metrics" %in% runners$runner)
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

  expect_equal(pyradiomics$resources$memory_mb, 6144L)
  expect_equal(pyradiomics$resources$max_concurrent, 2L)
  expect_match(pyradiomics$args_template[[1]],
    file.path(imaging_home, "runtime", "python"), fixed = TRUE)
  expect_false(grepl("00LOCK|site-library", pyradiomics$args_template[[1]]))
  expect_true(file.exists(pyradiomics$args_template[[1]]))
  expect_equal(lungmask$resources$max_concurrent, 1L)
  expect_equal(lungmask$resources$concurrency_group, "torch_cpu_heavy")
  expect_equal(threshold$resources$memory_mb, 1024L)
  expect_equal(mask_ops$resources$concurrency_group, "mask_ops")
  expect_equal(qc$resources$max_concurrent, 4L)
})

test_that("radiomics runners can declare containerized execution metadata", {
  home <- tempfile("dshpc_home")
  imaging_home <- tempfile("dsimaging_home")
  dir.create(file.path(home, "runners"), recursive = TRUE)
  withr::local_options(list(
    dshpc.home = home,
    dsimaging.analysis.home = imaging_home,
    dsimaging.container_images = list(
      pyradiomics_extract = "ghcr.io/isglobal-brge/dsimaging-runner:test"
    ),
    dsimaging.container_runtime = "docker",
    dsimaging.container_pull = "never"
  ))
  on.exit(unlink(c(home, imaging_home), recursive = TRUE), add = TRUE)

  dsImaging:::.register_radiomics_runners()
  runner <- yaml::read_yaml(file.path(home, "runners", "pyradiomics_extract.yml"))

  expect_equal(runner$container$image,
    "ghcr.io/isglobal-brge/dsimaging-runner:test")
  expect_equal(runner$container$runtime, "docker")
  expect_equal(runner$container$pull, "never")
  expect_equal(runner$container$command, "python")
  expect_equal(runner$container$args_template[[1]], "-m")
  expect_equal(runner$container$args_template[[2]], "dsimaging_extract")
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
