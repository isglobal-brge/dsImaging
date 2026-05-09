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
  home <- tempfile("dsjobs_home")
  dir.create(file.path(home, "runners"), recursive = TRUE)
  withr::local_options(list(dsjobs.home = home))
  on.exit(unlink(home, recursive = TRUE), add = TRUE)

  dsImaging:::.register_radiomics_runners()
  lungmask <- yaml::read_yaml(file.path(home, "runners", "lungmask_infer.yml"))
  threshold <- yaml::read_yaml(file.path(home, "runners", "ct_lung_threshold.yml"))
  mask_ops <- yaml::read_yaml(file.path(home, "runners", "mask_ops.yml"))
  qc <- yaml::read_yaml(file.path(home, "runners", "imaging_qc_metrics.yml"))

  expect_equal(lungmask$resources$max_concurrent, 1L)
  expect_equal(lungmask$resources$concurrency_group, "torch_cpu_heavy")
  expect_equal(threshold$resources$memory_mb, 1024L)
  expect_equal(mask_ops$resources$concurrency_group, "mask_ops")
  expect_equal(qc$resources$max_concurrent, 4L)
})

test_that("radiomics runners can declare containerized execution metadata", {
  home <- tempfile("dsjobs_home")
  dir.create(file.path(home, "runners"), recursive = TRUE)
  withr::local_options(list(
    dsjobs.home = home,
    dsimaging.container_images = list(
      pyradiomics_extract = "ghcr.io/isglobal-brge/dsimaging-runner:test"
    ),
    dsimaging.container_runtime = "docker",
    dsimaging.container_pull = "never"
  ))
  on.exit(unlink(home, recursive = TRUE), add = TRUE)

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
