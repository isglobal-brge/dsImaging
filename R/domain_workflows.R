# Module: Domain-mediated dsHPC workflows
#
# These DataSHIELD methods receive high-level imaging requests from
# dsImagingClient and compose dsHPC job specs server-side. The client never
# submits low-level dsHPC specs directly.

#' Process a radiomics collection
#' @param request_encoded B64/JSON request payload.
#' @return A job/generation handle assigned server-side.
#' @export
imagingProcessRadiomicsCollectionDS <- function(request_encoded) {
  .dsimaging_require_literal_arguments()
  owner_env <- parent.frame()
  req <- .imaging_allowed_list(.imaging_request(request_encoded),
    c("handle", "dataset_id", "segmenter", "mask_asset", "profile",
      "batch_size", "name"), "workflow request")
  context <- .imaging_authorized_request(req, owner_env)
  req <- context$request
  dataset_id <- context$authorized$dataset_id
  segmenter <- .imaging_segmenter_spec(req$segmenter %||%
    list(provider = "existing_mask_asset",
         mask_asset = req$mask_asset %||% "masks"))
  profile <- .imaging_profile_spec(req$profile %||%
    list(name = "ibsi_ct_3d_v1", bin_width = 25))
  visibility <- "global"
  batch_size <- as.integer(req$batch_size %||% 10L)
  if (length(batch_size) != 1L || is.na(batch_size) || batch_size < 1L ||
      batch_size > 100L) {
    stop("batch_size must be between 1 and 100.", call. = FALSE)
  }
  unit_selection <- dsHPC::hpcUnitSelectionInternal(owner_env,
    default_label = "dsImaging")

  scan <- tryCatch(
    .imaging_radiomics_scan_collection(dataset_id, segmenter, profile,
      visibility, resolved_override = context$authorized,
      unit_selection = unit_selection),
    error = function(e) stop(
      "Collection processing could not be initialized.", call. = FALSE))
  if (identical(scan$action, "reuse_asset")) {
    return(.register_imaging_workflow(list(
      action = "reuse_asset", asset_id = scan$asset_id,
      tracking_id = scan$tracking_id,
      dataset_id = dataset_id), owner_env))
  }

  pending_ids <- scan$pending_ids %||% character(0)
  if (length(pending_ids) > 0L) {
    first_batch <- pending_ids[seq_len(min(batch_size, length(pending_ids)))]
    fingerprints <- scan$fingerprints[first_batch]
    content_hashes <- (scan$content_hashes %||% list())[first_batch]
    tryCatch(
      .imaging_radiomics_submit_batch(scan$generation_id, first_batch,
        dataset_id, fingerprints, content_hashes),
      error = function(e) {
        update_generation(scan$generation_id,
          error = "Initial batch submission deferred")
        NULL
      })
  }

  .register_imaging_workflow(list(
    action = "running", generation_id = scan$generation_id,
    tracking_id = scan$tracking_id,
    dataset_id = dataset_id), owner_env)
}

#' Submit a radiomics extraction job
#' @param request_encoded B64/JSON request payload.
#' @return A job handle assigned server-side.
#' @export
imagingProcessRadiomicsAssetDS <- function(request_encoded) {
  .dsimaging_require_literal_arguments()
  owner_env <- parent.frame()
  req <- .imaging_allowed_list(.imaging_request(request_encoded),
    c("handle", "dataset_id", "image_asset", "mask_asset", "profile",
      "alias", "name"), "workflow request")
  context <- .imaging_authorized_request(req, owner_env)
  req <- context$request
  dataset_id <- context$authorized$dataset_id
  collection_seal <- .imaging_authorized_collection_seal(context$authorized)
  image_asset <- .imaging_safe_name(req$image_asset %||% "images",
                                    "image_asset")
  mask_asset <- .imaging_safe_name(req$mask_asset, "mask_asset")
  profile <- .imaging_profile_spec(req$profile %||%
    list(name = "ibsi_ct_3d_v1", bin_width = 25))
  visibility <- "global"
  unit_selection <- dsHPC::hpcUnitSelectionInternal(owner_env,
    default_label = "dsImaging")
  runtime_identity <- .imaging_runtime_identity(unit_selection)
  profile_signature <- .radiomics_profile_signature(profile)

  worker_assets <- c(image_asset, mask_asset)
  worker_manifest <- .imaging_worker_manifest(
    context$authorized, asset_names = worker_assets)
  collection_map <- .imaging_worker_collection_map(
    context$authorized, worker_manifest)
  image_input_identity <- .imaging_worker_asset_identity(
    collection_map, worker_manifest, image_asset)
  mask_input_identity <- .imaging_worker_asset_identity(
    collection_map, worker_manifest, mask_asset)
  hash <- compute_derivation_hash(dataset_id = dataset_id,
    collection_seal = collection_seal,
    image_asset = image_asset, mask_asset = mask_asset,
    image_input_identity = image_input_identity,
    mask_input_identity = mask_input_identity,
    profile = profile, profile_signature = profile_signature,
    runtime_identity = runtime_identity,
    execution_unit = unit_selection)
  existing <- find_asset_by_hash(dataset_id, hash, collection_seal)
  if (!is.null(existing)) {
    return(.imaging_reuse_handle(existing, dataset_id, owner_env,
      output_asset_kind = "feature_table", derivation_hash = hash,
      collection_seal = collection_seal))
  }

  worker_dataset_id <- .register_imaging_worker_context(context$authorized,
    asset_names = worker_assets, collection_map = collection_map,
    worker_manifest = worker_manifest)
  config <- profile
  config$mask_asset <- mask_asset
  config$image_asset <- image_asset
  config$settings_file <- .resolve_profile_path(profile$name)
  config$dataset_id <- worker_dataset_id
  config$worker_context <- .imaging_worker_context_file(worker_dataset_id)
  publish_step <- .imaging_step_publish_asset(dataset_id, "radiomics",
    asset_type = "feature_table", publish_kind = "imaging_radiomics_asset")
  publish_step$alias <- .imaging_safe_name(req$alias, "alias", required = FALSE)
  publish_step$runner <- "pyradiomics_extract"
  publish_step$config <- config
  publish_step$derivation_hash <- hash
  publish_step$tracking_output <- TRUE

  job <- .imaging_job(label = "dsImaging",
    name = req$name %||% paste(dataset_id, "radiomics extraction"),
    tags = c("extraction", dataset_id, profile$name),
    visibility = visibility,
    steps = list(
      .imaging_step_resolve_dataset(worker_dataset_id),
      .imaging_step_run_artifact("pyradiomics_extract", config = config),
      publish_step,
      .imaging_step_safe_summary()
    ))
  .imaging_submit_job(.imaging_enrich_job_resources(job), owner_env,
    dataset_id, worker_dataset_id, "feature_table", hash,
    unit_selection = unit_selection, collection_seal = collection_seal)
}

#' Submit a segmentation job
#' @param request_encoded B64/JSON request payload.
#' @return A job handle assigned server-side.
#' @export
imagingProcessSegmentationCollectionDS <- function(request_encoded) {
  .dsimaging_require_literal_arguments()
  owner_env <- parent.frame()
  req <- .imaging_allowed_list(.imaging_request(request_encoded),
    c("handle", "dataset_id", "image_asset", "segmenter", "alias", "name"),
    "workflow request")
  context <- .imaging_authorized_request(req, owner_env)
  req <- context$request
  dataset_id <- context$authorized$dataset_id
  collection_seal <- .imaging_authorized_collection_seal(context$authorized)
  segmenter <- .imaging_segmenter_spec(req$segmenter %||%
    stop("segmenter is required.", call. = FALSE))
  if (identical(segmenter$provider, "existing_mask_asset")) {
    tryCatch(.imaging_worker_manifest(
      context$authorized, asset_names = segmenter$mask_asset),
      error = function(e) stop("Requested imaging asset is unavailable.",
                               call. = FALSE))
    return(.imaging_source_asset_handle(
      segmenter$mask_asset, dataset_id, owner_env))
  }
  unit_selection <- dsHPC::hpcUnitSelectionInternal(owner_env,
    default_label = "dsImaging")
  runtime_identity <- .imaging_runtime_identity(unit_selection)
  image_asset <- .imaging_safe_name(req$image_asset %||% "images",
                                    "image_asset")
  visibility <- "global"
  worker_manifest <- .imaging_worker_manifest(
    context$authorized, asset_names = image_asset)
  collection_map <- .imaging_worker_collection_map(
    context$authorized, worker_manifest)
  image_input_identity <- .imaging_worker_asset_identity(
    collection_map, worker_manifest, image_asset)
  hash <- compute_derivation_hash(dataset_id = dataset_id,
    collection_seal = collection_seal,
    image_asset = image_asset,
    image_input_identity = image_input_identity,
    runtime_identity = runtime_identity,
    segmenter = segmenter, execution_unit = unit_selection)
  existing <- find_asset_by_hash(dataset_id, hash, collection_seal)
  if (!is.null(existing)) {
    return(.imaging_reuse_handle(existing, dataset_id, owner_env,
      output_asset_kind = "mask_root", derivation_hash = hash,
      collection_seal = collection_seal))
  }

  runner <- .imaging_segmenter_runner(segmenter$provider)
  worker_dataset_id <- .register_imaging_worker_context(context$authorized,
    asset_names = image_asset, collection_map = collection_map,
    worker_manifest = worker_manifest)
  config <- segmenter
  config$image_asset <- image_asset
  config$dataset_id <- worker_dataset_id
  config$worker_context <- .imaging_worker_context_file(worker_dataset_id)
  publish_step <- .imaging_step_publish_asset(dataset_id, "masks",
    asset_type = "mask_root", publish_kind = "imaging_asset")
  publish_step$alias <- .imaging_safe_name(req$alias, "alias", required = FALSE)
  publish_step$runner <- runner
  publish_step$config <- config
  publish_step$derivation_hash <- hash
  publish_step$tracking_output <- TRUE

  job <- .imaging_job(label = "dsImaging",
    name = req$name %||% paste(dataset_id, "segmentation"),
    tags = c("segmentation", dataset_id, segmenter$provider),
    visibility = visibility,
    steps = list(.imaging_step_resolve_dataset(worker_dataset_id),
      .imaging_step_run_artifact(runner, config = config),
      publish_step, .imaging_step_safe_summary()))
  .imaging_submit_job(.imaging_enrich_job_resources(job), owner_env,
    dataset_id, worker_dataset_id, "mask_root", hash,
    unit_selection = unit_selection, collection_seal = collection_seal)
}

#' Submit a chained segmentation and radiomics job
#' @param request_encoded B64/JSON request payload.
#' @return A job handle assigned server-side.
#' @export
imagingProcessSegmentAndExtractDS <- function(request_encoded) {
  .dsimaging_require_literal_arguments()
  owner_env <- parent.frame()
  req <- .imaging_allowed_list(.imaging_request(request_encoded),
    c("handle", "dataset_id", "image_asset", "segmenter", "profile",
      "alias", "name"), "workflow request")
  context <- .imaging_authorized_request(req, owner_env)
  req <- context$request
  dataset_id <- context$authorized$dataset_id
  collection_seal <- .imaging_authorized_collection_seal(context$authorized)
  image_asset <- .imaging_safe_name(req$image_asset %||% "images",
                                    "image_asset")
  segmenter <- .imaging_segmenter_spec(req$segmenter %||%
    stop("segmenter is required.", call. = FALSE))
  profile <- .imaging_profile_spec(req$profile %||%
    list(name = "ibsi_ct_3d_v1", bin_width = 25))
  visibility <- "global"
  unit_selection <- dsHPC::hpcUnitSelectionInternal(owner_env,
    default_label = "dsImaging")
  runtime_identity <- .imaging_runtime_identity(unit_selection)
  profile_signature <- .radiomics_profile_signature(profile)

  existing_mask <- identical(
    segmenter$provider, "existing_mask_asset")
  initial_worker_assets <- c(image_asset,
    if (existing_mask) segmenter$mask_asset %||% "masks" else character(0))
  worker_manifest <- .imaging_worker_manifest(
    context$authorized, asset_names = initial_worker_assets)
  collection_map <- .imaging_worker_collection_map(
    context$authorized, worker_manifest)
  image_input_identity <- .imaging_worker_asset_identity(
    collection_map, worker_manifest, image_asset)
  mask_input_identity <- if (existing_mask) {
    .imaging_worker_asset_identity(
      collection_map, worker_manifest, segmenter$mask_asset %||% "masks")
  } else NULL

  seg_hash <- compute_derivation_hash(dataset_id = dataset_id,
    collection_seal = collection_seal,
    image_asset = image_asset,
    image_input_identity = image_input_identity,
    runtime_identity = runtime_identity,
    segmenter = segmenter, execution_unit = unit_selection)
  full_hash_params <- list(dataset_id = dataset_id,
    collection_seal = collection_seal,
    image_asset = image_asset,
    image_input_identity = image_input_identity,
    segmenter = segmenter,
    profile = profile, profile_signature = profile_signature,
    runtime_identity = runtime_identity,
    execution_unit = unit_selection)
  if (!is.null(mask_input_identity)) {
    full_hash_params$mask_input_identity <- mask_input_identity
  }
  full_hash <- do.call(compute_derivation_hash, full_hash_params)
  existing <- find_asset_by_hash(dataset_id, full_hash, collection_seal)
  if (!is.null(existing)) {
    return(.imaging_reuse_handle(existing, dataset_id, owner_env,
      output_asset_kind = "feature_table", derivation_hash = full_hash,
      collection_seal = collection_seal))
  }

  seg_runner <- if (identical(segmenter$provider, "existing_mask_asset")) {
    NULL
  } else {
    .imaging_segmenter_runner(segmenter$provider)
  }
  mask_asset_for_extract <- segmenter$mask_asset %||% "masks"

  if (!is.null(seg_runner)) {
    existing_seg <- find_asset_by_hash(dataset_id, seg_hash, collection_seal)
    if (!is.null(existing_seg)) {
      mask_asset_for_extract <- existing_seg
      seg_runner <- NULL
      mask_manifest <- .imaging_worker_manifest(
        context$authorized, asset_names = mask_asset_for_extract)
      worker_manifest$assets[[mask_asset_for_extract]] <-
        mask_manifest$assets[[mask_asset_for_extract]]
      collection_map <- .imaging_worker_collection_map(
        context$authorized, worker_manifest)
    }
  }
  worker_assets <- c(image_asset,
    if (is.null(seg_runner)) mask_asset_for_extract else character(0))
  worker_dataset_id <- .register_imaging_worker_context(context$authorized,
    asset_names = worker_assets, collection_map = collection_map,
    worker_manifest = worker_manifest)
  worker_context <- .imaging_worker_context_file(worker_dataset_id)
  steps <- list(.imaging_step_resolve_dataset(worker_dataset_id))
  if (!is.null(seg_runner)) {
    seg_config <- segmenter
    seg_config$image_asset <- image_asset
    seg_config$dataset_id <- worker_dataset_id
    seg_config$worker_context <- worker_context
    mask_publish_step <- .imaging_step_publish_asset(dataset_id, "masks",
      asset_type = "mask_root", publish_kind = "imaging_asset")
    mask_publish_step$runner <- seg_runner
    mask_publish_step$config <- seg_config
    mask_publish_step$derivation_hash <- seg_hash
    steps <- c(steps, list(.imaging_step_run_artifact(seg_runner,
      config = seg_config), mask_publish_step))
  }

  extract_config <- profile
  extract_config$mask_asset <- mask_asset_for_extract
  extract_config$image_asset <- image_asset
  extract_config$settings_file <- .resolve_profile_path(profile$name)
  extract_config$dataset_id <- worker_dataset_id
  extract_config$worker_context <- worker_context
  radiomics_publish_step <- .imaging_step_publish_asset(dataset_id, "radiomics",
    asset_type = "feature_table", publish_kind = "imaging_radiomics_asset")
  radiomics_publish_step$runner <- "pyradiomics_extract"
  radiomics_publish_step$config <- extract_config
  radiomics_publish_step$derivation_hash <- full_hash
  radiomics_publish_step$tracking_output <- TRUE
  extract_step <- .imaging_step_run_artifact("pyradiomics_extract",
    config = extract_config)
  if (!is.null(seg_runner)) extract_step$inputs <- list(2L)
  steps <- c(steps, list(extract_step, radiomics_publish_step,
    .imaging_step_safe_summary()))

  job <- .imaging_job(label = "dsImaging",
    name = req$name %||% paste(dataset_id, "segment and extract"),
    tags = c("segment_and_extract", dataset_id, segmenter$provider, profile$name),
    visibility = visibility, steps = steps)
  .imaging_submit_job(.imaging_enrich_job_resources(job), owner_env,
    dataset_id, worker_dataset_id, "feature_table", full_hash,
    unit_selection = unit_selection, collection_seal = collection_seal)
}

#' Submit a generic clinical imaging asset workflow
#' @param request_encoded B64/JSON request payload.
#' @return A job handle assigned server-side.
#' @export
imagingProcessAssetWorkflowDS <- function(request_encoded) {
  .dsimaging_require_literal_arguments()
  owner_env <- parent.frame()
  req <- .imaging_request(request_encoded)
  .imaging_submit_asset_workflow(req, owner_env = owner_env)
}

#' Submit a QC metrics workflow
#' @param request_encoded B64/JSON request payload.
#' @return A job handle assigned server-side.
#' @export
imagingProcessQcCollectionDS <- function(request_encoded) {
  .dsimaging_require_literal_arguments()
  owner_env <- parent.frame()
  req <- .imaging_request(request_encoded)
  req$output_asset <- req$output_asset %||% "imaging_qc"
  .imaging_submit_asset_workflow(req, runner_override = "imaging_qc_metrics",
    owner_env = owner_env)
}

#' Submit an exact single-file DICOM conversion workflow
#'
#' Enhanced or ordinary single-file DICOM samples are mapped through the
#' admitted collection snapshot. Multi-file DICOM series remain fail-closed
#' until a volumetric reader preserves that same exact sample association.
#' @param request_encoded B64/JSON request payload.
#' @return A job handle assigned server-side.
#' @export
imagingConvertCollectionDS <- function(request_encoded) {
  .dsimaging_require_literal_arguments()
  owner_env <- parent.frame()
  req <- .imaging_request(request_encoded)
  req$output_asset <- req$output_asset %||% "nifti_images"
  .imaging_submit_asset_workflow(req, runner_override = "dicom_convert",
    owner_env = owner_env)
}

#' Publish a completed radiomics collection
#' @param request_encoded B64/JSON request payload.
#' @return Publication metadata assigned server-side.
#' @export
imagingPublishRadiomicsAssetDS <- function(request_encoded) {
  .dsimaging_require_literal_arguments()
  owner_env <- parent.frame()
  req <- .imaging_allowed_list(.imaging_request(request_encoded),
    c("collection_symbol", "workflow_symbol", "allow_partial"),
    "workflow request")
  if (isTRUE(req$allow_partial)) {
    stop("Partial collection publication is not permitted.", call. = FALSE)
  }
  reference <- .required_scalar(
    req$collection_symbol %||% req$workflow_symbol,
    "collection_symbol")
  .imaging_collection_publish(reference, owner_env)
}

#' Load radiomics features through the domain asset catalogue
#' @param request_encoded B64/JSON request payload.
#' @return Data frame assigned server-side.
#' @export
imagingLoadRadiomicsFeaturesDS <- function(request_encoded) {
  .dsimaging_require_literal_arguments()
  owner_env <- parent.frame()
  req <- .imaging_allowed_list(.imaging_request(request_encoded),
    c("handle", "dataset_id", "asset_id_or_alias", "asset_id", "alias",
      "columns", "include_metadata", "syntactic_names"),
    "workflow request")
  context <- .imaging_authorized_request(req, owner_env)
  req <- context$request
  data <- .imaging_load_asset(context$authorized,
    .required_scalar(req$asset_id_or_alias %||% req$asset_id %||% req$alias,
      "asset_id_or_alias"),
    columns = req$columns %||% NULL,
    include_metadata = isTRUE(req$include_metadata),
    syntactic_names = isTRUE(req$syntactic_names))
  .mark_imaging_feature_table_export(owner_env)
  data
}

#' Recover a collection workflow from its durable tracking root
#'
#' DataSHIELD ASSIGN method. Recovery still requires an imaging handle for the
#' exact admitted collection; a public tracking id alone never authorizes its
#' data or internal generation state.
#'
#' @param tracking_id Public dsHPC logical tracking id.
#' @param handle_symbol Session symbol holding an authorized imaging handle.
#' @return A new session-bound workflow reference.
#' @export
imagingRecoverWorkflowDS <- function(tracking_id, handle_symbol = "img") {
  .dsimaging_require_literal_arguments()
  tryCatch(.imaging_recover_tracking_workflow(
    tracking_id, handle_symbol, parent.frame()),
    error = function(e) stop(
      "Shared imaging workflow could not be recovered.", call. = FALSE))
}

#' @keywords internal
.imaging_recover_tracking_workflow <- function(tracking_id, handle_symbol,
                                                owner_env) {
  tracking_id <- .imaging_tracking_id(tracking_id)
  generation <- .imaging_generation_for_tracking(tracking_id)
  if (!identical(as.character(generation$kind), "radiomics_collection") ||
      !identical(as.character(generation$visibility), "global")) {
    stop("Shared imaging workflow is unavailable.", call. = FALSE)
  }
  collection_seal <- .asset_collection_seal(
    generation$collection_seal, required = TRUE)
  authorized <- .authorized_imaging_dataset(
    .required_scalar(handle_symbol, "handle_symbol"),
    as.character(generation$dataset_id), owner_env = owner_env)
  if (!identical(.imaging_authorized_collection_seal(authorized),
                 collection_seal)) {
    stop("Shared imaging workflow is unavailable.", call. = FALSE)
  }
  tracking_status <- dsHPC::hpcTrackingStatusInternal(tracking_id)
  if (!is.list(tracking_status) ||
      !identical(tracking_status$tracking_id, tracking_id)) {
    stop("Shared imaging workflow is unavailable.", call. = FALSE)
  }

  asset_id <- generation$published_asset_id
  has_asset <- is.character(asset_id) && length(asset_id) == 1L &&
    !is.na(asset_id) && grepl("^asset_[0-9a-f]{32}$", asset_id)
  workflow <- list(
    action = if (has_asset) "published" else "running",
    generation_id = as.character(generation$generation_id),
    tracking_id = tracking_id,
    dataset_id = as.character(generation$dataset_id))
  if (has_asset) {
    db <- .asset_db_connect()
    on.exit(.asset_db_close(db), add = TRUE)
    asset <- .asset_get(db, asset_id)
    valid_asset <- is.list(asset) && identical(asset$status, "active") &&
      identical(asset$visibility, "global") &&
      identical(as.character(asset$dataset_id), workflow$dataset_id) &&
      identical(as.character(asset$collection_seal), collection_seal)
    if (!isTRUE(valid_asset)) {
      stop("Shared imaging workflow is unavailable.", call. = FALSE)
    }
    workflow$asset_id <- asset_id
  } else if (identical(as.character(generation$state), "ACTIVE")) {
    stop("Shared imaging workflow is unavailable.", call. = FALSE)
  }
  .register_imaging_workflow(workflow, owner_env)
}

#' Get collection status
#' @param reference_symbol Session symbol holding the collection workflow.
#' @export
imagingCollectionStatusDS <- function(reference_symbol) {
  .dsimaging_require_literal_arguments()
  .imaging_collection_status(reference_symbol, parent.frame())
}

#' @keywords internal
.imaging_collection_status <- function(reference_symbol, owner_env) {
  workflow_context <- .resolve_imaging_workflow_context(
    reference_symbol, owner_env = owner_env)
  workflow <- workflow_context$workflow
  if (identical(workflow$action, "reuse_asset") ||
      identical(workflow$action, "published")) {
    .authorize_workflow_private_asset(workflow$asset_id, workflow_context)
    result <- list(state = "ACTIVE", is_done = TRUE,
                   asset_id = workflow$asset_id)
    tracking_id <- .imaging_tracking_id(
      workflow$tracking_id %||% NULL, required = FALSE)
    if (!is.null(tracking_id)) result$tracking_id <- tracking_id
    return(result)
  }
  status <- tryCatch(
    .imaging_radiomics_collection_status(workflow$generation_id),
    error = function(e) NULL)
  if (is.null(status)) {
    stop("Collection status is unavailable.", call. = FALSE)
  }
  state <- .safe_collection_state(status$state)
  tracking_id <- .imaging_tracking_id(
    workflow$tracking_id %||% .imaging_generation_tracking_id(
      workflow$generation_id, required = FALSE), required = FALSE)
  failed <- isTRUE(status$is_done) &&
    (as.integer(status$failed %||% 0L) > 0L ||
     state %in% c("FAILED", "CANCELLED", "PARTIAL"))
  if (failed) {
    .imaging_tracking_finish_generation(workflow$generation_id,
      success = FALSE)
    state <- if (identical(state, "CANCELLED")) "CANCELLED" else "FAILED"
  }
  result <- list(state = state, is_done = isTRUE(status$is_done))
  if (!is.null(tracking_id)) result$tracking_id <- tracking_id
  gen <- get_generation(workflow$generation_id)
  asset_id <- if (is.null(gen)) NULL else gen$published_asset_id %||% NULL
  if (!is.null(asset_id) && !is.na(asset_id) && nzchar(asset_id)) {
    .authorize_workflow_private_asset(asset_id, workflow_context)
    result$asset_id <- asset_id
  }
  result
}

#' Recover a collection generation
#' @param reference_symbol Session symbol holding the collection workflow.
#' @export
imagingCollectionRecoverDS <- function(reference_symbol) {
  .dsimaging_require_literal_arguments()
  owner_env <- parent.frame()
  workflow <- .resolve_imaging_workflow(reference_symbol,
                                        owner_env = owner_env)
  if (!identical(workflow$action, "reuse_asset") &&
      !identical(workflow$action, "published")) {
    tryCatch(
      .imaging_radiomics_recover_collection(workflow$generation_id),
      error = function(e) stop("Collection recovery failed.", call. = FALSE))
  }
  .imaging_collection_status(reference_symbol, owner_env)
}

#' Publish a completed collection workflow
#'
#' @param reference_symbol Session symbol holding the collection workflow.
#' @return Client-safe publication state and opaque asset reference.
#' @export
imagingCollectionPublishDS <- function(reference_symbol) {
  .dsimaging_require_literal_arguments()
  .imaging_collection_publish(reference_symbol, parent.frame())
}

#' @keywords internal
.imaging_collection_publish <- function(reference_symbol, owner_env) {
  workflow_context <- .resolve_imaging_workflow_context(
    reference_symbol, owner_env = owner_env)
  workflow <- workflow_context$workflow
  if (identical(workflow$action, "reuse_asset") ||
      identical(workflow$action, "published")) {
    .authorize_workflow_private_asset(workflow$asset_id, workflow_context)
    result <- list(state = "ACTIVE", asset_id = workflow$asset_id)
    tracking_id <- .imaging_tracking_id(
      workflow$tracking_id %||% NULL, required = FALSE)
    if (!is.null(tracking_id)) result$tracking_id <- tracking_id
    return(result)
  }
  published <- tryCatch(
    .imaging_radiomics_publish_collection(
      workflow$generation_id, workflow$dataset_id),
    error = function(e) stop("Collection publication failed.", call. = FALSE))
  workflow$action <- "published"
  workflow$asset_id <- published$asset_id
  .update_imaging_workflow(reference_symbol, workflow,
                           owner_env = owner_env)
  .authorize_workflow_private_asset(published$asset_id, workflow_context)
  result <- list(state = "ACTIVE", asset_id = published$asset_id)
  tracking_id <- .imaging_tracking_id(
    workflow$tracking_id %||% NULL, required = FALSE)
  if (!is.null(tracking_id)) result$tracking_id <- tracking_id
  result
}

#' Get a domain workflow status
#'
#' DataSHIELD AGGREGATE method. Resolves a session-bound workflow capability
#' and returns only coarse state plus, after successful publication, the opaque
#' asset identifier produced by that exact job.
#'
#' @param reference_symbol Session symbol holding the workflow capability.
#' @return A client-safe named list.
#' @export
imagingWorkflowStatusDS <- function(reference_symbol) {
  .dsimaging_require_literal_arguments()
  .imaging_workflow_status(reference_symbol, parent.frame())
}

#' @keywords internal
.imaging_workflow_status <- function(reference_symbol, owner_env) {
  workflow_context <- .resolve_imaging_workflow_context(
    reference_symbol, owner_env = owner_env)
  workflow <- workflow_context$workflow

  if (!is.null(workflow$generation_id)) {
    return(.imaging_collection_status(reference_symbol, owner_env))
  }
  if (identical(workflow$action, "reuse_asset")) {
    .authorize_workflow_private_asset(workflow$asset_id, workflow_context)
    result <- list(state = "ACTIVE", is_done = TRUE,
                   asset_id = workflow$asset_id)
    tracking_id <- .imaging_tracking_id(
      workflow$tracking_id %||% NULL, required = FALSE)
    if (!is.null(tracking_id)) result$tracking_id <- tracking_id
    return(result)
  }
  if (identical(workflow$action, "source_asset")) {
    return(list(state = "ACTIVE", is_done = TRUE))
  }
  if (identical(workflow$action, "tracking")) {
    tracking_id <- .imaging_tracking_id(workflow$tracking_id)
    status <- tryCatch(dsHPC::hpcTrackingStatusInternal(tracking_id),
                       error = function(e) NULL)
    if (is.null(status) || !is.character(status$state) ||
        length(status$state) != 1L || is.na(status$state) ||
        !status$state %in% c("queued", "running", "terminal") ||
        !is.logical(status$is_done) || length(status$is_done) != 1L ||
        is.na(status$is_done)) {
      stop("Workflow status is unavailable.", call. = FALSE)
    }
    if (!isTRUE(status$is_done)) {
      state <- if (identical(status$state, "queued")) "PENDING" else "RUNNING"
      return(list(state = state, is_done = FALSE,
                  tracking_id = tracking_id))
    }
    asset_id <- tryCatch(.imaging_workflow_shared_asset(workflow),
                         error = function(e) NULL)
    workflow$action <- "terminal"
    workflow$state <- if (is.null(asset_id)) "FAILED" else "PUBLISHED"
    workflow$asset_id <- asset_id
    .update_imaging_workflow(reference_symbol, workflow,
                             owner_env = owner_env)
    result <- list(state = workflow$state, is_done = TRUE,
                   tracking_id = tracking_id)
    if (!is.null(asset_id)) result$asset_id <- asset_id
    return(result)
  }
  if (identical(workflow$action, "terminal")) {
    result <- list(state = workflow$state, is_done = TRUE)
    tracking_id <- .imaging_tracking_id(
      workflow$tracking_id %||% NULL, required = FALSE)
    if (!is.null(tracking_id)) result$tracking_id <- tracking_id
    if (!is.null(workflow$asset_id)) {
      .authorize_workflow_private_asset(workflow$asset_id, workflow_context)
      result$asset_id <- workflow$asset_id
    }
    return(result)
  }
  if (!identical(workflow$action, "job") || !is.list(workflow$job)) {
    stop("Workflow status is unavailable.", call. = FALSE)
  }

  tracking_id <- .imaging_tracking_id(
    workflow$tracking_id %||% NULL, required = FALSE)
  tracking_status <- if (is.null(tracking_id)) NULL else tryCatch(
    dsHPC::hpcTrackingStatusInternal(tracking_id), error = function(e) NULL)
  if (is.list(tracking_status) && isTRUE(tracking_status$is_done)) {
    tracked_asset <- tryCatch(.imaging_workflow_shared_asset(workflow),
                              error = function(e) NULL)
    if (!is.null(tracked_asset)) {
      cleanup_ok <- tryCatch({
        if (!is.null(workflow$worker_context_id)) {
          .unregister_imaging_worker_context(workflow$worker_context_id)
        }
        TRUE
      }, error = function(e) FALSE)
      if (!cleanup_ok) {
        stop("Workflow cleanup is unavailable.", call. = FALSE)
      }
      workflow$action <- "terminal"
      workflow$state <- "PUBLISHED"
      workflow$worker_context_id <- NULL
      workflow$job <- NULL
      workflow$asset_id <- tracked_asset
      .update_imaging_workflow(reference_symbol, workflow,
                               owner_env = owner_env)
      return(list(state = "PUBLISHED", is_done = TRUE,
                  asset_id = tracked_asset, tracking_id = tracking_id))
    }
  }

  status <- tryCatch(dsHPC::hpcStatusInternal(workflow$job,
    required_label = "dsImaging"), error = function(e) NULL)
  if (is.null(status)) {
    stop("Workflow status is unavailable.", call. = FALSE)
  }
  state <- .safe_workflow_job_state(status$state)
  done <- isTRUE(status$is_done)
  if (identical(state, "UNKNOWN") ||
      (done && !state %in% c("FINISHED", "PUBLISHED", "FAILED", "CANCELLED"))) {
    stop("Workflow status is unavailable.", call. = FALSE)
  }
  if (!done) {
    result <- list(state = state, is_done = FALSE)
    if (!is.null(tracking_id)) result$tracking_id <- tracking_id
    return(result)
  }

  asset_id <- NULL
  if (state %in% c("FINISHED", "PUBLISHED")) {
    asset_id <- tryCatch(.imaging_workflow_result_asset(workflow),
                         error = function(e) NULL)
  }
  cleanup_ok <- tryCatch({
    if (!is.null(workflow$worker_context_id)) {
      .unregister_imaging_worker_context(workflow$worker_context_id)
    }
    TRUE
  }, error = function(e) FALSE)
  if (!cleanup_ok) {
    stop("Workflow cleanup is unavailable.", call. = FALSE)
  }

  missing_result <- state %in% c("FINISHED", "PUBLISHED") && is.null(asset_id)
  if (!is.null(tracking_id) &&
      (!state %in% c("FINISHED", "PUBLISHED") || missing_result)) {
    dsHPC::hpcTrackingFinishInternal(tracking_id, success = FALSE)
  }
  workflow$action <- "terminal"
  workflow$state <- if (missing_result) "FAILED" else state
  workflow$worker_context_id <- NULL
  workflow$job <- NULL
  workflow$asset_id <- asset_id
  .update_imaging_workflow(reference_symbol, workflow,
                           owner_env = owner_env)

  if (missing_result) {
    stop("Workflow result is unavailable.", call. = FALSE)
  }
  result <- list(state = state, is_done = TRUE)
  if (!is.null(tracking_id)) result$tracking_id <- tracking_id
  if (!is.null(asset_id)) {
    .authorize_workflow_private_asset(asset_id, workflow_context)
    result$asset_id <- asset_id
  }
  result
}

#' Destroy a completed domain workflow capability
#'
#' DataSHIELD ASSIGN method. Active work is never silently cancelled: callers
#' must wait for a terminal state (or use the collection cancellation API)
#' before destroying its session reference.
#'
#' @param reference_symbol Session symbol holding the workflow capability.
#' @return The opaque reference as an invisible retry tombstone. DataSHIELD
#'   assigns it back to the same symbol until the client confirms removal.
#' @export
imagingWorkflowDestroyDS <- function(reference_symbol) {
  .dsimaging_require_literal_arguments()
  owner_env <- parent.frame()
  unavailable <- function() {
    stop("Workflow reference is unavailable.", call. = FALSE)
  }
  if (!is.environment(owner_env) ||
      !is.character(reference_symbol) || length(reference_symbol) != 1L ||
      is.na(reference_symbol) || !nzchar(reference_symbol) ||
      !exists(reference_symbol, envir = owner_env, inherits = FALSE)) {
    unavailable()
  }
  reference <- get(reference_symbol, envir = owner_env, inherits = FALSE)
  if (!.is_imaging_workflow_reference(reference) ||
      bindingIsLocked(reference_symbol, owner_env)) {
    unavailable()
  }
  state <- tryCatch(
    .imaging_session_state(owner_env, create = FALSE),
    error = function(e) NULL)
  if (is.null(state)) unavailable()
  entry <- state$workflows[[reference$capability]]
  if (identical(entry, .dsimaging_session_tombstone)) {
    rm(list = reference_symbol, envir = owner_env)
    return(invisible(reference))
  }
  if (is.null(entry) || !is.list(entry$workflow)) {
    unavailable()
  }
  context <- list(
    workflow = entry$workflow, owner_env = owner_env,
    capability = reference$capability)
  workflow <- context$workflow
  if (identical(workflow$action, "job")) {
    .imaging_workflow_status(reference_symbol, owner_env)
    context <- .resolve_imaging_workflow_context(
      reference_symbol, owner_env = owner_env)
    workflow <- context$workflow
  }
  if (!is.null(workflow$generation_id) &&
      !workflow$action %in% c("reuse_asset", "published", "cancelled")) {
    stop("An active collection workflow cannot be destroyed.", call. = FALSE)
  }
  if (identical(workflow$action, "job")) {
    stop("An active imaging workflow cannot be destroyed.", call. = FALSE)
  }
  owner_env <- context$owner_env
  if (bindingIsLocked(reference_symbol, owner_env)) {
    stop("Workflow reference is unavailable.", call. = FALSE)
  }
  state <- .imaging_session_state(owner_env, create = FALSE)
  workflows <- state$workflows
  workflows[[context$capability]] <- .dsimaging_session_tombstone
  rm(list = reference_symbol, envir = owner_env)
  invisible(reference)
}

#' @keywords internal
.imaging_workflow_result_asset <- function(workflow) {
  job_id <- workflow$job$job_id %||% NULL
  dataset_id <- workflow$dataset_id %||% NULL
  kind <- workflow$output_asset_kind %||% NULL
  if (!is.character(job_id) || length(job_id) != 1L || is.na(job_id) ||
      !grepl(paste0("^job_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-",
                    "[0-9a-f]{4}-[0-9a-f]{12}$"), job_id) ||
      !is.character(dataset_id) || length(dataset_id) != 1L ||
      is.na(dataset_id) || !nzchar(dataset_id) ||
      !is.character(kind) || length(kind) != 1L || is.na(kind) ||
      !grepl("^[A-Za-z0-9][A-Za-z0-9_.-]*$", kind)) {
    stop("Workflow result is unavailable.", call. = FALSE)
  }
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  sql <- paste(
    "SELECT asset_id, derivation_hash FROM assets",
    "WHERE dataset_id = ? AND created_by_job = ? AND kind = ?",
    "AND status = 'active' AND visibility = 'global'"
  )
  rows <- DBI::dbGetQuery(db, sql, params = list(dataset_id, job_id, kind))
  expected_hash <- workflow$derivation_hash %||% NULL
  if (!is.null(expected_hash)) {
    rows <- rows[!is.na(rows$derivation_hash) &
      rows$derivation_hash == expected_hash, , drop = FALSE]
  }
  if (nrow(rows) != 1L ||
      !grepl("^asset_[0-9a-f]{32}$", rows$asset_id[[1L]])) {
    stop("Workflow result is unavailable.", call. = FALSE)
  }
  as.character(rows$asset_id[[1L]])
}

#' Resolve the validated global output behind a logical workflow root
#' @keywords internal
.imaging_workflow_shared_asset <- function(workflow) {
  tracking_id <- .imaging_tracking_id(workflow$tracking_id)
  dataset_id <- .required_scalar(workflow$dataset_id, "dataset_id")
  kind <- .imaging_safe_name(workflow$output_asset_kind,
                             "output asset kind")
  derivation_hash <- workflow$derivation_hash
  if (!is.character(derivation_hash) || length(derivation_hash) != 1L ||
      is.na(derivation_hash) || !grepl("^[0-9a-f]{64}$", derivation_hash)) {
    stop("Workflow result is unavailable.", call. = FALSE)
  }
  collection_seal <- .asset_collection_seal(
    workflow$collection_seal, required = TRUE)

  asset_id <- tryCatch(.imaging_tracking_resolve_asset(
    tracking_id, output_name = "imaging_asset", dataset_id = dataset_id,
    kind = kind, derivation_hash = derivation_hash,
    collection_seal = collection_seal), error = function(e) NULL)
  if (is.null(asset_id)) {
    asset_id <- find_asset_by_hash(dataset_id, derivation_hash,
                                   collection_seal)
  }
  if (is.null(asset_id)) {
    stop("Workflow result is unavailable.", call. = FALSE)
  }

  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  asset <- .asset_get(db, asset_id)
  valid <- is.list(asset) && identical(asset$status, "active") &&
    identical(asset$visibility, "global") &&
    identical(as.character(asset$dataset_id), dataset_id) &&
    identical(as.character(asset$kind), kind) &&
    identical(as.character(asset$derivation_hash), derivation_hash) &&
    identical(as.character(asset$collection_seal), collection_seal)
  if (!isTRUE(valid)) {
    stop("Workflow result is unavailable.", call. = FALSE)
  }
  as.character(asset_id)
}

#' Authorize a private result only through the workflow capability that
#' produced or exposed it in this DataSHIELD session.
#' @keywords internal
.authorize_workflow_private_asset <- function(asset_id, workflow_context) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  asset <- .asset_get(db, asset_id)
  if (is.null(asset)) {
    stop("Collection asset is unavailable.", call. = FALSE)
  }
  if (identical(asset$visibility, "private")) {
    .authorize_private_imaging_asset(asset_id, workflow_context$owner_env)
  } else if (!identical(asset$visibility, "global")) {
    stop("Collection asset is unavailable.", call. = FALSE)
  }
  invisible(TRUE)
}

#' List asset generations
#' @param dataset_id Optional dataset id.
#' @param state Optional generation state.
#' @keywords internal
.imaging_list_generations_admin <- function(dataset_id = NULL, state = NULL) {
  db <- .asset_db_connect()
  on.exit(.asset_db_close(db))
  where <- character(0)
  params <- list()
  if (!is.null(dataset_id) && nzchar(dataset_id)) {
    where <- c(where, "dataset_id = ?")
    params <- c(params, list(dataset_id))
  }
  if (!is.null(state) && nzchar(state)) {
    where <- c(where, "state = ?")
    params <- c(params, list(state))
  }
  sql <- paste("SELECT generation_id, dataset_id, kind, state, visibility,",
    "expected_n, completed_n, failed_n, created_at, updated_at",
    "FROM asset_generations")
  if (length(where) > 0L) sql <- paste(sql, "WHERE", paste(where, collapse = " AND "))
  sql <- paste(sql, "ORDER BY created_at DESC")
  if (length(params) > 0L) DBI::dbGetQuery(db, sql, params = params)
  else DBI::dbGetQuery(db, sql)
}

#' Disabled legacy generation-listing entry point
#' @param dataset_id Ignored legacy dataset id.
#' @param state Ignored legacy generation state.
#' @return Never returns; this legacy DataSHIELD surface is disabled.
#' @export
imagingListGenerationsDS <- function(dataset_id = NULL, state = NULL) {
  .legacy_imaging_ds_disabled("imagingListGenerationsDS")
}

#' Describe a radiomics profile
#' @param profile_name Profile name.
#' @export
imagingDescribeProfileDS <- function(profile_name) {
  .dsimaging_require_literal_arguments()
  profile_name <- .required_scalar(.decode_maybe(profile_name), "profile_name")
  profile <- tryCatch(
    suppressWarnings(read_radiomics_profile(profile_name)),
    error = function(e) stop("Profile description is unavailable.",
                             call. = FALSE))
  feature_classes <- names(profile$featureClass %||%
    profile$feature_classes %||% list())
  image_types <- names(profile$imageType %||%
    profile$image_types %||% list())
  safe_names <- function(values) {
    values <- unique(as.character(values))
    values[!is.na(values) & grepl("^[A-Za-z][A-Za-z0-9_]{0,63}$", values)]
  }
  # Admin YAML is configuration, not public metadata. Project only a fixed
  # structural schema and never echo arbitrary keys, paths, or secret hints.
  list(
    name = profile_name,
    feature_classes = safe_names(feature_classes),
    image_types = safe_names(image_types)
  )
}

#' List radiomics profiles
#' @export
imagingListProfilesDS <- function() {
  .dsimaging_require_literal_arguments()
  data.frame(
    profile_name = .safe_public_identifiers(list_radiomics_profiles()),
    stringsAsFactors = FALSE)
}

#' @keywords internal
.imaging_request <- function(request_encoded) {
  req <- .dsr_decode(request_encoded)
  if (!is.list(req)) stop("Workflow request must be a list.", call. = FALSE)
  if (is.null(names(req)) || any(!nzchar(names(req))) ||
      anyDuplicated(names(req))) {
    stop("Workflow request must have unique, non-empty field names.",
         call. = FALSE)
  }
  req
}

#' @keywords internal
.imaging_authorized_request <- function(req, owner_env = NULL) {
  handle_symbol <- .required_scalar(req$handle %||% "img", "handle")
  requested_dataset <- req$dataset_id %||% NULL
  if (!is.null(requested_dataset)) {
    requested_dataset <- .required_scalar(requested_dataset, "dataset_id")
  }
  authorized <- .authorized_imaging_dataset(
    handle_symbol, requested_dataset, owner_env = owner_env)
  req$handle <- handle_symbol
  req$dataset_id <- authorized$dataset_id
  list(request = req, authorized = authorized)
}

#' Return the immutable collection identity admitted with a handle
#' @keywords internal
.imaging_authorized_collection_seal <- function(authorized) {
  snapshot <- authorized$collection_snapshot %||%
    (authorized$handle %||% list())$collection_snapshot
  if (!is.null(snapshot)) {
    return(.validate_imaging_collection_snapshot(snapshot)$seal)
  }
  # Trusted internal descriptor handles have no storage snapshot, but still
  # receive a server-derived immutable manifest/roster seal. Analyst-created
  # resources always take the snapshot branch above.
  .asset_collection_seal(authorized$collection_seal %||%
    (authorized$handle %||% list())$collection_seal, required = TRUE)
}

#' @keywords internal
.imaging_visibility <- function(value) {
  value <- as.character(value)
  if (length(value) != 1L || is.na(value) ||
      !value %in% c("private", "global")) {
    stop("visibility must be 'private' or 'global'.", call. = FALSE)
  }
  value
}

#' @keywords internal
.safe_collection_state <- function(state) {
  state <- as.character(state %||% "UNKNOWN")
  allowed <- c("PENDING", "RUNNING", "ACTIVE", "PARTIAL", "FAILED",
               "CANCELLED")
  if (length(state) != 1L || is.na(state) || !state %in% allowed) "UNKNOWN"
  else state
}

#' @keywords internal
.safe_workflow_job_state <- function(state) {
  state <- as.character(state %||% "UNKNOWN")
  allowed <- c("PENDING", "RUNNING", "FINISHED", "PUBLISHED", "FAILED",
               "CANCELLED")
  if (length(state) != 1L || is.na(state) || !state %in% allowed) "UNKNOWN"
  else state
}

#' @keywords internal
.decode_maybe <- function(x) {
  if (is.character(x) && length(x) == 1L && startsWith(x, "B64:")) .dsr_decode(x) else x
}

#' @keywords internal
.required_scalar <- function(x, name) {
  if (is.null(x) || length(x) != 1L || is.na(x) || !nzchar(as.character(x))) {
    stop(name, " is required.", call. = FALSE)
  }
  as.character(x)
}

#' Validate one logical imaging asset/profile/model name
#' @keywords internal
.imaging_safe_name <- function(x, name, required = TRUE) {
  if (is.null(x) && !isTRUE(required)) return(NULL)
  value <- .required_scalar(x, name)
  if (nchar(value, type = "bytes") > 128L ||
      !grepl("^[A-Za-z0-9][A-Za-z0-9_.-]*$", value) ||
      grepl("\\.\\.", value, fixed = FALSE)) {
    stop(name, " must be a logical server-side name, not a path or URI.",
         call. = FALSE)
  }
  value
}

#' Validate an analyst-supplied named list against an exact field allowlist
#' @keywords internal
.imaging_allowed_list <- function(x, allowed, name) {
  if (is.null(x)) return(list())
  if (!is.list(x)) stop(name, " must be a named list.", call. = FALSE)
  if (length(x) == 0L) return(list())
  nms <- names(x)
  if (is.null(nms) || any(!nzchar(nms)) || anyDuplicated(nms)) {
    stop(name, " must have unique, non-empty field names.", call. = FALSE)
  }
  unknown <- setdiff(nms, allowed)
  if (length(unknown) > 0L) {
    stop(name, " contains unsupported field(s): ",
         paste(unknown, collapse = ", "), ".", call. = FALSE)
  }
  x
}

#' @keywords internal
.imaging_scalar_number <- function(x, name, min = -Inf, max = Inf,
                                   integer = FALSE) {
  value <- suppressWarnings(as.numeric(unlist(x, use.names = FALSE)))
  if (length(value) != 1L || is.na(value) || !is.finite(value) ||
      value < min || value > max || (isTRUE(integer) && value != round(value))) {
    stop(name, " is outside its permitted range.", call. = FALSE)
  }
  if (isTRUE(integer)) as.integer(value) else value
}

#' @keywords internal
.imaging_scalar_logical <- function(x, name) {
  if (!is.logical(x) || length(x) != 1L || is.na(x)) {
    stop(name, " must be one logical value.", call. = FALSE)
  }
  x
}

#' @keywords internal
.imaging_csv_values <- function(x, name, allowed = NULL, numeric = FALSE,
                                integer = FALSE, min = -Inf, max = Inf,
                                max_items = 64L) {
  values <- unlist(x, use.names = FALSE)
  if (length(values) == 1L && is.character(values)) {
    values <- strsplit(values, ",", fixed = TRUE)[[1L]]
  }
  values <- trimws(as.character(values))
  values <- values[nzchar(values)]
  if (length(values) == 0L || length(values) > max_items) {
    stop(name, " has an invalid number of values.", call. = FALSE)
  }
  if (isTRUE(numeric)) {
    parsed <- suppressWarnings(as.numeric(values))
    if (anyNA(parsed) || any(!is.finite(parsed)) || any(parsed < min) ||
        any(parsed > max) || (isTRUE(integer) && any(parsed != round(parsed)))) {
      stop(name, " contains an invalid numeric value.", call. = FALSE)
    }
    values <- if (isTRUE(integer)) as.character(as.integer(parsed)) else
      as.character(parsed)
  } else {
    if (any(nchar(values, type = "bytes") > 128L) ||
        any(grepl("[/\\\\]", values)) || any(grepl("\\.\\.", values)) ||
        any(grepl("^[A-Za-z][A-Za-z0-9+.-]*:", values))) {
      stop(name, " must not contain paths or URIs.", call. = FALSE)
    }
  }
  if (!is.null(allowed) && any(!values %in% allowed)) {
    stop(name, " contains an unsupported value.", call. = FALSE)
  }
  paste(unique(values), collapse = ",")
}

#' Validate and canonicalise an analyst-selected segmentation specification
#' @keywords internal
.imaging_segmenter_spec <- function(segmenter) {
  if (!is.list(segmenter) || is.null(segmenter$provider)) {
    stop("segmenter must declare a provider.", call. = FALSE)
  }
  provider <- .imaging_safe_name(segmenter$provider, "segmenter provider")
  allowed <- switch(provider,
    existing_mask_asset = c("provider", "mask_asset"),
    totalsegmentator = c("provider", "task", "fast", "roi_subset"),
    totalsegmentator_infer = c("provider", "task", "fast", "roi_subset"),
    lungmask = c("provider", "model_name"),
    lungmask_infer = c("provider", "model_name"),
    ct_lung_threshold = c("provider", "threshold", "max_components",
                          "min_voxels"),
    nnunetv2 = c("provider", "model_name", "fold"),
    nnunetv2_predict = c("provider", "model_name", "fold"),
    monai = stop(paste(
      "MONAI segmentation is unavailable through DataSHIELD until its",
      "output path has an exact per-sample contract."), call. = FALSE),
    monai_bundle_infer = stop(paste(
      "MONAI segmentation is unavailable through DataSHIELD until its",
      "output path has an exact per-sample contract."), call. = FALSE),
    stop("Unknown segmentation provider.", call. = FALSE))
  segmenter <- .imaging_allowed_list(segmenter, allowed, "segmenter")
  out <- list(provider = provider)
  if (identical(provider, "existing_mask_asset")) {
    out$mask_asset <- .imaging_safe_name(segmenter$mask_asset, "mask_asset")
  } else if (provider %in% c("totalsegmentator", "totalsegmentator_infer")) {
    out$task <- .imaging_safe_name(segmenter$task %||% "total", "task")
    out$fast <- .imaging_scalar_logical(segmenter$fast %||% FALSE, "fast")
    if (!is.null(segmenter$roi_subset)) {
      out$roi_subset <- .imaging_csv_values(segmenter$roi_subset, "roi_subset")
    }
  } else if (provider %in% c("lungmask", "lungmask_infer")) {
    model <- .imaging_safe_name(segmenter$model_name %||% "R231", "model_name")
    if (!model %in% c("R231", "LTRCLobes", "LTRCLobes_R231",
                      "R231CovidWeb")) {
      stop("model_name is not an approved LungMask model.", call. = FALSE)
    }
    out$model_name <- model
  } else if (identical(provider, "ct_lung_threshold")) {
    out$threshold <- .imaging_scalar_number(segmenter$threshold %||% -320,
      "threshold", min = -10000, max = 10000)
    out$max_components <- .imaging_scalar_number(
      segmenter$max_components %||% 2L, "max_components",
      min = 1, max = 1000, integer = TRUE)
    out$min_voxels <- .imaging_scalar_number(segmenter$min_voxels %||% 1000L,
      "min_voxels", min = 1, max = 1e9, integer = TRUE)
  } else if (provider %in% c("nnunetv2", "nnunetv2_predict")) {
    out$model_name <- .imaging_safe_name(segmenter$model_name, "model_name")
    out$fold <- .imaging_safe_name(segmenter$fold %||% "all", "fold")
  } else {
    out$bundle_name <- .imaging_safe_name(segmenter$bundle_name,
                                           "bundle_name")
  }
  out
}

#' Validate and canonicalise a radiomics profile without accepting a path
#' @keywords internal
.imaging_profile_spec <- function(profile) {
  allowed <- c("name", "bin_width", "force2D", "normalize",
    "resampled_spacing", "feature_classes", "image_types",
    "selected_features", "voxel_based", "kernel_radius")
  profile <- .imaging_allowed_list(profile, allowed, "profile")
  name <- .imaging_safe_name(profile$name, "profile name")
  if (is.null(.resolve_profile_path(name))) {
    stop("Unknown radiomics profile.", call. = FALSE)
  }
  out <- list(name = name)
  if (!is.null(profile$bin_width)) out$bin_width <- .imaging_scalar_number(
    profile$bin_width, "bin_width", min = .Machine$double.eps, max = 1e6)
  for (field in c("force2D", "normalize", "voxel_based")) {
    if (!is.null(profile[[field]])) {
      out[[field]] <- .imaging_scalar_logical(profile[[field]], field)
    }
  }
  if (!is.null(profile$resampled_spacing)) {
    out$resampled_spacing <- .imaging_csv_values(profile$resampled_spacing,
      "resampled_spacing", numeric = TRUE, min = .Machine$double.eps,
      max = 1000, max_items = 3L)
  }
  if (!is.null(profile$feature_classes)) {
    out$feature_classes <- .imaging_csv_values(profile$feature_classes,
      "feature_classes", allowed = c("firstorder", "shape", "shape2D",
        "glcm", "glrlm", "glszm", "gldm", "ngtdm"))
  }
  if (!is.null(profile$image_types)) {
    out$image_types <- .imaging_csv_values(profile$image_types, "image_types",
      allowed = c("Original", "LoG", "Wavelet", "Square", "SquareRoot",
        "Logarithm", "Exponential", "Gradient", "LBP2D", "LBP3D"))
  }
  if (!is.null(profile$selected_features)) {
    out$selected_features <- strsplit(.imaging_csv_values(
      profile$selected_features, "selected_features", max_items = 512L),
      ",", fixed = TRUE)[[1L]]
  }
  if (!is.null(profile$kernel_radius)) out$kernel_radius <-
    .imaging_scalar_number(profile$kernel_radius, "kernel_radius",
      min = 1, max = 100, integer = TRUE)
  out
}

#' Server contracts for the generic clinical runner entry point
#' @keywords internal
.imaging_runner_contract <- function(runner) {
  if (runner %in% c("rt_convert", "rt_dose_plan", "wsi_tile")) {
    stop(paste(
      "Runner is unavailable through DataSHIELD until its inputs have an",
      "exact admitted sample mapping."), call. = FALSE)
  }
  contracts <- list(
    dicom_convert = list(fields = c("dicom_asset", "converter"),
      assets = "dicom_asset", asset_type = "image_root", tag = "dicom_convert"),
    image_preprocess = list(fields = c("image_asset", "operations", "spacing",
      "lower", "upper"), assets = "image_asset", asset_type = "image_root",
      tag = "preprocess"),
    mask_ops = list(fields = c("operation", "mask_asset", "mask_b_asset",
      "reference_asset", "labels", "threshold", "mode", "radius",
      "min_voxels", "max_components"),
      assets = c("mask_asset", "mask_b_asset", "reference_asset"),
      asset_type = "mask_root", tag = "mask_operation"),
    imaging_qc_metrics = list(fields = c("image_asset", "mask_asset"),
      assets = c("image_asset", "mask_asset"), asset_type = "qc_table",
      tag = "qc_metrics"),
    imaging_qc_visuals = list(fields = c("image_asset", "mask_asset",
      "max_size"),
      assets = c("image_asset", "mask_asset"), asset_type = "qc_visual_asset",
      tag = "qc_visuals"),
    image_spatial = list(fields = c("image_asset", "operations", "mask_asset",
      "reference_asset", "spacing", "crop_size"),
      assets = c("image_asset", "mask_asset", "reference_asset"),
      asset_type = "image_root", tag = "spatial"),
    image_embeddings = list(fields = c("image_asset", "model", "bins"),
      assets = "image_asset", asset_type = "embedding_table",
      tag = "embeddings")
  )
  contracts[[runner]] %||% stop("Runner is not available through this workflow.",
                                call. = FALSE)
}

#' Validate and rebuild one generic clinical runner config
#' @keywords internal
.imaging_runner_config <- function(runner, config) {
  contract <- .imaging_runner_contract(runner)
  config <- .imaging_allowed_list(config, contract$fields, "runner config")
  defaults <- switch(runner,
    dicom_convert = list(dicom_asset = "images", converter = "simpleitk"),
    image_preprocess = list(image_asset = "images", operations = "float32"),
    mask_ops = list(mask_asset = "masks"),
    imaging_qc_metrics = list(image_asset = "images"),
    imaging_qc_visuals = list(image_asset = "images"),
    image_spatial = list(image_asset = "images", operations = "resample"),
    image_embeddings = list(image_asset = "images",
      model = "intensity_histogram", bins = 32L))
  for (field in names(defaults)) {
    if (is.null(config[[field]])) config[[field]] <- defaults[[field]]
  }
  out <- config
  for (field in intersect(contract$assets, names(out))) {
    out[[field]] <- .imaging_safe_name(out[[field]], field)
  }
  if (identical(runner, "dicom_convert") && !is.null(out$converter)) {
    out$converter <- match.arg(as.character(out$converter), "simpleitk")
  }
  operation_fields <- switch(runner,
    image_preprocess = list(field = "operations", values = c("resample",
      "normalize", "zscore", "clamp", "window", "float32", "cast_float32")),
    image_spatial = list(field = "operations", values = c("resample",
      "crop_to_mask", "center_crop", "n4_bias", "register_rigid")),
    NULL)
  if (!is.null(operation_fields) && !is.null(out[[operation_fields$field]])) {
    out[[operation_fields$field]] <- .imaging_csv_values(
      out[[operation_fields$field]], operation_fields$field,
      allowed = operation_fields$values)
  }
  if (identical(runner, "mask_ops")) {
    op <- match.arg(as.character(out$operation), c("binarize", "label_select",
      "connected_components", "morphology", "union", "intersection",
      "difference", "resample_to_image"))
    out$operation <- op
    if (!is.null(out$labels)) out$labels <- .imaging_csv_values(out$labels,
      "labels", numeric = TRUE, integer = TRUE, min = 0, max = 1e9)
    if (!is.null(out$mode)) out$mode <- match.arg(as.character(out$mode),
      c("closing", "close", "opening", "open", "dilate", "dilation",
        "erode", "erosion"))
  }
  if (identical(runner, "image_embeddings") && !is.null(out$model) &&
      !identical(as.character(out$model), "intensity_histogram")) {
    stop("model is not available for image_embeddings.", call. = FALSE)
  }
  numeric_fields <- list(
    spacing = c(.Machine$double.eps, 1000, 0),
    crop_size = c(1, 100000, 1), lower = c(-1e12, 1e12, 0),
    upper = c(-1e12, 1e12, 0), threshold = c(-1e12, 1e12, 0),
    radius = c(0, 10000, 1), min_voxels = c(1, 1e12, 1),
    max_components = c(1, 1e6, 1), max_size = c(16, 4096, 1),
    bins = c(2, 4096, 1)
  )
  csv_numeric <- c("spacing", "crop_size")
  for (field in intersect(names(numeric_fields), names(out))) {
    bounds <- numeric_fields[[field]]
    if (field %in% csv_numeric) {
      out[[field]] <- .imaging_csv_values(out[[field]], field,
        numeric = TRUE, integer = isTRUE(bounds[3] == 1), min = bounds[1],
        max = bounds[2], max_items = 3L)
    } else {
      out[[field]] <- .imaging_scalar_number(out[[field]], field,
        min = bounds[1], max = bounds[2], integer = isTRUE(bounds[3] == 1))
    }
  }
  out
}

#' @keywords internal
.imaging_step <- function(type, plane = "session", ...) {
  list(type = type, plane = plane, ...)
}

#' @keywords internal
.imaging_step_resolve_dataset <- function(dataset_id) {
  .imaging_step("resolve_dataset", plane = "session", dataset_id = dataset_id)
}

#' @keywords internal
.imaging_step_run_artifact <- function(runner, config = list(), inputs = NULL) {
  step <- .imaging_step("run_artifact", plane = "artifact", runner = runner,
    config = config)
  if (!is.null(inputs)) step$inputs <- inputs
  step
}

#' @keywords internal
.imaging_step_publish_asset <- function(target_dataset, asset_name,
                                        asset_type = "derived",
                                        publish_kind = "imaging_asset") {
  .imaging_step("publish_asset", plane = "session", dataset_id = target_dataset,
    asset_name = asset_name, asset_type = asset_type, publish_kind = publish_kind)
}

#' @keywords internal
.imaging_step_safe_summary <- function() {
  .imaging_step("safe_summary", plane = "session")
}

#' @keywords internal
.imaging_job <- function(steps, label = "dsImaging", tags = NULL,
                         visibility = "private", name = NULL,
                         resource_class = "default", ...) {
  list(steps = steps, name = name, label = label, tags = tags,
    visibility = visibility, resource_class = resource_class, ...)
}

#' @keywords internal
.imaging_submit_job <- function(job, owner_env, dataset_id,
                                worker_context_id, output_asset_kind,
                                derivation_hash,
                                unit_selection = NULL,
                                collection_seal) {
  if (!is.character(job$label) || length(job$label) != 1L ||
      is.na(job$label) || !nzchar(job$label)) {
    stop("dsImaging dsHPC submissions must carry a non-empty label.",
      call. = FALSE)
  }
  if (!is.environment(owner_env) ||
      !is.character(worker_context_id) || length(worker_context_id) != 1L ||
      is.na(worker_context_id) ||
      !grepl("^dsctx_[0-9a-f]{64}$", worker_context_id)) {
    stop("Invalid imaging workflow context.", call. = FALSE)
  }
  output_asset_kind <- .imaging_safe_name(
    output_asset_kind, "output asset kind")
  if (!is.character(derivation_hash) || length(derivation_hash) != 1L ||
      is.na(derivation_hash) || !grepl("^[0-9a-f]{64}$", derivation_hash)) {
    stop("Invalid imaging workflow derivation.", call. = FALSE)
  }
  collection_seal <- .asset_collection_seal(collection_seal, required = TRUE)
  # The derivation hash binds the admitted collection seal and normalized
  # workflow inputs. dsHPC adds package, runner and execution-unit identity to
  # its own hash before allowing completed-result reuse.
  job$reuse_fingerprint <- derivation_hash
  tracking <- .imaging_tracking_create(output_asset_kind, derivation_hash)
  tracking_id <- tracking$tracking_id

  if (isTRUE(tracking$is_done)) {
    asset_id <- tryCatch(.imaging_tracking_resolve_asset(
      tracking_id, output_name = "imaging_asset", dataset_id = dataset_id,
      kind = output_asset_kind, derivation_hash = derivation_hash,
      collection_seal = collection_seal), error = function(e) NULL)
    tryCatch(.unregister_imaging_worker_context(worker_context_id),
             error = function(e) NULL)
    if (is.null(asset_id)) {
      stop("Shared imaging workflow result is unavailable.", call. = FALSE)
    }
    return(.register_imaging_workflow(list(
      action = "reuse_asset", asset_id = asset_id,
      dataset_id = dataset_id, tracking_id = tracking_id,
      output_asset_kind = output_asset_kind,
      derivation_hash = derivation_hash,
      collection_seal = collection_seal), owner_env))
  }

  job$.owner <- .dsr_owner_id()
  result <- tryCatch(
    if (is.null(unit_selection)) {
      dsHPC::hpcSubmitInternal(.dsr_encode(job), session_env = owner_env,
        tracking_id = tracking_id, tracking_role = "primary",
        tracking_finalize = TRUE)
    } else {
      dsHPC::hpcSubmitInternal(.dsr_encode(job),
        unit_selection = unit_selection, tracking_id = tracking_id,
        tracking_role = "primary", tracking_finalize = TRUE)
    },
    error = function(e) {
      tryCatch(.unregister_imaging_worker_context(worker_context_id),
               error = function(e2) NULL)
      if (!isTRUE(tracking$reused)) {
        tryCatch(dsHPC::hpcTrackingFinishInternal(
          tracking_id, success = FALSE), error = function(e2) NULL)
      }
      stop("Imaging workflow submission failed.", call. = FALSE)
    })
  reused_execution <- isTRUE(result$reused) || isTRUE(result$deduplicated)
  if (reused_execution) {
    tryCatch(.unregister_imaging_worker_context(worker_context_id),
             error = function(e) NULL)
    worker_context_id <- NULL
  }
  .register_imaging_workflow(list(
    action = if (reused_execution) "tracking" else "job",
    job = if (reused_execution) NULL else result,
    dataset_id = dataset_id,
    worker_context_id = worker_context_id,
    output_asset_kind = output_asset_kind,
    derivation_hash = derivation_hash,
    collection_seal = collection_seal,
    tracking_id = tracking_id), owner_env)
}

#' @keywords internal
.imaging_reuse_handle <- function(asset_id, dataset_id, owner_env,
                                  output_asset_kind, derivation_hash,
                                  collection_seal) {
  tracking <- .imaging_tracking_create(output_asset_kind, derivation_hash)
  .imaging_tracking_reuse_asset(tracking, asset_id)
  .register_imaging_workflow(list(
    action = "reuse_asset", asset_id = asset_id,
    dataset_id = dataset_id, tracking_id = tracking$tracking_id,
    output_asset_kind = output_asset_kind,
    derivation_hash = derivation_hash,
    collection_seal = collection_seal), owner_env)
}

#' @keywords internal
.imaging_source_asset_handle <- function(asset_name, dataset_id, owner_env) {
  .register_imaging_workflow(list(
    action = "source_asset", asset_name = asset_name,
    dataset_id = dataset_id), owner_env)
}

#' @keywords internal
.imaging_segmenter_runner <- function(provider) {
  switch(provider,
    totalsegmentator = "totalsegmentator_infer",
    totalsegmentator_infer = "totalsegmentator_infer",
    lungmask = "lungmask_infer",
    lungmask_infer = "lungmask_infer",
    ct_lung_threshold = "ct_lung_threshold",
    nnunetv2 = "nnunetv2_predict",
    nnunetv2_predict = "nnunetv2_predict",
    monai = "monai_bundle_infer",
    monai_bundle_infer = "monai_bundle_infer",
    stop("Unknown segmentation provider: ", provider, call. = FALSE))
}

#' @keywords internal
.imaging_submit_asset_workflow <- function(req, runner_override = NULL,
                                           owner_env) {
  allowed <- c("handle", "dataset_id", "config", "output_asset",
    "alias", "name")
  if (is.null(runner_override)) allowed <- c(allowed, "runner")
  req <- .imaging_allowed_list(req, allowed, "workflow request")
  if (!is.null(runner_override)) req$runner <- runner_override
  context <- .imaging_authorized_request(req, owner_env)
  req <- context$request
  dataset_id <- context$authorized$dataset_id
  runner <- .imaging_safe_name(req$runner, "runner")
  contract <- .imaging_runner_contract(runner)
  config <- .imaging_runner_config(runner, req$config %||% list())
  unit_selection <- dsHPC::hpcUnitSelectionInternal(owner_env,
    default_label = "dsImaging")
  runtime_identity <- .imaging_runtime_identity(unit_selection)
  execution_runner <- if (identical(runner, "image_preprocess")) {
    "dsimaging_image_preprocess"
  } else runner
  output_asset <- .imaging_safe_name(req$output_asset, "output_asset")
  asset_type <- contract$asset_type
  input_assets <- unlist(config[intersect(contract$assets, names(config))],
                         use.names = FALSE)
  input_assets <- unique(as.character(input_assets))
  collection_seal <- .imaging_authorized_collection_seal(context$authorized)
  worker_manifest <- .imaging_worker_manifest(
    context$authorized, asset_names = input_assets)
  collection_map <- .imaging_worker_collection_map(
    context$authorized, worker_manifest)
  input_identities <- stats::setNames(lapply(input_assets, function(asset) {
    .imaging_worker_asset_identity(collection_map, worker_manifest, asset)
  }), input_assets)
  derivation_hash <- compute_derivation_hash(
    dataset_id = dataset_id,
    collection_seal = collection_seal,
    runner = execution_runner,
    config = config,
    input_identities = input_identities,
    output_asset = output_asset,
    output_asset_kind = asset_type,
    runtime_identity = runtime_identity,
    execution_unit = unit_selection)
  existing <- find_asset_by_hash(dataset_id, derivation_hash, collection_seal)
  if (!is.null(existing)) {
    return(.imaging_reuse_handle(existing, dataset_id, owner_env,
      output_asset_kind = asset_type, derivation_hash = derivation_hash,
      collection_seal = collection_seal))
  }
  worker_dataset_id <- .register_imaging_worker_context(context$authorized,
    asset_names = input_assets, collection_map = collection_map,
    worker_manifest = worker_manifest)
  config$dataset_id <- worker_dataset_id
  config$worker_context <- .imaging_worker_context_file(worker_dataset_id)
  visibility <- "global"
  label_tag <- contract$tag

  publish_step <- .imaging_step_publish_asset(dataset_id, output_asset,
    asset_type = asset_type, publish_kind = "imaging_asset")
  publish_step$alias <- .imaging_safe_name(req$alias, "alias", required = FALSE)
  publish_step$runner <- execution_runner
  publish_step$config <- c(config, list(execution_unit = unit_selection))
  publish_step$derivation_hash <- derivation_hash
  publish_step$tracking_output <- TRUE

  job <- .imaging_job(label = "dsImaging",
    name = req$name %||% paste(dataset_id, label_tag),
    tags = c(label_tag, dataset_id),
    visibility = visibility,
    steps = list(.imaging_step_resolve_dataset(worker_dataset_id),
      .imaging_step_run_artifact(execution_runner, config = config),
      publish_step, .imaging_step_safe_summary()))
  .imaging_submit_job(.imaging_enrich_job_resources(job), owner_env,
    dataset_id, worker_dataset_id, asset_type, derivation_hash,
    unit_selection = unit_selection, collection_seal = collection_seal)
}

#' @keywords internal
.imaging_collect_resource_names <- function(x) {
  fields <- c("resource", "dataset_id", "image_asset", "mask_asset",
    "mask_b_asset", "reference_asset", "dicom_asset", "rt_asset",
    "dose_asset", "plan_asset", "wsi_asset")
  out <- character(0)
  walk <- function(value, field = "") {
    if (is.list(value)) {
      if (identical(field, "resource") && !is.null(value$name) &&
          is.character(value$name) && length(value$name) == 1L) {
        out <<- c(out, value$name)
      }
      nms <- names(value)
      for (i in seq_along(value)) {
        child <- if (!is.null(nms) && nzchar(nms[i])) nms[i] else ""
        walk(value[[i]], child)
      }
      return(invisible(NULL))
    }
    if (field %in% fields && is.character(value) && length(value) == 1L &&
        !is.na(value) && nzchar(value)) {
      out <<- c(out, value)
    }
    invisible(NULL)
  }
  walk(x)
  unique(out)
}

#' @keywords internal
.imaging_enrich_job_resources <- function(job) {
  resources <- .imaging_collect_resource_names(job)
  if (length(resources) == 0L) return(job)
  records <- list()
  for (resource_name in resources) {
    item <- tryCatch(.content_hash_for_resource(resource_name),
                     error = function(e) NULL)
    if (!is.list(item)) next
    hash <- item$content_hash %||% NA_character_
    source <- item$source %||% NA_character_
    if (identical(source, "unsupported") || is.na(hash) || !nzchar(hash)) next
    records[[resource_name]] <- list(name = resource_name, content_hash = hash,
      source = source, updated_at = item$updated_at %||% NA_character_)
  }
  if (length(records) == 0L) return(job)
  job$content_hashes <- records
  for (i in seq_along(job$steps)) {
    step_resources <- intersect(.imaging_collect_resource_names(job$steps[[i]]),
      names(records))
    if (length(step_resources) > 0L)
      job$steps[[i]]$resource_hashes <- records[step_resources]
  }
  job
}
