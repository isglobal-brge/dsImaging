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
  req <- .imaging_request(request_encoded)
  dataset_id <- .required_scalar(req$dataset_id, "dataset_id")
  segmenter <- req$segmenter %||% list(provider = "existing_mask_asset",
    mask_asset = req$mask_asset %||% "masks")
  profile <- req$profile %||% list(name = "ibsi_ct_3d_v1", bin_width = 25)
  visibility <- req$visibility %||% "private"
  batch_size <- as.integer(req$batch_size %||% 10L)

  scan <- imagingRadiomicsScanCollectionDS(.dsr_encode(dataset_id),
    .dsr_encode(segmenter), .dsr_encode(profile), .dsr_encode(visibility))
  if (identical(scan$action, "reuse_asset")) {
    return(c(scan, list(domain_method = "imagingProcessRadiomicsCollectionDS")))
  }

  pending_ids <- scan$pending_ids %||% character(0)
  submitted <- 0L
  if (length(pending_ids) > 0L) {
    first_batch <- pending_ids[seq_len(min(batch_size, length(pending_ids)))]
    fingerprints <- scan$fingerprints[first_batch]
    content_hashes <- (scan$content_hashes %||% list())[first_batch]
    imagingRadiomicsSubmitBatchDS(.dsr_encode(scan$generation_id),
      .dsr_encode(first_batch), .dsr_encode(segmenter), .dsr_encode(profile),
      .dsr_encode(dataset_id), .dsr_encode(fingerprints),
      .dsr_encode(content_hashes))
    submitted <- length(first_batch)
  }

  list(action = "kicked_off", domain = "dsImaging",
    domain_method = "imagingProcessRadiomicsCollectionDS",
    generation_id = scan$generation_id, dataset_id = dataset_id,
    total = scan$total, done = scan$done %||% 0L,
    submitted = submitted,
    pending = max(0L, length(pending_ids) - submitted))
}

#' Submit a radiomics extraction job
#' @param request_encoded B64/JSON request payload.
#' @return A job handle assigned server-side.
#' @export
imagingProcessRadiomicsAssetDS <- function(request_encoded) {
  req <- .imaging_request(request_encoded)
  dataset_id <- .required_scalar(req$dataset_id, "dataset_id")
  image_asset <- req$image_asset %||% "images"
  mask_asset <- .required_scalar(req$mask_asset, "mask_asset")
  profile <- req$profile %||% list(name = "ibsi_ct_3d_v1", bin_width = 25)
  visibility <- req$visibility %||% "private"

  hash <- compute_derivation_hash(dataset_id = dataset_id,
    image_asset = image_asset, mask_asset = mask_asset,
    profile_name = profile$name, bin_width = profile$bin_width,
    force2D = profile$force2D %||% FALSE,
    feature_classes = profile$feature_classes)
  existing <- find_asset_by_hash(dataset_id, hash)
  if (!is.null(existing)) return(.imaging_reuse_handle(existing, dataset_id))

  config <- c(profile, list(mask_asset = mask_asset, image_asset = image_asset,
    settings_file = profile$name))
  publish_step <- .imaging_step_publish_asset(dataset_id, "radiomics",
    asset_type = "feature_table", publish_kind = "imaging_radiomics_asset")
  publish_step$alias <- req$alias %||% NULL
  publish_step$runner <- "pyradiomics_extract"
  publish_step$config <- config
  publish_step$derivation_hash <- hash

  job <- .imaging_job(label = "dsImaging",
    name = req$name %||% paste(dataset_id, "radiomics extraction"),
    tags = c("extraction", dataset_id, profile$name),
    visibility = visibility,
    steps = list(
      .imaging_step_resolve_dataset(dataset_id),
      .imaging_step_run_artifact("pyradiomics_extract", config = config),
      publish_step,
      .imaging_step_safe_summary()
    ))
  if (!is.null(req$job_id)) job$job_id <- req$job_id
  .imaging_submit_job(.imaging_enrich_job_resources(job))
}

#' Submit a segmentation job
#' @param request_encoded B64/JSON request payload.
#' @return A job handle assigned server-side.
#' @export
imagingProcessSegmentationCollectionDS <- function(request_encoded) {
  req <- .imaging_request(request_encoded)
  dataset_id <- .required_scalar(req$dataset_id, "dataset_id")
  segmenter <- req$segmenter %||% stop("segmenter is required.", call. = FALSE)
  if (identical(segmenter$provider, "existing_mask_asset")) {
    return(.imaging_reuse_handle(segmenter$mask_asset, dataset_id))
  }
  image_asset <- req$image_asset %||% "images"
  visibility <- req$visibility %||% "private"
  hash <- compute_derivation_hash(dataset_id = dataset_id,
    image_asset = image_asset, provider = segmenter$provider,
    task = segmenter$task %||% "default",
    model_name = segmenter$model_name %||% segmenter$bundle_name %||% "default")
  existing <- find_asset_by_hash(dataset_id, hash)
  if (!is.null(existing)) return(.imaging_reuse_handle(existing, dataset_id))

  runner <- .imaging_segmenter_runner(segmenter$provider)
  config <- segmenter
  config$image_asset <- image_asset
  publish_step <- .imaging_step_publish_asset(dataset_id, "masks",
    asset_type = "mask_root", publish_kind = "imaging_asset")
  publish_step$alias <- req$alias %||% NULL
  publish_step$runner <- runner
  publish_step$config <- config
  publish_step$derivation_hash <- hash

  job <- .imaging_job(label = "dsImaging",
    name = req$name %||% paste(dataset_id, "segmentation"),
    tags = c("segmentation", dataset_id, segmenter$provider),
    visibility = visibility,
    steps = list(.imaging_step_resolve_dataset(dataset_id),
      .imaging_step_run_artifact(runner, config = config),
      publish_step, .imaging_step_safe_summary()))
  if (!is.null(req$job_id)) job$job_id <- req$job_id
  .imaging_submit_job(.imaging_enrich_job_resources(job))
}

#' Submit a chained segmentation and radiomics job
#' @param request_encoded B64/JSON request payload.
#' @return A job handle assigned server-side.
#' @export
imagingProcessSegmentAndExtractDS <- function(request_encoded) {
  req <- .imaging_request(request_encoded)
  dataset_id <- .required_scalar(req$dataset_id, "dataset_id")
  image_asset <- req$image_asset %||% "images"
  segmenter <- req$segmenter %||% stop("segmenter is required.", call. = FALSE)
  profile <- req$profile %||% list(name = "ibsi_ct_3d_v1", bin_width = 25)
  visibility <- req$visibility %||% "private"

  seg_hash <- compute_derivation_hash(dataset_id = dataset_id,
    image_asset = image_asset, provider = segmenter$provider,
    task = segmenter$task %||% "default",
    model_name = segmenter$model_name %||% segmenter$bundle_name %||% "default")
  full_hash <- compute_derivation_hash(dataset_id = dataset_id,
    image_asset = image_asset, segmenter = segmenter,
    profile_name = profile$name, bin_width = profile$bin_width)
  existing <- find_asset_by_hash(dataset_id, full_hash)
  if (!is.null(existing)) return(.imaging_reuse_handle(existing, dataset_id))

  seg_runner <- if (identical(segmenter$provider, "existing_mask_asset")) {
    NULL
  } else {
    .imaging_segmenter_runner(segmenter$provider)
  }
  mask_asset_for_extract <- segmenter$mask_asset %||% "masks"
  steps <- list(.imaging_step_resolve_dataset(dataset_id))

  if (!is.null(seg_runner)) {
    existing_seg <- find_asset_by_hash(dataset_id, seg_hash)
    if (!is.null(existing_seg)) {
      mask_asset_for_extract <- existing_seg
      seg_runner <- NULL
    }
  }
  if (!is.null(seg_runner)) {
    seg_config <- segmenter
    seg_config$image_asset <- image_asset
    mask_publish_step <- .imaging_step_publish_asset(dataset_id, "masks",
      asset_type = "mask_root", publish_kind = "imaging_asset")
    mask_publish_step$runner <- seg_runner
    mask_publish_step$config <- seg_config
    mask_publish_step$derivation_hash <- seg_hash
    steps <- c(steps, list(.imaging_step_run_artifact(seg_runner,
      config = seg_config), mask_publish_step))
  }

  extract_config <- c(profile, list(mask_asset = mask_asset_for_extract,
    image_asset = image_asset, settings_file = profile$name))
  radiomics_publish_step <- .imaging_step_publish_asset(dataset_id, "radiomics",
    asset_type = "feature_table", publish_kind = "imaging_radiomics_asset")
  radiomics_publish_step$runner <- "pyradiomics_extract"
  radiomics_publish_step$config <- extract_config
  radiomics_publish_step$derivation_hash <- full_hash
  extract_step <- .imaging_step_run_artifact("pyradiomics_extract",
    config = extract_config)
  if (!is.null(seg_runner)) extract_step$inputs <- list(2L)
  steps <- c(steps, list(extract_step, radiomics_publish_step,
    .imaging_step_safe_summary()))

  job <- .imaging_job(label = "dsImaging",
    name = req$name %||% paste(dataset_id, "segment and extract"),
    tags = c("segment_and_extract", dataset_id, segmenter$provider, profile$name),
    visibility = visibility, steps = steps)
  if (!is.null(req$job_id)) job$job_id <- req$job_id
  .imaging_submit_job(.imaging_enrich_job_resources(job))
}

#' Submit a generic clinical imaging asset workflow
#' @param request_encoded B64/JSON request payload.
#' @return A job handle assigned server-side.
#' @export
imagingProcessAssetWorkflowDS <- function(request_encoded) {
  req <- .imaging_request(request_encoded)
  .imaging_submit_asset_workflow(req)
}

#' Submit a QC metrics workflow
#' @param request_encoded B64/JSON request payload.
#' @return A job handle assigned server-side.
#' @export
imagingProcessQcCollectionDS <- function(request_encoded) {
  req <- .imaging_request(request_encoded)
  req$runner <- req$runner %||% "imaging_qc_metrics"
  req$label_tag <- req$label_tag %||% "qc_metrics"
  req$output_asset <- req$output_asset %||% "imaging_qc"
  req$asset_type <- req$asset_type %||% "qc_table"
  .imaging_submit_asset_workflow(req)
}

#' Submit a DICOM conversion workflow
#' @param request_encoded B64/JSON request payload.
#' @return A job handle assigned server-side.
#' @export
imagingConvertCollectionDS <- function(request_encoded) {
  req <- .imaging_request(request_encoded)
  req$runner <- req$runner %||% "dicom_convert"
  req$label_tag <- req$label_tag %||% "dicom_convert"
  req$output_asset <- req$output_asset %||% "nifti_images"
  req$asset_type <- req$asset_type %||% "image_root"
  .imaging_submit_asset_workflow(req)
}

#' Publish a completed radiomics collection
#' @param request_encoded B64/JSON request payload.
#' @return Publication metadata assigned server-side.
#' @export
imagingPublishRadiomicsAssetDS <- function(request_encoded) {
  req <- .imaging_request(request_encoded)
  imagingRadiomicsPublishCollectionDS(.dsr_encode(req$generation_id),
    .dsr_encode(req$dataset_id %||% NULL),
    .dsr_encode(isTRUE(req$allow_partial)))
}

#' Load radiomics features through the domain asset catalogue
#' @param request_encoded B64/JSON request payload.
#' @return Data frame assigned server-side.
#' @export
imagingLoadRadiomicsFeaturesDS <- function(request_encoded) {
  req <- .imaging_request(request_encoded)
  imagingLoadAssetDS(.required_scalar(req$dataset_id, "dataset_id"),
    .required_scalar(req$asset_id_or_alias %||% req$asset_id %||% req$alias,
      "asset_id_or_alias"),
    columns = req$columns %||% NULL,
    include_metadata = isTRUE(req$include_metadata),
    syntactic_names = isTRUE(req$syntactic_names))
}

#' Get collection status
#' @param generation_id Generation id or encoded generation id.
#' @export
imagingCollectionStatusDS <- function(generation_id) {
  imagingRadiomicsCollectionStatusDS(.dsr_encode(.decode_maybe(generation_id)))
}

#' Recover a collection generation
#' @param generation_id Generation id or encoded generation id.
#' @export
imagingCollectionRecoverDS <- function(generation_id) {
  imagingRadiomicsRecoverCollectionDS(.dsr_encode(.decode_maybe(generation_id)))
}

#' List asset generations
#' @param dataset_id Optional dataset id.
#' @param state Optional generation state.
#' @export
imagingListGenerationsDS <- function(dataset_id = NULL, state = NULL) {
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

#' Describe a radiomics profile
#' @param profile_name Profile name.
#' @export
imagingDescribeProfileDS <- function(profile_name) {
  profile_name <- .required_scalar(.decode_maybe(profile_name), "profile_name")
  profile <- read_radiomics_profile(profile_name)
  profile$name <- profile$name %||% profile_name
  profile
}

#' List radiomics profiles
#' @export
imagingListProfilesDS <- function() {
  data.frame(profile_name = list_radiomics_profiles(), stringsAsFactors = FALSE)
}

#' @keywords internal
.imaging_request <- function(request_encoded) {
  req <- .dsr_decode(request_encoded)
  if (!is.list(req)) stop("Workflow request must be a list.", call. = FALSE)
  req
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
.imaging_submit_job <- function(job) {
  if (!is.character(job$label) || length(job$label) != 1L ||
      is.na(job$label) || !nzchar(job$label)) {
    stop("dsImaging dsHPC submissions must carry a non-empty label.",
      call. = FALSE)
  }
  job$.owner <- .dsr_owner_id()
  result <- dsHPC::hpcSubmitDS(.dsr_encode(job))
  c(result, list(domain = "dsImaging", label = job$label,
    visibility = job$visibility))
}

#' @keywords internal
.imaging_reuse_handle <- function(asset_id, dataset_id) {
  list(action = "reused", deduplicated = TRUE, asset_id = asset_id,
    dataset_id = dataset_id, domain = "dsImaging")
}

#' @keywords internal
.imaging_segmenter_runner <- function(provider) {
  switch(provider,
    totalsegmentator = "totalsegmentator_infer",
    totalsegmentator_infer = "totalsegmentator_infer",
    lungmask = "lungmask_infer",
    lungmask_infer = "lungmask_infer",
    ct_lung_threshold = "ct_lung_threshold",
    nnunetv2_predict = "nnunetv2_predict",
    monai_bundle_infer = "monai_bundle_infer",
    stop("Unknown segmentation provider: ", provider, call. = FALSE))
}

#' @keywords internal
.imaging_submit_asset_workflow <- function(req) {
  dataset_id <- .required_scalar(req$dataset_id, "dataset_id")
  runner <- .required_scalar(req$runner, "runner")
  config <- req$config %||% list()
  if (is.null(config$dataset_id)) config$dataset_id <- dataset_id
  output_asset <- .required_scalar(req$output_asset, "output_asset")
  asset_type <- req$asset_type %||% "derived"
  visibility <- req$visibility %||% "private"
  label_tag <- req$label_tag %||% runner

  publish_step <- .imaging_step_publish_asset(dataset_id, output_asset,
    asset_type = asset_type, publish_kind = req$publish_kind %||% "imaging_asset")
  publish_step$alias <- req$alias %||% NULL
  publish_step$runner <- runner
  publish_step$config <- config
  if (!is.null(req$derivation_hash)) publish_step$derivation_hash <- req$derivation_hash

  job <- .imaging_job(label = "dsImaging",
    name = req$name %||% paste(dataset_id, label_tag),
    tags = c(label_tag, dataset_id),
    visibility = visibility,
    steps = list(.imaging_step_resolve_dataset(dataset_id),
      .imaging_step_run_artifact(runner, config = config),
      publish_step, .imaging_step_safe_summary()))
  if (!is.null(req$job_id)) job$job_id <- req$job_id
  .imaging_submit_job(.imaging_enrich_job_resources(job))
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
    item <- tryCatch(contentHashDS(resource_name), error = function(e) NULL)
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
