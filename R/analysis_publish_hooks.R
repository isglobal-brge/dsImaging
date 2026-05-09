# Module: dsHPC Publisher Hooks
# Called by dsHPC when a publish_asset step completes.

#' Radiomics asset publisher (dsHPC plugin) -- collection-level
#' @keywords internal
.radiomics_publisher <- function(job_id, step, output_dir, db) {
  dataset_id <- step$dataset_id
  asset_name <- step$asset_name
  asset_type <- step$asset_type %||% "feature_table"

  if (!requireNamespace("dsImaging", quietly = TRUE)) {
    warning("dsImaging required for asset publishing.", call. = FALSE)
    return(list(status = "skipped"))
  }

  deriv_hash <- NULL
  if (!is.null(step$config)) {
    deriv_hash <- compute_derivation_hash(
      dataset_id = dataset_id,
      runner = step$runner %||% "pyradiomics",
      config = step$config
    )
  }

  provenance <- list(
    runner = step$runner %||% "pyradiomics",
    job_id = job_id,
    config = step$config
  )
  provenance$output <- .imaging_output_metadata(output_dir)

  asset_id <- register_derived_asset(
    dataset_id = dataset_id,
    kind = asset_type,
    path_or_root = output_dir,
    derivation_hash = deriv_hash,
    provenance = provenance,
    created_by_job = job_id,
    description = step$description %||% paste(asset_type, "from job", job_id),
    alias = step$alias
  )

  list(status = "published", asset_id = asset_id,
       dataset_id = dataset_id, kind = asset_type)
}

#' Generic imaging asset publisher (dsHPC plugin).
#'
#' Registers the output directory of a dsHPC publish step as a first-class
#' dsImaging asset. This is used by segmentation-only jobs to publish mask
#' roots, and can also publish derived image roots, feature tables, embeddings,
#' or quality-control artifacts.
#'
#' @keywords internal
.imaging_asset_publisher <- function(job_id, step, output_dir, db) {
  dataset_id <- step$dataset_id
  asset_name <- step$asset_name
  asset_type <- step$asset_type %||% step$kind %||% "derived_asset"
  cfg <- step$config %||% list()

  deriv_hash <- step$derivation_hash %||% cfg$derivation_hash
  if (is.null(deriv_hash)) {
    deriv_hash <- compute_derivation_hash(
      dataset_id = dataset_id,
      asset_name = asset_name,
      asset_type = asset_type,
      config = cfg
    )
  }

  provenance <- list(
    type = "dshpc_publish",
    job_id = job_id,
    asset_name = asset_name,
    runner = step$runner,
    config = cfg
  )
  provenance$output <- .imaging_output_metadata(output_dir)

  asset_id <- register_derived_asset(
    dataset_id = dataset_id,
    kind = asset_type,
    path_or_root = output_dir,
    derivation_hash = deriv_hash,
    provenance = provenance,
    created_by_job = job_id,
    description = step$description %||% paste(asset_type, "from job", job_id),
    alias = step$alias
  )

  list(status = "published", asset_id = asset_id,
       dataset_id = dataset_id, kind = asset_type)
}

#' Per-image result publisher (dsHPC plugin)
#'
#' Called when a per-image job completes its publish step.
#' Four responsibilities:
#'   1. Record item as completed in the generation
#'   2. Atomically increment completed_n counter
#'   3. Register per-image result as individual asset (cross-user dedup)
#'   4. Auto-submit next batch of pending images (server-side drip feed)
#'
#' Step 4 is what makes the system "fire and forget": the user kicks off
#' the first batch, then the server self-sustains by submitting more work
#' as slots free up. No client connection required.
#' @keywords internal
.radiomics_image_publisher <- function(job_id, step, output_dir, db) {
  config <- step$config
  generation_id <- config$generation_id
  sample_id <- config$sample_id
  dataset_id <- config$dataset_id
  spec_hash <- config$spec_hash

  if (!requireNamespace("dsImaging", quietly = TRUE)) {
    warning("dsImaging required for per-image publishing.", call. = FALSE)
    return(list(status = "skipped"))
  }

  # Find the artifact path (output from previous extraction step)
  artifact_relpath <- NULL
  if (!is.null(output_dir) && dir.exists(output_dir)) {
    files <- list.files(output_dir, recursive = TRUE)
    parquet <- files[grepl("\\.parquet$", files)]
    csv <- files[grepl("\\.csv$", files)]
    nifti <- files[grepl("\\.nii(\\.gz)?$", files)]
    artifact_relpath <- if (length(parquet) > 0) parquet[1]
                        else if (length(csv) > 0) csv[1]
                        else if (length(nifti) > 0) nifti[1]
                        else if (length(files) > 0) files[1]
                        else NULL
    if (!is.null(artifact_relpath))
      artifact_relpath <- file.path(output_dir, artifact_relpath)
  }

  # Single atomic transaction: item status + counter + asset registration
  asset_reg <- NULL
  if (!is.null(spec_hash) && !is.null(artifact_relpath)) {
    asset_reg <- list(
      dataset_id = dataset_id,
      kind = "per_image_result",
      path_or_root = artifact_relpath,
      derivation_hash = spec_hash,
      provenance = list(type = "per_image", job_id = job_id,
                         generation_id = generation_id, sample_id = sample_id,
                         runner = step$runner,
                         config = config,
                         output = .imaging_output_metadata(output_dir)),
      created_by_job = job_id
    )
  }

  complete_item_atomic(
    generation_id = generation_id,
    sample_id = sample_id,
    status = "completed",
    artifact_relpath = artifact_relpath,
    register_asset = asset_reg
  )

  # 4. Server-side drip feed: auto-submit next batch of pending images
  tryCatch(
    .drip_feed_next_batch(generation_id, dataset_id),
    error = function(e) {
      update_generation(generation_id,
        error = paste("Drip-feed failed:", conditionMessage(e)))
      NULL
    }
  )

  list(status = "published", generation_id = generation_id,
       sample_id = sample_id, job_id = job_id)
}

#' Extract compact runner output metadata for asset provenance.
#'
#' Runner summaries stay server-side with the artifact. The publisher keeps a
#' compact copy of counts, formats, and package versions in the asset
#' provenance so downstream audits can identify the exact execution stack.
#'
#' @keywords internal
.imaging_output_metadata <- function(output_dir) {
  if (is.null(output_dir) || !dir.exists(output_dir)) return(list())
  files <- list.files(output_dir, pattern = "summary\\.json$",
                      recursive = TRUE, full.names = TRUE)
  if (length(files) == 0) return(list())
  files <- files[seq_len(min(length(files), 20L))]

  summaries <- list()
  versions <- list()
  for (path in files) {
    obj <- tryCatch(
      jsonlite::fromJSON(path, simplifyVector = FALSE),
      error = function(e) NULL)
    if (!is.list(obj)) next
    key <- sub("\\.json$", "", basename(path))
    root <- normalizePath(output_dir, winslash = "/", mustWork = FALSE)
    rel <- normalizePath(path, winslash = "/", mustWork = FALSE)
    prefix <- paste0(root, "/")
    if (startsWith(rel, prefix)) rel <- substring(rel, nchar(prefix) + 1L)
    compact <- obj[setdiff(names(obj), c("columns", "samples"))]
    compact$file <- rel
    summaries[[key]] <- compact
    if (is.list(obj$versions)) versions[[key]] <- obj$versions
  }

  out <- list(summaries = summaries)
  if (length(versions) > 0) out$runner_versions <- versions
  out
}

#' Auto-submit next batch of pending images from the generation spec
#'
#' Called from the publisher hook after each job completes.
#' Checks if there are pending items that haven't been submitted yet,
#' and submits a batch if there are free slots.
#'
#' The generation's spec_json stores the full orchestration config
#' (segmenter, profile, fingerprints for pending items). The drip feeder
#' reads this to know what to submit next.
#' @keywords internal
.drip_feed_next_batch <- function(generation_id, dataset_id) {
  # dsHPC is in Imports, always available

  gen <- get_generation(generation_id)
  if (is.null(gen) || !gen$state %in% c("RUNNING", "PENDING")) return(invisible(NULL))
  .sync_active_jobs(generation_id)
  requeue_stale_claimed_items(generation_id)

  # Check how many per-image jobs for this generation are currently active
  active_n <- dsHPC::count_active_jobs(paste0("%", generation_id, "%"))

  max_inflight <- .imaging_max_inflight()
  if (active_n >= max_inflight) return(invisible(NULL))

  # Read the generation spec before claiming rows. If this fails, leave items
  # pending so the next status/publish call can retry instead of stranding them
  # in "claimed".
  spec <- tryCatch(
    jsonlite::fromJSON(gen$spec_json, simplifyVector = FALSE),
    error = function(e) NULL)
  if (is.null(spec)) return(invisible(NULL))

  segmenter <- spec$segmenter
  profile <- spec$profile
  if (!is.list(profile)) {
    profile <- list(name = spec$profile_name %||% profile, bin_width = 25L)
  }
  profile_name <- profile$name %||% spec$profile_name
  processor <- spec$processor
  profile_signature <- spec$profile_signature %||%
    .radiomics_profile_signature(profile)

  resolved <- .resolve_ds(dataset_id)
  if (is.null(resolved))
    resolved <- .resolve_ds_from_generation(generation_id, dataset_id)
  if (is.null(resolved))
    stop("Cannot resolve dataset for drip-feed: ", dataset_id, call. = FALSE)

  manifest <- resolved$manifest
  if (is.null(manifest) && !is.null(resolved$manifest_uri)) {
    manifest <- tryCatch(
      parse_manifest(resolved$manifest_uri, resolved$backend),
      error = function(e) NULL)
  }
  if (is.null(manifest))
    stop("Cannot load manifest for drip-feed: ", dataset_id, call. = FALSE)

  backend <- resolved$backend
  image_root <- manifest$assets$images$uri
  mask_root <- .resolve_mask_root(dataset_id, segmenter, resolved = resolved)
  mask_hashes <- .existing_mask_hashes(resolved, manifest, segmenter)
  seg_runner <- switch(segmenter$provider,
    existing_mask_asset = NULL,
    totalsegmentator = "totalsegmentator_infer",
    totalsegmentator_infer = "totalsegmentator_infer",
    lungmask = "lungmask_infer",
    lungmask_infer = "lungmask_infer",
    ct_lung_threshold = "ct_lung_threshold",
    nnunetv2 = "nnunetv2_predict",
    nnunetv2_predict = "nnunetv2_predict",
    monai = "monai_bundle_infer",
    monai_bundle_infer = "monai_bundle_infer",
    NULL)

  # Atomically claim pending items -- prevents duplicate submissions
  # when multiple publisher hooks fire concurrently
  batch_size <- min(
    as.integer(.imaging_analysis_option("batch_size", 10L)),
    max_inflight - active_n)
  if (batch_size <= 0) return(invisible(NULL))

  batch_ids <- claim_pending_items(
    generation_id, batch_size,
    claimer_id = paste0("drip_", Sys.getpid()))
  if (length(batch_ids) == 0) return(invisible(NULL))

  # Get content hashes for these samples from dsImaging
  content_hashes <- get_content_hashes(dataset_id, batch_ids)

  for (sid in batch_ids) {
    ch <- content_hashes[[sid]]
    if (is.null(ch) || !nzchar(ch)) {
      complete_item_atomic(generation_id, sid, "failed",
        error = "Content hash not found for drip-feed submission")
      next
    }

    mask_ch <- mask_hashes[[sid]]
    spec_hash <- compute_image_derivation_hash(
      content_hash = ch,
      processor = processor,
      params = list(
        segmenter = segmenter,
        profile = profile,
        profile_signature = profile_signature,
        mask_content_hash = mask_ch
      )
    )

    # Cross-user dedup. Reuse only if the stored artifact path still exists
    # and matches the current profile contract.
    existing <- .existing_per_image_asset(dataset_id, spec_hash,
      selected_features = profile$selected_features)
    if (!is.null(existing)) {
      complete_item_atomic(generation_id, sid, "completed",
        artifact_relpath = existing$path)
      next
    }

    image_uri <- .resolve_sample_image(image_root, sid,
      dataset_id = dataset_id, backend = backend)
    if (is.null(image_uri)) {
      complete_item_atomic(generation_id, sid, "failed",
        error = "Image file not found")
      next
    }
    image_path <- .stage_image_for_job(image_uri, sid, dataset_id, backend)

    settings_path <- .resolve_profile_path(profile_name)

    steps <- list()
    steps[[1]] <- list(
      type = "emit", output_name = "image_config",
      value = list(image_path = image_path, sample_id = sid,
                    dataset_id = dataset_id, generation_id = generation_id))

    if (!is.null(seg_runner)) {
      seg_config <- segmenter
      seg_config$image <- image_path
      seg_config$sample_id <- sid
      seg_config$generation_id <- generation_id
      steps[[length(steps) + 1]] <- list(
        type = "segment", runner = seg_runner,
        name = "segment_single", config = seg_config)
    }

    extract_config <- c(profile, list(
      image = image_path, sample_id = sid,
      generation_id = generation_id,
      settings_file = settings_path %||% "default"))
    extract_config <- .normalise_extract_config(extract_config)
    if (!is.null(mask_root)) {
      mp <- .resolve_sample_mask(mask_root, sid, backend = backend,
        manifest = manifest, mask_asset = segmenter$mask_asset %||% "masks")
      if (is.null(mp)) {
        complete_item_atomic(generation_id, sid, "failed",
          error = "Mask file not found")
        next
      }
      extract_config$mask <- .stage_backend_file_for_job(mp, sid, dataset_id,
        backend, role = "masks")
    }
    steps[[length(steps) + 1]] <- list(
      type = "extract", runner = "pyradiomics_extract",
      name = "extract_single", config = extract_config)

    steps[[length(steps) + 1]] <- list(
      type = "publish_asset",
      publish_kind = "imaging_radiomics_image_result",
      publisher_package = "dsImaging",
      config = list(generation_id = generation_id, sample_id = sid,
                     dataset_id = dataset_id, spec_hash = spec_hash))

    job_spec <- list(
      label = "dsImaging_image",
      tags = c("per_image", dataset_id, sid, generation_id),
      visibility = "private", steps = steps,
      .owner = gen$owner_id)

    tryCatch({
      spec_enc <- .dsr_encode(job_spec)
      dsHPC::hpcSubmitDS(spec_enc)
      record_item_status(generation_id, sid, "running")
    }, error = function(e) {
      msg <- conditionMessage(e)
      if (.is_transient_job_submit_error(msg)) {
        record_item_status(generation_id, sid, "pending",
          error = paste("Drip-feed submit deferred:", msg))
      } else {
        complete_item_atomic(generation_id, sid, "failed",
          error = paste("Drip-feed submit failed:", msg))
      }
    })
  }

  invisible(NULL)
}
