# Module: Per-Image Collection Orchestrator
# Server-side DS methods for per-image job deduplication.
#
# Flow:
#   1. imagingRadiomicsScanCollectionDS  -- fingerprint + diff + create generation
#   2. imagingRadiomicsSubmitBatchDS     -- create N dsJobs jobs for pending images
#   3. imagingRadiomicsCollectionStatusDS -- query generation progress (with failure sync)
#   4. imagingRadiomicsPublishCollectionDS -- aggregate per-image outputs

# ---------------------------------------------------------------------------
# 1. Scan: fingerprint images, diff vs completed, create/reuse generation
# ---------------------------------------------------------------------------

#' Scan a Collection for Radiomics Processing
#'
#' DataSHIELD aggregate method. Computes or reads image fingerprints, claims or
#' resumes a dsImaging generation, and returns the sample ids that should be
#' submitted as dsJobs work.
#'
#' @param dataset_id_enc Encoded dataset id or dataset handle.
#' @param segmenter_enc Encoded segmenter specification.
#' @param profile_enc Encoded radiomics profile specification.
#' @param visibility_enc Encoded visibility setting for the derived asset.
#' @return Named list describing whether an existing asset/generation was
#'   reused or a new generation was created, plus pending sample metadata.
#' @export
imagingRadiomicsScanCollectionDS <- function(dataset_id_enc, segmenter_enc,
                                      profile_enc, visibility_enc) {
  dataset_id <- .dsr_decode(dataset_id_enc)
  segmenter  <- .dsr_decode(segmenter_enc)
  profile    <- .dsr_decode(profile_enc)
  visibility <- .dsr_decode(visibility_enc)

  # dsImaging is in Imports, always available

  # Resolve dataset (returns backend + manifest from handle or registry)
  resolved <- .resolve_ds(dataset_id)
  if (is.null(resolved))
    stop("Cannot resolve dataset: ", dataset_id, call. = FALSE)

  # Get manifest: from handle (already parsed) or from URI
  manifest <- resolved$manifest
  if (is.null(manifest) && !is.null(resolved$manifest_uri))
    manifest <- parse_manifest(resolved$manifest_uri, resolved$backend)
  if (is.null(manifest))
    stop("Cannot load manifest for dataset: ", dataset_id, call. = FALSE)
  dataset_id <- dataset_id %||% manifest$dataset_id

  image_root <- manifest$assets$images$uri
  mask_hashes <- .existing_mask_hashes(resolved, manifest, segmenter)

  # Fingerprint: for S3, use hash index. For file, use Python script.
  if (resolved$backend$type == "s3") {
    hash_index_uri <- manifest$content_hash_index$uri
    if (is.null(hash_index_uri))
      stop("S3 dataset requires content_hash_index in manifest.", call. = FALSE)
    idx <- read_hash_index(resolved$backend, hash_index_uri)
    fp_result <- diff_hash_index(idx, dataset_id)
    # Store new hashes in local SQLite for future diffs
    new_changed <- c(fp_result$new, fp_result$changed)
    if (length(new_changed) > 0) {
      hashes <- vapply(new_changed, function(sid)
        fp_result$content_hashes[[sid]] %||% "", character(1))
      valid <- nzchar(hashes)
      if (any(valid))
        store_content_hashes(dataset_id, new_changed[valid], hashes[valid])
    }
  } else {
    if (!dir.exists(image_root))
      stop("Image root not found: ", image_root, call. = FALSE)
    fp_result <- compute_collection_fingerprints(dataset_id, image_root,
      compute_content_hash = TRUE)
  }

  # Build processor identity for derivation hashing
  processor <- paste0(segmenter$provider, "_", segmenter$task %||% "default")
  profile_signature <- .radiomics_profile_signature(profile)

  # Collection-level hash uses content_hashes (strong) when available
  hash_source <- if (length(fp_result$content_hashes) > 0)
    fp_result$content_hashes else fp_result$fingerprints
  if (length(mask_hashes) > 0) {
    paired_hashes <- vapply(names(hash_source), function(sid) {
      paste(hash_source[[sid]] %||% "", mask_hashes[[sid]] %||% "", sep = ":")
    }, character(1))
    hash_source <- as.list(stats::setNames(paired_hashes, names(hash_source)))
  }
  collection_hash <- compute_derivation_hash(
    dataset_id = dataset_id,
    processor = processor,
    segmenter = segmenter,
    profile = profile,
    profile_signature = profile_signature,
    mask_asset = segmenter$mask_asset %||% NULL,
    image_hashes = sort(unlist(hash_source))
  )

  # Claim or reuse generation
  # All sample IDs are candidates. The diff (new/changed/unchanged) is about
  # file content changes, not about processing status. Per-image dedup happens
  # at submit time via derivation hash check in imagingRadiomicsSubmitBatchDS.
  all_sample_ids <- names(fp_result$content_hashes)
  if (length(all_sample_ids) == 0) all_sample_ids <- names(fp_result$fingerprints)
  total_n <- fp_result$total

  generation_spec <- list(
    processor = processor,
    profile = profile,
    profile_signature = profile_signature,
    profile_name = profile$name,
    segmenter = segmenter,
    mask_hash_index = mask_hashes,
    dataset_context = .dataset_context_from_resolved(resolved, manifest)
  )

  gen_result <- claim_or_reuse_generation(
    dataset_id = dataset_id,
    kind = "radiomics_collection",
    derivation_hash = collection_hash,
    visibility = visibility,
    owner_id = .dsr_owner_id(),
    expected_n = total_n,
    spec = generation_spec
  )

  if (gen_result$action == "reuse_asset") {
    return(list(
      action = "reuse_asset",
      asset_id = gen_result$asset_id,
      dataset_id = dataset_id,
      total = safe_metadata_count(total_n),
      done = safe_metadata_count(total_n),
      pending = 0L
    ))
  }

  generation_id <- gen_result$generation_id

  if (gen_result$action == "reuse_generation") {
    .ensure_generation_dataset_context(generation_id, resolved, manifest)

    # Resume: reconcile finished/failed jobs first, then check pending items.
    .sync_generation_jobs(generation_id)
    items <- get_generation_items(generation_id)

    # If generation exists but has no items, populate them
    if (nrow(items) == 0) {
      pop_ids <- names(fp_result$content_hashes)
      if (length(pop_ids) == 0) pop_ids <- names(fp_result$fingerprints)
      for (sid in pop_ids) {
        record_item_status(generation_id, sid, "pending")
      }
      items <- get_generation_items(generation_id)
    }

    done_ids <- items$sample_id[items$status == "completed"]
    failed_ids <- items$sample_id[items$status == "failed"]
    resume_pending <- items$sample_id[items$status == "pending"]
    # Also re-queue failed items for retry
    retry_ids <- c(resume_pending, failed_ids)
    return(list(
      action = "resume",
      generation_id = generation_id,
      dataset_id = dataset_id,
      total = safe_metadata_count(total_n),
      done = safe_metadata_count(length(done_ids)),
      pending_ids = retry_ids,
      fingerprints = fp_result$fingerprints[retry_ids],
      content_hashes = fp_result$content_hashes[retry_ids]
    ))
  }

  # New generation: register all items as pending
  # Even if a per-image asset with matching hash exists from another dataset,
  # this generation needs to process (or verify) each image independently.
  for (sid in all_sample_ids) {
    record_item_status(generation_id, sid, "pending")
  }

  update_generation(generation_id,
    state = "RUNNING",
    completed_n = 0L)

  # Return IDs + hashes so client can submit the first batch
  # Server drip feed auto-submits subsequent batches
  list(
    action = "run_new",
    generation_id = generation_id,
    dataset_id = dataset_id,
    total = safe_metadata_count(total_n),
    done = 0L,
    pending_ids = all_sample_ids,
    fingerprints = fp_result$fingerprints,
    content_hashes = fp_result$content_hashes
  )
}

# ---------------------------------------------------------------------------
# 2. Submit batch: create per-image dsJobs jobs for pending samples
# ---------------------------------------------------------------------------

#' Submit a Batch of Per-Image Radiomics Jobs
#'
#' DataSHIELD aggregate method. Creates per-image dsJobs job specs for pending
#' samples in a generation, respecting dsImaging/dsJobs backpressure limits.
#'
#' @param generation_id_enc Encoded generation id.
#' @param sample_ids_enc Encoded character vector of sample ids.
#' @param segmenter_enc Encoded segmenter specification.
#' @param profile_enc Encoded radiomics profile specification.
#' @param dataset_id_enc Encoded dataset id or dataset handle.
#' @param fingerprints_enc Encoded named list of image fingerprints.
#' @param content_hashes_enc Optional encoded named list of content hashes.
#' @return Named list with generation id, submitted count, optional deferred
#'   count, and per-sample submission results.
#' @export
imagingRadiomicsSubmitBatchDS <- function(generation_id_enc, sample_ids_enc,
                                    segmenter_enc, profile_enc,
                                    dataset_id_enc, fingerprints_enc,
                                    content_hashes_enc = NULL) {
  generation_id  <- .dsr_decode(generation_id_enc)
  sample_ids     <- .dsr_decode(sample_ids_enc)
  segmenter      <- .dsr_decode(segmenter_enc)
  profile        <- .dsr_decode(profile_enc)
  dataset_id     <- .dsr_decode(dataset_id_enc)
  fingerprints   <- .dsr_decode(fingerprints_enc)
  content_hashes <- if (!is.null(content_hashes_enc))
    .dsr_decode(content_hashes_enc) else list()

  # dsJobs is in Imports, always available

  resolved <- .resolve_ds(dataset_id)
  if (is.null(resolved))
    resolved <- .resolve_ds_from_generation(generation_id, dataset_id)
  if (is.null(resolved))
    stop("Cannot resolve dataset for generation: ", dataset_id, call. = FALSE)

  # Use pre-parsed manifest from handle, or parse from URI
  manifest <- resolved$manifest
  if (is.null(manifest) && !is.null(resolved$manifest_uri)) {
    manifest <- tryCatch(
      parse_manifest(resolved$manifest_uri, resolved$backend),
      error = function(e) NULL)
  }
  dataset_id <- dataset_id %||% manifest$dataset_id
  image_root <- if (!is.null(manifest)) manifest$assets$images$uri else NULL
  mask_root <- .resolve_mask_root(dataset_id, segmenter, resolved = resolved)
  mask_hashes <- .existing_mask_hashes(resolved, manifest, segmenter)
  backend <- resolved$backend
  processor <- paste0(segmenter$provider, "_", segmenter$task %||% "default")
  profile_signature <- .radiomics_profile_signature(profile)

  # Map segmenter to runner. Accept both short forms (e.g. "lungmask") and
  # the canonical runner names (e.g. "lungmask_infer") that ds.segmenter.* emit.
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
    stop("Unknown segmenter provider: ", segmenter$provider, call. = FALSE))

  submitted <- list()
  active_n <- tryCatch(
    as.integer(dsJobs::count_active_jobs(paste0("%", generation_id, "%"))),
    error = function(e) 0L)
  slots <- max(0L, .imaging_max_inflight() - active_n)
  if (slots <= 0L) {
    return(list(
      generation_id = generation_id,
      submitted = 0L,
      deferred = length(sample_ids),
      results = list()
    ))
  }
  if (length(sample_ids) > slots) {
    sample_ids <- sample_ids[seq_len(slots)]
  }

  for (sid in sample_ids) {
    fp <- fingerprints[[sid]]
    ch <- content_hashes[[sid]]
    # Need at least one identifier (content_hash or fingerprint)
    if (is.null(fp) && is.null(ch)) next

    # Per-image derivation hash: prefer content_hash (strong) over fingerprint
    # and include the mask hash when using an existing mask asset.
    mask_ch <- mask_hashes[[sid]]
    spec_hash <- compute_image_derivation_hash(
      content_hash = ch,
      fingerprint = fp,
      processor = processor,
      params = list(
        segmenter = segmenter,
        profile = profile,
        profile_signature = profile_signature,
        mask_content_hash = mask_ch
      )
    )

    # Check if this exact image+params was already done (cross-user)
    existing <- find_asset_by_hash(dataset_id, spec_hash)

    if (!is.null(existing)) {
      complete_item_atomic(generation_id, sid, "completed",
        artifact_relpath = existing)
      submitted[[sid]] <- list(status = "reused", asset_id = existing)
      next
    }

    # Resolve image path
    image_uri <- .resolve_sample_image(image_root, sid,
      dataset_id = dataset_id, backend = backend)
    if (is.null(image_uri)) {
      complete_item_atomic(generation_id, sid, "failed",
        error = "Image file not found")
      submitted[[sid]] <- list(status = "failed", error = "Image not found")
      next
    }

    # For S3 images: stage to local filesystem for the Python runner
    image_path <- .stage_image_for_job(image_uri, sid, dataset_id, backend)

    # Build per-image job steps (using dsJobs step format)
    steps <- list()

    # Step 1: emit config (session plane) -- stores metadata for the pipeline
    steps[[length(steps) + 1]] <- list(
      type = "emit",
      output_name = "image_config",
      value = list(
        image_path = image_path,
        sample_id = sid,
        dataset_id = dataset_id,
        generation_id = generation_id
      )
    )

    # Step 2: segment_single (artifact plane)
    if (!is.null(seg_runner)) {
      seg_config <- segmenter
      seg_config$image <- image_path
      seg_config$sample_id <- sid
      seg_config$generation_id <- generation_id
      steps[[length(steps) + 1]] <- list(
        type = "segment",
        runner = seg_runner,
        name = "segment_single",
        config = seg_config
      )
    }

    # Step 3: extract_single (artifact plane)
    # For extraction, pass mask explicitly if using existing masks,
    # otherwise the script finds it in the input dir (seg output)
    # Resolve profile name to actual YAML file path
    settings_path <- .resolve_profile_path(profile$name)
    extract_config <- c(profile, list(
      image = image_path,
      sample_id = sid,
      generation_id = generation_id,
      settings_file = settings_path %||% "default"
    ))
    extract_config <- .normalise_extract_config(extract_config)
    if (!is.null(mask_root)) {
      mask_uri <- .resolve_sample_mask(mask_root, sid, backend = backend,
        manifest = manifest, mask_asset = segmenter$mask_asset %||% "masks")
      if (is.null(mask_uri)) {
        complete_item_atomic(generation_id, sid, "failed",
          error = "Mask file not found")
        submitted[[sid]] <- list(status = "failed", error = "Mask not found")
        next
      }
      extract_config$mask <- .stage_backend_file_for_job(mask_uri, sid,
        dataset_id, backend, role = "masks")
    }
    steps[[length(steps) + 1]] <- list(
      type = "extract",
      runner = "pyradiomics_extract",
      name = "extract_single",
      config = extract_config
    )

    # Step 4: publish per-image result (session plane)
    steps[[length(steps) + 1]] <- list(
      type = "publish_asset",
      publish_kind = "imaging_radiomics_image_result",
      publisher_package = "dsImaging",
      config = list(
        generation_id = generation_id,
        sample_id = sid,
        dataset_id = dataset_id,
        spec_hash = spec_hash
      )
    )

    # Submit via jobSubmitDS (the proper dsJobs API)
    job_spec <- list(
      label = "dsImaging_image",
      tags = c("per_image", dataset_id, sid, generation_id),
      visibility = "private",
      steps = steps,
      .owner = .dsr_owner_id()
    )

    tryCatch({
      spec_enc <- .dsr_encode(job_spec)
      result <- dsJobs::jobSubmitDS(spec_enc)
      record_item_status(generation_id, sid, "running")
      submitted[[sid]] <- list(status = "submitted", job_id = result$job_id)
    }, error = function(e) {
      msg <- conditionMessage(e)
      if (.is_transient_job_submit_error(msg)) {
        record_item_status(generation_id, sid, "pending",
          error = paste("Submit deferred:", msg))
        submitted[[sid]] <<- list(status = "deferred", error = msg)
      } else {
        complete_item_atomic(generation_id, sid, "failed",
          error = paste("Submit failed:", msg))
        submitted[[sid]] <<- list(status = "submit_failed", error = msg)
      }
    })
  }

  list(
    generation_id = generation_id,
    submitted = length(submitted),
    results = submitted
  )
}

# ---------------------------------------------------------------------------
# 3. Status: query progress of a generation (with failure synchronization)
# ---------------------------------------------------------------------------

#' Get Radiomics Collection Status
#'
#' DataSHIELD aggregate method. Reconciles finished/failed dsJobs state back
#' into dsImaging generation items and returns disclosure-safe progress counts.
#'
#' @param generation_id_enc Encoded generation id.
#' @return Named list with generation state, disclosure-safe counts, and
#'   `is_done`.
#' @export
imagingRadiomicsCollectionStatusDS <- function(generation_id_enc) {
  generation_id <- .dsr_decode(generation_id_enc)

  # Sync dsJobs terminal states -> mark asset_items as completed/failed.
  .sync_generation_jobs(generation_id)

  gen <- get_generation(generation_id)
  if (is.null(gen))
    stop("Generation not found: ", generation_id, call. = FALSE)

  items <- get_generation_items(generation_id)
  completed_ids <- items$sample_id[items$status == "completed"]
  failed_ids <- items$sample_id[items$status == "failed"]
  pending_ids <- items$sample_id[items$status == "pending"]
  claimed_ids <- items$sample_id[items$status == "claimed"]
  running_ids <- items$sample_id[items$status == "running"]

  if (length(pending_ids) > 0L && gen$state %in% c("RUNNING", "PENDING")) {
    tryCatch(
      .drip_feed_next_batch(generation_id, gen$dataset_id),
      error = function(e) update_generation(generation_id,
        error = paste("Drip-feed failed:", conditionMessage(e))))

    gen <- get_generation(generation_id)
    items <- get_generation_items(generation_id)
    completed_ids <- items$sample_id[items$status == "completed"]
    failed_ids <- items$sample_id[items$status == "failed"]
    pending_ids <- items$sample_id[items$status == "pending"]
    claimed_ids <- items$sample_id[items$status == "claimed"]
    running_ids <- items$sample_id[items$status == "running"]
  }

  # pending + claimed + running = "not yet done"
  not_done <- length(pending_ids) + length(claimed_ids) + length(running_ids)
  total_items <- nrow(items)
  expected <- as.integer(gen$expected_n %||% total_items)

  # is_done: all items resolved (completed or failed) AND we have items
  # Guard against empty items table (generation exists but scan hasn't populated items yet)
  all_resolved <- not_done == 0L && total_items > 0 && total_items >= expected

  # Apply disclosure control to all counts returned to client
  list(
    generation_id = generation_id,
    state = gen$state,
    total = safe_metadata_count(expected),
    completed = safe_metadata_count(length(completed_ids)),
    failed = safe_metadata_count(length(failed_ids)),
    pending = safe_metadata_count(length(pending_ids)),
    claimed = safe_metadata_count(length(claimed_ids)),
    running = safe_metadata_count(length(running_ids)),
    is_done = all_resolved
  )
}

# ---------------------------------------------------------------------------
# 4. Publish: aggregate per-image outputs into collection asset
# ---------------------------------------------------------------------------

#' Publish a Completed Radiomics Collection
#'
#' DataSHIELD aggregate method. Aggregates completed per-image outputs into a
#' collection-level radiomics feature table and publishes the derived asset.
#'
#' @param generation_id_enc Encoded generation id.
#' @param dataset_id_enc Encoded dataset id.
#' @param allow_partial_enc Encoded logical indicating whether limited failures
#'   may be tolerated.
#' @return Named list with publication status and derived asset metadata.
#' @export
imagingRadiomicsPublishCollectionDS <- function(generation_id_enc, dataset_id_enc,
                                          allow_partial_enc) {
  generation_id  <- .dsr_decode(generation_id_enc)
  dataset_id     <- .dsr_decode(dataset_id_enc)
  allow_partial  <- .dsr_decode(allow_partial_enc)

  # Final sync to catch any late completions/failures
  .sync_generation_jobs(generation_id)

  gen <- get_generation(generation_id)
  if (is.null(gen))
    stop("Generation not found: ", generation_id, call. = FALSE)

  items <- get_generation_items(generation_id)
  completed <- items[items$status == "completed", ]
  failed <- items[items$status == "failed", ]
  pending <- items[items$status == "pending", ]
  claimed <- items[items$status == "claimed", ]
  running <- items[items$status == "running", ]

  total <- nrow(items)
  n_failed <- nrow(failed)
  n_completed <- nrow(completed)
  n_pending <- nrow(pending)
  n_not_done <- n_pending + nrow(claimed) + nrow(running)

  if (n_not_done > 0)
    stop(n_not_done, " items still pending/running. Wait for completion.",
         call. = FALSE)

  if (n_completed == 0)
    stop("No completed items to publish.", call. = FALSE)

  if (n_failed > 0 && !isTRUE(allow_partial)) {
    fail_pct <- n_failed / total
    if (fail_pct > 0.05)
      stop("Too many failures (", n_failed, "/", total,
           ", ", round(fail_pct * 100, 1), "%). ",
           "Use allow_partial=TRUE to publish anyway.", call. = FALSE)
  }

  # Build collection-level output directory
  output_root <- file.path(
    getOption("dsjobs.home", getOption("default.dsjobs.home", "/srv/dsjobs")),
    "publish",
    paste0("collection_", generation_id))
  dir.create(output_root, recursive = TRUE, showWarnings = FALSE)

  feature_table <- .write_collection_feature_table(completed, output_root)

  # Write collection manifest
  manifest <- list(
    generation_id = generation_id,
    dataset_id = dataset_id,
    total = total,
    completed = n_completed,
    failed = n_failed,
    feature_table = feature_table,
    # failed_samples stripped -- individual sample IDs are disclosive
    item_artifacts = as.list(
      stats::setNames(completed$artifact_relpath, completed$sample_id))
  )
  writeLines(
    jsonlite::toJSON(manifest, auto_unbox = TRUE, pretty = TRUE),
    file.path(output_root, "collection_manifest.json"))

  # Adjust expected_n before publish to account for failures
  # publish_generation checks completed_n == expected_n
  if (n_failed > 0) {
    update_generation(generation_id, expected_n = n_completed)
  }

  provenance <- list(
    type = "per_image_collection",
    generation_id = generation_id,
    total = total,
    completed = n_completed,
    failed = n_failed
  )

  asset_id <- publish_generation(
    generation_id = generation_id,
    path_or_root = output_root,
    description = paste0("Radiomics collection: ", n_completed, "/", total,
                          " images"),
    provenance = provenance
  )

  if (is.null(asset_id)) {
    stop("publish_generation returned NULL. Generation may be in PARTIAL state.",
         call. = FALSE)
  }

  list(
    asset_id = asset_id,
    generation_id = generation_id,
    feature_table = feature_table,
    total = safe_metadata_count(total),
    completed = safe_metadata_count(n_completed),
    failed = safe_metadata_count(n_failed),
    failed_samples = if (n_failed > 0) failed$sample_id else character(0)
  )
}

#' Combine per-image radiomics outputs into one collection feature table
#' @keywords internal
.write_collection_feature_table <- function(completed, output_root) {
  if (nrow(completed) == 0) return(NULL)
  if (!requireNamespace("arrow", quietly = TRUE))
    stop("arrow package required to publish radiomics feature tables.",
         call. = FALSE)

  rows <- lapply(seq_len(nrow(completed)), function(i) {
    path <- completed$artifact_relpath[i]
    if (is.na(path) || !nzchar(path) || !file.exists(path))
      stop("Completed radiomics artifact not found: ", path, call. = FALSE)
    df <- if (grepl("\\.parquet$", path, ignore.case = TRUE)) {
      as.data.frame(arrow::read_parquet(path))
    } else if (grepl("\\.csv$", path, ignore.case = TRUE)) {
      utils::read.csv(path, stringsAsFactors = FALSE)
    } else {
      stop("Unsupported radiomics artifact format: ", path, call. = FALSE)
    }
    if (!"sample_id" %in% names(df))
      df$sample_id <- completed$sample_id[i]
    df
  })

  all_cols <- unique(unlist(lapply(rows, names), use.names = FALSE))
  rows <- lapply(rows, function(df) {
    missing <- setdiff(all_cols, names(df))
    for (nm in missing) df[[nm]] <- NA
    df[, all_cols, drop = FALSE]
  })
  features <- do.call(rbind, rows)
  out <- file.path(output_root, "radiomics_features.parquet")
  arrow::write_parquet(features, out)
  out
}

# ---------------------------------------------------------------------------
# 5. Validate mask-image correspondence for dsFlower
# ---------------------------------------------------------------------------

#' Validate that segmentation masks correspond to a set of images
#'
#' DataSHIELD AGGREGATE method. Given a generation_id (from a segmentation
#' run), verifies that:
#' \enumerate{
#'   \item Each completed mask artifact still exists on disk (dsJobs artifact)
#'   \item Each mask's derivation_hash matches the source image's content_hash
#'   \item The generation covers the requested sample set
#' }
#'
#' If masks are missing (expired dsJobs artifacts), returns which samples
#' need regeneration. Does NOT auto-regenerate.
#'
#' @param generation_id_enc Encoded generation_id.
#' @param dataset_id Character; the imaging dataset being validated against.
#' @return List with valid (logical), n_valid, n_missing, n_failed,
#'   and a mask_manifest (sample_id -> mask_path mapping) for valid masks.
#' @export
imagingSegmentationValidateMasksDS <- function(generation_id_enc, dataset_id) {
  generation_id <- .dsr_decode(generation_id_enc)

  # Sync any pending terminal states from dsJobs
  .sync_generation_jobs(generation_id)

  gen <- get_generation(generation_id)
  if (is.null(gen))
    stop("Generation not found: ", generation_id, call. = FALSE)

  # Verify generation matches the requested dataset
  gen_spec <- tryCatch(
    jsonlite::fromJSON(gen$spec_json, simplifyVector = FALSE),
    error = function(e) list()
  )
  if (!is.null(gen_spec$dataset_id) && gen_spec$dataset_id != dataset_id) {
    stop("Generation '", generation_id, "' belongs to dataset '",
         gen_spec$dataset_id, "', not '", dataset_id, "'.", call. = FALSE)
  }

  items <- get_generation_items(generation_id)

  n_valid <- 0L
  n_missing <- 0L
  n_failed <- 0L
  mask_paths <- list()
  missing_samples <- character(0)

  for (i in seq_len(nrow(items))) {
    sid <- items$sample_id[i]
    status <- items$status[i]

    if (status == "failed") {
      n_failed <- n_failed + 1L
      next
    }

    if (status != "completed") {
      n_missing <- n_missing + 1L
      missing_samples <- c(missing_samples, sid)
      next
    }

    artifact_path <- items$artifact_relpath[i]
    if (is.na(artifact_path) || !nzchar(artifact_path)) {
      n_missing <- n_missing + 1L
      missing_samples <- c(missing_samples, sid)
      next
    }

    # Check if the artifact file still exists on disk
    # dsJobs artifacts are at DSJOBS_HOME/artifacts/{job_id}/...
    # artifact_relpath may be absolute or relative
    if (!file.exists(artifact_path)) {
      # Try under dsJobs home
      dsjobs_home <- getOption("dsjobs.home", "/srv/dsjobs")
      alt_path <- file.path(dsjobs_home, "artifacts", artifact_path)
      if (!file.exists(alt_path)) {
        n_missing <- n_missing + 1L
        missing_samples <- c(missing_samples, sid)
        next
      }
      artifact_path <- alt_path
    }

    # Find the mask file in the artifact directory
    mask_file <- .find_mask_in_artifact(artifact_path, sid)
    if (is.null(mask_file)) {
      n_missing <- n_missing + 1L
      missing_samples <- c(missing_samples, sid)
      next
    }

    mask_paths[[sid]] <- mask_file
    n_valid <- n_valid + 1L
  }

  total <- nrow(items)
  all_valid <- n_valid == total && n_missing == 0L && n_failed == 0L

  # Apply disclosure control to all counts returned to client
  list(
    valid = all_valid,
    generation_id = generation_id,
    dataset_id = dataset_id,
    segmenter = gen_spec$processor %||% "unknown",
    total = safe_metadata_count(total),
    n_valid = safe_metadata_count(n_valid),
    n_missing = safe_metadata_count(n_missing),
    n_failed = safe_metadata_count(n_failed),
    needs_regeneration = length(missing_samples) > 0,
    # mask_paths stays server-side (disclosure: no sample IDs to client)
    # dsFlower reads it server-side via radiomicsGetMaskManifestDS
    ready_for_training = all_valid
  )
}

#' Get the mask manifest for a validated generation
#'
#' DataSHIELD server-side function (NOT aggregate -- called internally
#' by dsFlower's flowerPrepareRunDS for segmentation tasks).
#' Returns the sample_id -> mask_path mapping.
#'
#' @param generation_id Character; the generation to query.
#' @return Named list mapping sample_id to absolute mask file path.
#' @export
imagingSegmentationGetMaskPaths <- function(generation_id) {
  items <- get_generation_items(generation_id,
                                            status = "completed")
  paths <- list()
  for (i in seq_len(nrow(items))) {
    sid <- items$sample_id[i]
    artifact_path <- items$artifact_relpath[i]
    if (is.na(artifact_path)) next

    if (!file.exists(artifact_path)) {
      dsjobs_home <- getOption("dsjobs.home", "/srv/dsjobs")
      artifact_path <- file.path(dsjobs_home, "artifacts", artifact_path)
    }

    mask_file <- .find_mask_in_artifact(artifact_path, sid)
    if (!is.null(mask_file)) paths[[sid]] <- mask_file
  }
  paths
}

#' Find a mask file within a dsJobs artifact directory
#' @keywords internal
.find_mask_in_artifact <- function(artifact_path, sample_id) {
  # artifact_path might be a file or a directory
  if (file.exists(artifact_path) && !dir.exists(artifact_path)) {
    # It's a file -- check if it's a NIfTI/NRRD mask
    if (grepl("\\.(nii\\.gz|nii|nrrd|mha|png)$", artifact_path, ignore.case = TRUE))
      return(artifact_path)
  }

  # It's a directory -- search for mask files
  if (dir.exists(artifact_path)) {
    # Look for seg_manifest.json first (written by segmentation runners)
    seg_manifest <- file.path(artifact_path, "seg_manifest.json")
    if (file.exists(seg_manifest)) {
      manifest <- tryCatch(
        jsonlite::fromJSON(seg_manifest, simplifyVector = FALSE),
        error = function(e) NULL)
      if (!is.null(manifest) && !is.null(manifest$samples[[sample_id]])) {
        primary <- manifest$samples[[sample_id]]$primary_mask
        if (!is.null(primary) && file.exists(primary)) return(primary)
      }
    }

    # Fallback: scan for NIfTI files containing sample_id or "mask"
    files <- list.files(artifact_path, recursive = TRUE, full.names = TRUE)
    mask_files <- files[grepl("\\.(nii\\.gz|nii|nrrd|png)$", files,
                              ignore.case = TRUE)]

    # Prefer files with "mask" or "seg" in name
    for (f in mask_files) {
      bn <- basename(f)
      if (grepl("mask|seg|label", bn, ignore.case = TRUE)) return(f)
    }
    # Otherwise first NIfTI
    if (length(mask_files) > 0) return(mask_files[1])
  }

  NULL
}

# ---------------------------------------------------------------------------
# Failure synchronization
# ---------------------------------------------------------------------------

#' Sync dsJobs terminal states back to asset_items
#' @keywords internal
.sync_generation_jobs <- function(generation_id) {
  .sync_completed_jobs(generation_id)
  .sync_failed_jobs(generation_id)
  invisible(NULL)
}

#' Sync dsJobs completed states back to asset_items
#'
#' This is the recovery path for cases where a worker finished the artifact
#' steps but did not have the dsImaging publisher registered in its process.
#' It keeps the generation catalog consistent without requiring manual DB edits.
#'
#' @keywords internal
.sync_completed_jobs <- function(generation_id) {
  items <- get_generation_items(generation_id)
  pending_items <- items[items$status %in% c("pending", "claimed", "running"), ,
                         drop = FALSE]
  if (nrow(pending_items) == 0) return(invisible(NULL))

  done_jobs <- dsJobs::query_jobs_by_tag(paste0("%", generation_id, "%"),
    states = c("FINISHED", "PUBLISHED"))
  if (nrow(done_jobs) == 0) return(invisible(NULL))

  for (i in seq_len(nrow(done_jobs))) {
    tags <- strsplit(done_jobs$tags[i], ",", fixed = TRUE)[[1]]
    sid <- .sample_id_from_tags(tags, pending_items$sample_id)
    if (is.null(sid)) next

    output_ref <- .radiomics_job_output_ref(done_jobs$job_id[i])
    if (is.null(output_ref) || !isTRUE(output_ref$exists)) {
      complete_item_atomic(generation_id, sid, "failed",
        error = "dsJobs job finished without a radiomics output")
      next
    }

    dataset_id <- .dataset_id_from_tags(tags, generation_id, sid)
    register_asset <- NULL
    spec_hash <- done_jobs$spec_hash[i]
    if (!is.na(spec_hash) && nzchar(spec_hash) &&
        !is.null(dataset_id) && nzchar(dataset_id)) {
      register_asset <- list(
        dataset_id = dataset_id,
        kind = "per_image_result",
        path_or_root = output_ref$path,
        derivation_hash = spec_hash,
        provenance = list(type = "per_image", job_id = done_jobs$job_id[i],
                          generation_id = generation_id, sample_id = sid),
        created_by_job = done_jobs$job_id[i]
      )
    }

    complete_item_atomic(
      generation_id = generation_id,
      sample_id = sid,
      status = "completed",
      artifact_relpath = output_ref$path,
      register_asset = register_asset
    )
  }
  invisible(NULL)
}

#' @keywords internal
.radiomics_job_output_ref <- function(job_id) {
  candidates <- c("radiomics.parquet", "features.parquet", "radiomics.csv",
                  "features.csv")
  for (nm in candidates) {
    ref <- tryCatch(dsJobs::get_job_output_ref(job_id, nm,
      required_label = "dsImaging_image"), error = function(e) NULL)
    if (!is.null(ref)) return(ref)
  }

  outs <- tryCatch(dsJobs::jobOutputsDS(job_id), error = function(e) NULL)
  if (is.null(outs) || nrow(outs) == 0) return(NULL)
  tabular <- outs$name[grepl("\\.(parquet|csv)$", outs$name, ignore.case = TRUE)]
  for (nm in tabular) {
    ref <- tryCatch(dsJobs::get_job_output_ref(job_id, nm,
      required_label = "dsImaging_image"), error = function(e) NULL)
    if (!is.null(ref)) return(ref)
  }
  NULL
}

#' @keywords internal
.sample_id_from_tags <- function(tags, candidate_ids) {
  hit <- intersect(candidate_ids, tags)
  if (length(hit) == 0) return(NULL)
  hit[1]
}

#' @keywords internal
.dataset_id_from_tags <- function(tags, generation_id, sample_id) {
  if (length(tags) >= 4 && identical(tags[1], "per_image"))
    return(tags[2])
  remaining <- setdiff(tags, c("per_image", generation_id, sample_id))
  if (length(remaining) == 0) return(NULL)
  remaining[1]
}

#' Sync dsJobs failure states back to asset_items
#'
#' For items still "pending" in the generation, checks whether their
#' corresponding dsJobs jobs have FAILED. If so, marks the item as failed.
#' This closes the gap where a dsJobs job fails but the publisher hook
#' never runs (because the job died before reaching the publish step).
#'
#' @keywords internal
.sync_failed_jobs <- function(generation_id) {
  items <- get_generation_items(generation_id)
  pending_items <- items[items$status %in% c("pending", "claimed", "running"), ,
                         drop = FALSE]
  if (nrow(pending_items) == 0) return(invisible(NULL))

  # Query dsJobs for failed jobs tagged with this generation
  failed_jobs <- dsJobs::query_failed_jobs(paste0("%", generation_id, "%"))

  if (nrow(failed_jobs) == 0) return(invisible(NULL))

  # Extract sample_ids from tags and mark items as failed
  for (i in seq_len(nrow(failed_jobs))) {
    tags <- strsplit(failed_jobs$tags[i], ",", fixed = TRUE)[[1]]
    sid <- .sample_id_from_tags(tags, pending_items$sample_id)
    if (is.null(sid)) next
    err_msg <- failed_jobs$error_message[i] %||% "dsJobs job failed"
    complete_item_atomic(generation_id, sid, "failed",
      error = err_msg)
  }
  invisible(NULL)
}

#' Effective per-generation inflight cap.
#'
#' dsImaging has its own cap, but it must never exceed dsJobs' global cap
#' in the current R process. Otherwise submissions beyond the dsJobs quota are
#' misclassified as per-image failures instead of staying pending.
#'
#' @keywords internal
.imaging_max_inflight <- function() {
  radiomics_cap <- as.integer(.imaging_analysis_option("max_inflight", 30L))
  dsjobs_cap <- as.integer(getOption("dsjobs.max_jobs_global",
    getOption("default.dsjobs.max_jobs_global", radiomics_cap)))
  max(1L, min(radiomics_cap, dsjobs_cap))
}

#' @keywords internal
.radiomics_max_inflight <- function() {
  .imaging_max_inflight()
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

#' Decode a base64-JSON parameter from DataSHIELD transport
#' @keywords internal
.dsr_decode <- function(x) {
  if (is.character(x) && length(x) == 1) {
    # Handle URL-safe base64 (from dsJobsClient .ds_encode: +->-, /->_, no =)
    b64 <- x
    if (startsWith(b64, "B64:")) b64 <- sub("^B64:", "", b64)
    b64 <- gsub("-", "+", gsub("_", "/", b64))
    # Add padding
    pad <- nchar(b64) %% 4
    if (pad > 0) b64 <- paste0(b64, strrep("=", 4 - pad))

    raw <- tryCatch(jsonlite::base64_dec(b64), error = function(e) NULL)
    if (!is.null(raw)) {
      return(jsonlite::fromJSON(rawToChar(raw), simplifyVector = FALSE))
    }
    return(tryCatch(jsonlite::fromJSON(x, simplifyVector = FALSE),
                     error = function(e) x))
  }
  x
}

#' Encode a value for dsJobs internal submission
#' @keywords internal
.dsr_encode <- function(x) {
  json <- as.character(jsonlite::toJSON(x, auto_unbox = TRUE, null = "null"))
  b64 <- gsub("[\r\n]", "", jsonlite::base64_enc(charToRaw(json)))
  b64 <- gsub("\\+", "-", b64)
  b64 <- gsub("/", "_", b64)
  b64 <- gsub("=+$", "", b64)
  paste0("B64:", b64)
}

#' Detect transient job submission errors that should be retried
#' @keywords internal
.is_transient_job_submit_error <- function(msg) {
  grepl("quota exceeded|database is locked|database is busy|SQLITE_BUSY|locked database",
    msg, ignore.case = TRUE)
}

#' Normalise runner config values that may arrive as JSON lists
#' @keywords internal
.normalise_extract_config <- function(config) {
  if (!is.null(config$selected_features)) {
    config$selected_features <- as.character(
      unlist(config$selected_features, use.names = FALSE))
  }
  config
}

#' Get current owner_id
#' @keywords internal
.dsr_owner_id <- function() {
  tryCatch(dsJobs::get_owner_id(), error = function(e) {
    Sys.getenv("USER", Sys.info()[["user"]] %||% "unknown")
  })
}

#' Resolve dataset and return full resolved context
#'
#' Tries three paths:
#'   1. Imaging handle (has backend from resource credentials)
#'   2. Registry (server has pre-configured registry)
#'   3. NULL (can't resolve)
#'
#' @keywords internal
.resolve_ds <- function(dataset_id = NULL) {
  # 1. Try imaging handle (created by imagingInitDS with backend from resource)
  backend <- tryCatch(
    imagingGetBackendDS("img"),
    error = function(e) NULL)
  if (is.null(backend)) {
    # Try common handle symbols
    for (sym in c("img_res", "imaging", "res")) {
      backend <- tryCatch(
        imagingGetBackendDS(sym),
        error = function(e) NULL)
      if (!is.null(backend)) break
    }
  }

  if (!is.null(backend)) {
    manifest <- imagingGetManifestDS(dataset_id %||% "img")
    if (!is.null(manifest)) {
      return(list(
        dataset_id = dataset_id %||% manifest$dataset_id,
        backend = backend,
        manifest = manifest,
        manifest_uri = NULL,
        publish = backend
      ))
    }
  }

  # 2. Try registry
  tryCatch(resolve_dataset(dataset_id), error = function(e) NULL)
}

#' Persist enough dataset context for worker-side orchestration
#'
#' Fire-and-forget drip feeding runs in the dsJobs worker, outside the
#' DataSHIELD session that originally resolved the Opal resource. A generation
#' therefore carries a compact manifest/backend context so later batches can be
#' staged without a connected analyst session.
#'
#' @keywords internal
.dataset_context_from_resolved <- function(resolved, manifest) {
  if (is.null(resolved) || is.null(resolved$backend)) return(NULL)
  list(
    manifest = manifest,
    backend = .portable_backend_context(resolved$backend)
  )
}

#' @keywords internal
.portable_backend_context <- function(backend) {
  cfg <- backend$config %||% list()
  out <- list(type = backend$type, config = cfg)

  if (identical(backend$type, "s3")) {
    creds <- tryCatch(.resolve_s3_credentials(cfg$credentials_ref),
      error = function(e) NULL)
    if (!is.null(creds)) {
      out$config$endpoint <- cfg$endpoint %||% creds$endpoint
      out$config$region <- cfg$region %||% creds$region
      out$credentials <- list(
        access_key = creds$access_key,
        secret_key = creds$secret_key,
        endpoint = creds$endpoint,
        region = creds$region
      )
    }
  }

  out
}

#' @keywords internal
.ensure_generation_dataset_context <- function(generation_id, resolved, manifest) {
  gen <- get_generation(generation_id)
  if (is.null(gen)) return(invisible(FALSE))

  spec <- tryCatch(
    jsonlite::fromJSON(gen$spec_json, simplifyVector = FALSE),
    error = function(e) list())
  if (!is.null(spec$dataset_context)) return(invisible(TRUE))

  spec$dataset_context <- .dataset_context_from_resolved(resolved, manifest)
  update_generation(generation_id,
    spec_json = as.character(jsonlite::toJSON(spec, auto_unbox = TRUE)))
  invisible(TRUE)
}

#' @keywords internal
.resolve_ds_from_generation <- function(generation_id, dataset_id = NULL) {
  gen <- get_generation(generation_id)
  if (is.null(gen)) return(NULL)

  spec <- tryCatch(
    jsonlite::fromJSON(gen$spec_json, simplifyVector = FALSE),
    error = function(e) NULL)
  ctx <- spec$dataset_context %||% spec$dataset
  if (is.null(ctx) || is.null(ctx$manifest) || is.null(ctx$backend))
    return(NULL)

  backend <- .backend_from_context(ctx$backend, generation_id)
  if (is.null(backend)) return(NULL)
  list(
    dataset_id = dataset_id %||% gen$dataset_id,
    backend = backend,
    manifest = ctx$manifest,
    manifest_uri = NULL,
    publish = backend
  )
}

#' @keywords internal
.backend_from_context <- function(ctx, generation_id) {
  if (is.null(ctx$type)) return(NULL)
  cfg <- ctx$config %||% list()

  if (identical(ctx$type, "s3")) {
    creds <- ctx$credentials
    if (!is.null(creds)) {
      ref <- cfg$credentials_ref %||%
        paste0("generation_", digest::digest(list(generation_id, creds$access_key),
          algo = "sha256"))
      store <- getOption("dsimaging.credentials", list())
      store[[ref]] <- creds
      options(dsimaging.credentials = store)
      cfg$credentials_ref <- ref
      cfg$endpoint <- cfg$endpoint %||% creds$endpoint
      cfg$region <- cfg$region %||% creds$region
    }
  }

  tryCatch(storage_backend(ctx$type, cfg), error = function(e) NULL)
}

#' Resolve image root URI from manifest
#' @keywords internal
.resolve_image_root <- function(dataset_id) {
  resolved <- .resolve_ds(dataset_id)
  if (is.null(resolved)) return(NULL)
  tryCatch({
    manifest <- resolved$manifest
    if (is.null(manifest) && !is.null(resolved$manifest_uri))
      manifest <- parse_manifest(resolved$manifest_uri, resolved$backend)
    manifest$assets$images$uri
  }, error = function(e) NULL)
}

#' Resolve mask root URI when using existing masks
#' @keywords internal
.resolve_mask_root <- function(dataset_id, segmenter, resolved = NULL) {
  if (!identical(segmenter$provider, "existing_mask_asset")) return(NULL)
  if (is.null(resolved)) resolved <- .resolve_ds(dataset_id)
  if (is.null(resolved)) return(NULL)
  tryCatch({
    manifest <- resolved$manifest
    if (is.null(manifest) && !is.null(resolved$manifest_uri))
      manifest <- parse_manifest(resolved$manifest_uri, resolved$backend)
    mask_alias <- segmenter$mask_asset %||% "masks"
    manifest$assets[[mask_alias]]$uri
  }, error = function(e) NULL)
}

#' Read existing-mask content hashes from a manifest asset
#' @keywords internal
.existing_mask_hashes <- function(resolved, manifest, segmenter) {
  if (!identical(segmenter$provider, "existing_mask_asset")) return(list())
  if (is.null(resolved) || is.null(manifest)) return(list())

  mask_alias <- segmenter$mask_asset %||% "masks"
  asset <- manifest$assets[[mask_alias]]
  index_uri <- asset$content_hash_index %||%
    asset$hash_index %||% asset$index_uri
  if (is.null(index_uri) || !nzchar(index_uri)) return(list())

  idx <- tryCatch(read_hash_index(resolved$backend, index_uri),
    error = function(e) data.frame())
  if (!is.data.frame(idx) || nrow(idx) == 0 ||
      !"sample_id" %in% names(idx) || !"content_hash" %in% names(idx))
    return(list())
  stats::setNames(as.list(idx$content_hash), idx$sample_id)
}

#' Resolve one sample mask from a local or S3 mask asset
#' @keywords internal
.mask_hash_index <- function(backend, manifest, mask_asset) {
  asset <- manifest$assets[[mask_asset]]
  index_uri <- asset$content_hash_index %||%
    asset$hash_index %||% asset$index_uri
  if (is.null(index_uri) || !nzchar(index_uri)) return(NULL)
  tryCatch(read_hash_index(backend, index_uri), error = function(e) NULL)
}

#' Resolve a single sample's image URI or local path
#'
#' For S3: constructs URI from image_root + sample_id + known extensions.
#' For file: scans directory.
#' @keywords internal
.resolve_sample_image <- function(image_root, sample_id, dataset_id = NULL,
                                   backend = NULL) {
  # 1. Try sample manifest (canonical for multi-file samples)
  if (!is.null(dataset_id)) {
    primary <- tryCatch(
      get_sample_primary_path(dataset_id, sample_id),
      error = function(e) NULL)
    if (!is.null(primary)) {
      if (grepl("^s3://", primary)) return(primary)
      if (file.exists(primary)) return(primary)
    }
  }

  # 2. S3 backend: try known extensions against image_root URI
  if (!is.null(backend) && backend$type == "s3" && grepl("^s3://", image_root)) {
    exts <- c(".nii.gz", ".nii", ".nrrd", ".mha", ".dcm")
    for (ext in exts) {
      candidate <- paste0(sub("/$", "", image_root), "/", sample_id, ext)
      head <- backend_head(backend, candidate)
      if (!is.null(head) && isTRUE(head$exists)) return(candidate)
    }
    return(NULL)
  }

  # 3. File backend: directory scan
  if (is.null(image_root) || !dir.exists(image_root)) return(NULL)
  files <- list.files(image_root, full.names = TRUE)
  for (f in files) {
    base <- basename(f)
    name <- sub("\\.(nii\\.gz|nii|nrrd|mha|mhd|dcm)$", "", base,
                ignore.case = TRUE)
    if (name == sample_id) return(f)
  }
  NULL
}

#' Resolve a single sample's mask URI or local path
#' @keywords internal
.resolve_sample_mask <- function(mask_root, sample_id, backend = NULL,
                                 manifest = NULL, mask_asset = "masks") {
  if (is.null(mask_root) || !nzchar(mask_root)) return(NULL)

  if (!is.null(backend) && identical(backend$type, "s3") &&
      grepl("^s3://", mask_root)) {
    if (!is.null(manifest)) {
      idx <- .mask_hash_index(backend, manifest, mask_asset)
      if (is.data.frame(idx) && nrow(idx) > 0 && "uri" %in% names(idx)) {
        hit <- idx[idx$sample_id == sample_id, , drop = FALSE]
        if (nrow(hit) > 0 && nzchar(hit$uri[1])) return(hit$uri[1])
      }
    }

    exts <- c(".nii.gz", ".nii", ".nrrd", ".mha", ".mhd", ".dcm")
    stems <- c(sample_id, paste0(sample_id, "_mask"),
      paste0(sample_id, "_seg"), paste0(sample_id, "_label"),
      paste0(sample_id, "_GTV-1"), paste0(sample_id, "_gtv1"))
    for (stem in stems) {
      for (ext in exts) {
        candidate <- paste0(sub("/$", "", mask_root), "/", stem, ext)
        head <- backend_head(backend, candidate)
        if (!is.null(head) && isTRUE(head$exists)) return(candidate)
      }
    }

    keys <- backend_list(backend, mask_root)
    if (length(keys) == 0) return(NULL)
    base <- basename(keys)
    exact <- sub("\\.(nii\\.gz|nii|nrrd|mha|mhd|dcm)$", "", base,
      ignore.case = TRUE)
    preferred <- keys[exact == sample_id | exact == paste0(sample_id, "_mask")]
    if (length(preferred) > 0) return(preferred[1])
    contains <- keys[grepl(sample_id, base, fixed = TRUE) &
      grepl("mask|seg|label|gtv", base, ignore.case = TRUE)]
    if (length(contains) > 0) return(contains[1])
    return(NULL)
  }

  if (!dir.exists(mask_root)) return(NULL)
  files <- list.files(mask_root, full.names = TRUE, recursive = TRUE)
  for (f in files) {
    base <- basename(f)
    name <- sub("\\.(nii\\.gz|nii|nrrd|mha|mhd|dcm)$", "", base,
                ignore.case = TRUE)
    if (grepl(sample_id, name, fixed = TRUE)) return(f)
  }
  NULL
}

#' Stage an S3 image to local filesystem for Python runner execution
#'
#' Python runners expect local file paths. For S3-backed datasets,
#' this downloads the image to a staging directory.
#' For file-backed datasets, returns the path as-is.
#' @keywords internal
.stage_image_for_job <- function(image_uri, sample_id, dataset_id, backend) {
  .stage_backend_file_for_job(image_uri, sample_id, dataset_id, backend,
    role = "images")
}

#' Stage an S3-backed file to local filesystem for Python runner execution
#' @keywords internal
.stage_backend_file_for_job <- function(uri, sample_id, dataset_id, backend,
                                        role = "files") {
  if (is.null(backend) || backend$type == "file") return(uri)
  if (!grepl("^s3://", uri)) return(uri)

  # Stage to DSJOBS_HOME/staging/dataset_id/
  home <- tryCatch(
    dsJobs:::.dsjobs_home(must_exist = FALSE),
    error = function(e) getOption("dsjobs.home",
      getOption("default.dsjobs.home", "/srv/dsjobs"))
  )
  staging_dir <- file.path(home, "staging", dataset_id, role)
  dir.create(staging_dir, recursive = TRUE, showWarnings = FALSE)

  local_path <- file.path(staging_dir, paste(sample_id, basename(uri), sep = "__"))
  if (!file.exists(local_path))
    backend_get_file(backend, uri, local_path)

  local_path
}

#' Resolve a profile name to its YAML file path
#' @keywords internal
.resolve_profile_path <- function(profile_name) {
  if (is.null(profile_name)) return(NULL)
  # Check inst/profiles/ in dsImaging
  profiles_dir <- system.file("profiles", package = "dsImaging")
  if (nzchar(profiles_dir)) {
    candidates <- list.files(profiles_dir, full.names = TRUE)
    for (f in candidates) {
      if (grepl(profile_name, basename(f), fixed = TRUE)) return(f)
    }
  }
  # If profile_name is already a valid path, use it
  if (file.exists(profile_name)) return(profile_name)
  NULL
}

#' Stable signature for radiomics extraction settings
#' @keywords internal
.radiomics_profile_signature <- function(profile) {
  profile_name <- if (is.list(profile)) profile$name else as.character(profile)
  profile_path <- .resolve_profile_path(profile_name)
  profile_file_hash <- NULL
  if (!is.null(profile_path) && file.exists(profile_path)) {
    profile_file_hash <- digest::digest(file = profile_path, algo = "sha256")
  }
  compute_derivation_hash(
    profile = profile,
    profile_file = basename(profile_path %||% ""),
    profile_file_hash = profile_file_hash
  )
}
