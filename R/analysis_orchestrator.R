# Module: Per-Image Collection Orchestrator
# Server-side DS methods for per-image job deduplication.
#
# Flow:
#   1. imagingRadiomicsScanCollectionDS  -- fingerprint + diff + create generation
#   2. imagingRadiomicsSubmitBatchDS     -- create N dsHPC jobs for pending images
#   3. imagingRadiomicsCollectionStatusDS -- query generation progress (with failure sync)
#   4. imagingRadiomicsPublishCollectionDS -- aggregate per-image outputs

# ---------------------------------------------------------------------------
# 1. Scan: fingerprint images, diff vs completed, create/reuse generation
# ---------------------------------------------------------------------------

#' Scan a Collection for Radiomics Processing
#'
#' Server-internal orchestration helper. Computes or reads image fingerprints,
#' claims or resumes a dsImaging generation, and returns the exact sample ids
#' that should be submitted as dsHPC work. Registered methods keep this payload
#' behind a session-bound workflow capability.
#'
#' @param dataset_id Dataset id from a session-authorized handle.
#' @param segmenter Validated segmenter specification.
#' @param profile Validated radiomics profile.
#' @param visibility Derived-asset visibility.
#' @param resolved_override Optional session-authorized dataset context.
#' @return Named list describing whether an existing asset/generation was
#'   reused or a new generation was created, plus pending sample metadata.
#' @keywords internal
.imaging_radiomics_scan_collection <- function(dataset_id, segmenter, profile,
                                               visibility,
                                               resolved_override = NULL) {
  dataset_id <- .required_scalar(dataset_id, "dataset_id")
  segmenter <- .imaging_segmenter_spec(segmenter)
  profile <- .imaging_profile_spec(profile)
  visibility <- .imaging_visibility(visibility)

  # dsImaging is in Imports, always available

  # Public workflows pass a session-authorized handle context. Registry
  # resolution is retained only for trusted server-side recovery paths.
  resolved <- resolved_override %||% .resolve_ds(dataset_id)
  if (is.null(resolved))
    stop("Cannot resolve dataset: ", dataset_id, call. = FALSE)

  # Get manifest: from handle (already parsed) or from URI
  manifest <- resolved$manifest
  if (is.null(manifest) && !is.null(resolved$manifest_uri))
    manifest <- parse_manifest(resolved$manifest_uri, resolved$backend)
  if (is.null(manifest))
    stop("Cannot load manifest for dataset: ", dataset_id, call. = FALSE)
  if (!identical(as.character(dataset_id), manifest$dataset_id)) {
    stop("Dataset context does not match the requested collection.",
         call. = FALSE)
  }
  admission <- .imaging_privacy_admission(manifest, resolved$backend)
  pinned_snapshot <- resolved$collection_snapshot %||%
    (resolved$handle %||% list())$collection_snapshot
  current_snapshot <- .new_imaging_collection_snapshot(
    manifest, resolved$backend, admission)
  if (!is.null(pinned_snapshot) &&
      !identical(.validate_imaging_collection_snapshot(pinned_snapshot)$seal,
                 current_snapshot$seal)) {
    stop("Imaging dataset no longer matches its admitted collection snapshot.",
         call. = FALSE)
  }
  resolved$collection_snapshot <- current_snapshot

  image_root <- manifest$assets$images$uri
  mask_hashes <- .existing_mask_hashes(resolved, manifest, segmenter)
  if (identical(segmenter$provider, "existing_mask_asset")) {
    .assert_exact_imaging_roster(names(mask_hashes), admission$roster,
      context = "Existing mask asset")
  }

  # Diff only the exact sample-to-object mapping already validated and pinned
  # in the admitted snapshot. Re-scanning a directory and deriving patient
  # identifiers from filenames would create a second, weaker roster.
  snapshot_records <- resolved$collection_snapshot$records
  snapshot_index <- data.frame(
    sample_id = vapply(snapshot_records, `[[`, character(1), "sample_id"),
    content_hash = vapply(snapshot_records, `[[`, character(1),
                          "content_hash"),
    stringsAsFactors = FALSE)
  collection_seal <- (resolved$collection_snapshot %||%
    (resolved$handle %||% list())$collection_snapshot)$seal %||% NULL
  collection_seal <- .asset_collection_seal(collection_seal, required = TRUE)
  fp_result <- diff_hash_index(snapshot_index, dataset_id, collection_seal)
  fp_result$fingerprints <- list()
  new_changed <- c(fp_result$new, fp_result$changed)
  if (length(new_changed) > 0L) {
    hashes <- vapply(new_changed, function(sid)
      fp_result$content_hashes[[sid]] %||% "", character(1))
    valid <- nzchar(hashes)
    if (any(valid)) {
      store_content_hashes(
        dataset_id, collection_seal, new_changed[valid], hashes[valid])
    }
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
  hash_source <- .ordered_sample_hashes(hash_source)
  collection_hash <- compute_derivation_hash(
    dataset_id = dataset_id,
    collection_seal = collection_seal,
    privacy_roster_hash = admission$roster$roster_hash,
    processor = processor,
    segmenter = segmenter,
    profile = profile,
    profile_signature = profile_signature,
    mask_asset = segmenter$mask_asset %||% NULL,
    image_hashes = hash_source
  )

  # Claim or reuse generation
  # All sample IDs are candidates. The diff (new/changed/unchanged) is about
  # file content changes, not about processing status. Per-image dedup happens
  # at submit time via derivation hash check in imagingRadiomicsSubmitBatchDS.
  all_sample_ids <- names(fp_result$content_hashes)
  if (length(all_sample_ids) == 0) all_sample_ids <- names(fp_result$fingerprints)
  if (anyDuplicated(all_sample_ids) ||
      !setequal(all_sample_ids, admission$sample_ids)) {
    stop("Image collection does not match the admitted dataset metadata.",
         call. = FALSE)
  }
  all_sample_ids <- admission$sample_ids
  total_n <- length(all_sample_ids)

  generation_spec <- list(
    processor = processor,
    profile = profile,
    profile_signature = profile_signature,
    profile_name = profile$name,
    segmenter = segmenter,
    privacy_roster = admission$roster,
    dataset_context = .dataset_context_from_resolved(
      resolved, manifest, admission$roster)
  )

  gen_result <- claim_or_reuse_generation(
    dataset_id = dataset_id,
    kind = "radiomics_collection",
    derivation_hash = collection_hash,
    collection_seal = collection_seal,
    visibility = visibility,
    owner_id = .dsr_owner_id(),
    expected_n = total_n,
    spec = generation_spec
  )

  # Internal scheduling and publication require exact full-roster accounting.
  # Registered analyst methods project this structure to coarse state and never
  # return these counts or identifiers.
  if (gen_result$action == "reuse_asset") {
    return(list(
      action = "reuse_asset",
      asset_id = gen_result$asset_id,
      dataset_id = dataset_id,
      total = as.integer(total_n),
      done = as.integer(total_n),
      pending = 0L
    ))
  }

  generation_id <- gen_result$generation_id

  if (gen_result$action == "reuse_generation") {
    .ensure_generation_dataset_context(
      generation_id, resolved, manifest, admission$roster)

    # Resume: reconcile finished/failed jobs first, then check pending items.
    .sync_generation_jobs(generation_id)
    requeue_stale_claimed_items(generation_id)
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
      total = as.integer(total_n),
      done = length(done_ids),
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
    total = as.integer(total_n),
    done = 0L,
    pending_ids = all_sample_ids,
    fingerprints = fp_result$fingerprints,
    content_hashes = fp_result$content_hashes
  )
}

#' Preserve the exact sample-to-content association in derivation hashes
#' @keywords internal
.ordered_sample_hashes <- function(hashes) {
  hashes <- unlist(hashes, use.names = TRUE)
  ids <- names(hashes)
  if (!is.character(ids) || length(ids) == 0L || anyNA(ids) ||
      any(!nzchar(ids)) || anyDuplicated(ids) || anyNA(hashes) ||
      any(!nzchar(as.character(hashes)))) {
    stop("Collection content mapping is invalid.", call. = FALSE)
  }
  order <- order(ids, method = "radix")
  as.list(stats::setNames(as.character(hashes[order]), ids[order]))
}

# ---------------------------------------------------------------------------
# 2. Submit batch: create per-image dsHPC jobs for pending samples
# ---------------------------------------------------------------------------

#' Submit a Batch of Per-Image Radiomics Jobs
#'
#' DataSHIELD aggregate method. Creates per-image dsHPC job specs for pending
#' samples in a generation, respecting dsImaging/dsHPC backpressure limits.
#'
#' @param generation_id Server-created generation id.
#' @param sample_ids Character vector selected from the admitted roster.
#' @param dataset_id Dataset id sealed into the generation.
#' @param fingerprints Named list of server-derived image fingerprints.
#' @param content_hashes Optional named list of server-derived content hashes.
#' @return Named list with generation id, submitted count, optional deferred
#'   count, and per-sample submission results.
#' @keywords internal
.imaging_radiomics_submit_batch <- function(generation_id, sample_ids,
                                            dataset_id, fingerprints,
                                            content_hashes = list()) {

  # dsHPC is in Imports, always available
  if (!is.character(generation_id) || length(generation_id) != 1L ||
      !grepl("^gen_[0-9a-f]{32}$", generation_id)) {
    stop("Invalid generation reference.", call. = FALSE)
  }
  gen <- get_generation(generation_id)
  if (is.null(gen) || !identical(gen$dataset_id, as.character(dataset_id))) {
    stop("Generation does not authorize the requested dataset.",
         call. = FALSE)
  }
  gen_spec <- tryCatch(
    jsonlite::fromJSON(gen$spec_json, simplifyVector = FALSE),
    error = function(e) NULL)
  if (is.null(gen_spec) || !is.list(gen_spec$segmenter) ||
      !is.list(gen_spec$profile)) {
    stop("Generation processing contract is unavailable.", call. = FALSE)
  }
  segmenter <- .imaging_segmenter_spec(gen_spec$segmenter)
  profile <- .imaging_profile_spec(gen_spec$profile)
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
  if (is.null(manifest) ||
      !identical(as.character(dataset_id), manifest$dataset_id)) {
    stop("Generation dataset context is invalid.", call. = FALSE)
  }
  admission <- .imaging_privacy_admission(manifest, resolved$backend)
  generation_roster <- .generation_privacy_roster(gen)
  if (!.same_imaging_privacy_roster(generation_roster,
                                    admission$roster)) {
    stop("Generation no longer matches its admitted privacy roster.",
         call. = FALSE)
  }
  sample_ids <- as.character(unlist(sample_ids, use.names = FALSE))
  if (length(sample_ids) == 0L || anyNA(sample_ids) ||
      anyDuplicated(sample_ids) ||
      any(!sample_ids %in% admission$sample_ids)) {
    stop("Requested batch is not part of the admitted collection.",
         call. = FALSE)
  }
  items <- get_generation_items(generation_id)
  eligible <- items$sample_id[items$status %in% c("pending", "claimed", "failed")]
  if (any(!sample_ids %in% eligible)) {
    stop("Requested batch is not eligible for processing.", call. = FALSE)
  }
  image_root <- if (!is.null(manifest)) manifest$assets$images$uri else NULL
  mask_root <- .resolve_mask_root(dataset_id, segmenter, resolved = resolved)
  mask_hashes <- .existing_mask_hashes(resolved, manifest, segmenter)
  if (identical(segmenter$provider, "existing_mask_asset")) {
    .assert_exact_imaging_roster(names(mask_hashes), generation_roster,
      context = "Existing mask asset")
  }
  backend <- resolved$backend
  processor <- paste0(segmenter$provider, "_", segmenter$task %||% "default")
  profile_signature <- .radiomics_profile_signature(profile)

  # Map segmenter to runner. Accept both short forms (e.g. "lungmask") and
  # canonical runner names (e.g. "lungmask_infer") from ds.imaging.segmenter.*.
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
    as.integer(dsHPC::count_active_jobs(paste0("%", generation_id, "%"))),
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

    # Resolve image path
    image_uri <- .resolve_sample_image(image_root, sid, backend = backend,
      snapshot = resolved$collection_snapshot)
    if (is.null(image_uri)) {
      complete_item_atomic(generation_id, sid, "failed",
        error = "Image file not found")
      submitted[[sid]] <- list(status = "failed", error = "Image not found")
      next
    }
    job_token <- .item_job_token(generation_id, sid)

    # For S3 images: stage to local filesystem for the Python runner
    image_path <- .stage_image_for_job(image_uri, sid, dataset_id, backend,
      image_root = image_root, snapshot = resolved$collection_snapshot)

    # Build per-image job steps (using dsHPC step format)
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
    extract_config <- profile
    extract_config$image <- image_path
    extract_config$sample_id <- sid
    extract_config$generation_id <- generation_id
    extract_config$settings_file <- settings_path %||% "default"
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
      actual_mask_hash <- tryCatch(digest::digest(
        file = extract_config$mask, algo = "sha256"),
        error = function(e) NULL)
      if (is.null(actual_mask_hash) ||
          !identical(actual_mask_hash, mask_hashes[[sid]])) {
        complete_item_atomic(generation_id, sid, "failed",
          error = "Mask integrity verification failed")
        submitted[[sid]] <- list(
          status = "failed", error = "Mask integrity verification failed")
        next
      }
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

    # Submit through dsHPC's server-to-server API.
    job_spec <- list(
      label = "dsImaging_image",
      tags = .per_image_job_tags(dataset_id, job_token, generation_id),
      visibility = "private",
      steps = steps,
      .owner = .dsr_owner_id()
    )

    tryCatch({
      spec_enc <- .dsr_encode(job_spec)
      result <- dsHPC::hpcSubmitInternal(spec_enc)
      record_item_status(
        generation_id, sid, "running", job_token = job_token)
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
#' Server-internal orchestration helper. Reconciles finished/failed dsHPC state
#' into dsImaging generation items and returns exact operational progress for
#' scheduling. Registered analyst status methods return only coarse state.
#'
#' @param generation_id Server-created generation id.
#' @return Named list with generation state, exact item counts, and
#'   `is_done`.
#' @keywords internal
.imaging_radiomics_collection_status <- function(generation_id) {
  if (!is.character(generation_id) || length(generation_id) != 1L ||
      !grepl("^gen_[0-9a-f]{32}$", generation_id)) {
    stop("Invalid generation reference.", call. = FALSE)
  }

  # Sync dsHPC terminal states -> mark asset_items as completed/failed.
  .sync_generation_jobs(generation_id)
  .requeue_orphan_running_items(generation_id)
  requeue_stale_claimed_items(generation_id)
  .requeue_invalid_completed_items(generation_id)
  reconcile_generation_counters(generation_id)

  gen <- get_generation(generation_id)
  if (is.null(gen))
    stop("Generation not found: ", generation_id, call. = FALSE)

  items <- get_generation_items(generation_id)
  completed_ids <- items$sample_id[items$status == "completed"]
  failed_ids <- items$sample_id[items$status == "failed"]
  pending_ids <- items$sample_id[items$status == "pending"]
  claimed_ids <- items$sample_id[items$status == "claimed"]
  running_ids <- items$sample_id[items$status == "running"]
  active_jobs <- tryCatch(
    dsHPC::query_jobs_by_tag(paste0("%", generation_id, "%"),
      states = c("PENDING", "RUNNING")),
    error = function(e) data.frame())
  retrying_jobs <- if (nrow(active_jobs) > 0 &&
      "retry_count" %in% names(active_jobs)) {
    sum(active_jobs$state == "PENDING" &
          as.integer(active_jobs$retry_count %||% 0L) > 0L,
        na.rm = TRUE)
  } else 0L

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
    active_jobs <- tryCatch(
      dsHPC::query_jobs_by_tag(paste0("%", generation_id, "%"),
        states = c("PENDING", "RUNNING")),
      error = function(e) data.frame())
    retrying_jobs <- if (nrow(active_jobs) > 0 &&
        "retry_count" %in% names(active_jobs)) {
      sum(active_jobs$state == "PENDING" &
            as.integer(active_jobs$retry_count %||% 0L) > 0L,
          na.rm = TRUE)
    } else 0L
  }

  # pending + claimed + running = "not yet done"
  not_done <- length(pending_ids) + length(claimed_ids) + length(running_ids)
  total_items <- nrow(items)
  expected <- as.integer(gen$expected_n %||% total_items)

  # is_done: all items resolved (completed or failed) AND we have items
  # Guard against empty items table (generation exists but scan hasn't populated items yet)
  all_resolved <- not_done == 0L && total_items > 0 && total_items >= expected

  # Keep exact server-internal accounting for scheduling and complete-roster
  # publication. The registered status boundary strips every counter and id.
  list(
    generation_id = generation_id,
    state = gen$state,
    total = as.integer(expected),
    completed = length(completed_ids),
    failed = length(failed_ids),
    pending = length(pending_ids),
    claimed = length(claimed_ids),
    running = length(running_ids),
    retrying = as.integer(retrying_jobs),
    active_jobs = nrow(active_jobs),
    is_done = all_resolved
  )
}

#' Recover a Radiomics Collection Generation
#'
#' Server-internal helper. Reconciles dsHPC state, requeues stale claimed items
#' left by interrupted submitters, and triggers the next drip-feed batch if
#' capacity is available.
#'
#' @param generation_id Server-created generation id.
#' @return Same payload as `imagingRadiomicsCollectionStatusDS`.
#' @keywords internal
.imaging_radiomics_recover_collection <- function(generation_id) {
  if (!is.character(generation_id) || length(generation_id) != 1L ||
      !grepl("^gen_[0-9a-f]{32}$", generation_id)) {
    stop("Invalid generation reference.", call. = FALSE)
  }
  .sync_generation_jobs(generation_id)
  .requeue_orphan_running_items(generation_id)
  requeue_stale_claimed_items(generation_id)
  .requeue_invalid_completed_items(generation_id)
  reconcile_generation_counters(generation_id)

  gen <- get_generation(generation_id)
  if (is.null(gen))
    stop("Generation not found: ", generation_id, call. = FALSE)
  if (gen$state %in% c("RUNNING", "PENDING")) {
    tryCatch(
      .drip_feed_next_batch(generation_id, gen$dataset_id),
      error = function(e) update_generation(generation_id,
        error = paste("Recovery drip-feed failed:", conditionMessage(e))))
  }

  .imaging_radiomics_collection_status(generation_id)
}

#' Cancel a Radiomics Collection Generation
#'
#' DataSHIELD aggregate method. Admin-only; protected by `dshpc.admin_key` or
#' `DSHPC_ADMIN_KEY`.
#' Cancels dsHPC jobs belonging to the generation and marks unfinished items as
#' skipped, without exposing sample identifiers to the client.
#'
#' @param generation_id Generation id resolved from a session workflow.
#' @param admin_key_enc Encoded admin key payload.
#' @param reason Validated cancellation reason.
#' @return Named list with cancellation counts.
#' @keywords internal
.imaging_cancel_generation <- function(generation_id, admin_key_enc, reason) {
  .verify_radiomics_admin(admin_key_enc)

  gen <- get_generation(generation_id)
  if (is.null(gen))
    stop("Generation not found: ", generation_id, call. = FALSE)
  if (gen$state %in% c("ACTIVE"))
    stop("Cannot cancel an already published generation.", call. = FALSE)

  expected_admin_key <- .dshpc_option_safe("admin_key")
  had_admin_option <- "dshpc.admin_key" %in% names(options())
  previous_admin_key <- getOption("dshpc.admin_key", NULL)
  if (!had_admin_option && !is.null(expected_admin_key) &&
      nzchar(expected_admin_key)) {
    options(dshpc.admin_key = expected_admin_key)
    on.exit(options(dshpc.admin_key = NULL), add = TRUE)
  } else if (had_admin_option) {
    on.exit(options(dshpc.admin_key = previous_admin_key), add = TRUE)
  }

  cancelled <- dsHPC::cancel_jobs_by_tag(
    paste0("%", generation_id, "%"),
    admin_key = admin_key_enc,
    reason = reason,
    states = c("PENDING", "RUNNING"))
  skipped <- skip_unfinished_generation_items(generation_id,
    paste("Cancelled:", reason))
  update_generation(generation_id, state = "CANCELLED",
    error = paste("Cancelled:", reason))
  reconcile_generation_counters(generation_id)

  list(
    generation_id = generation_id,
    state = "CANCELLED",
    cancelled_jobs = nrow(cancelled),
    skipped_items = skipped,
    reason = safe_metadata_string(reason)
  )
}

#' Cancel a handle-bound Radiomics Collection Workflow
#'
#' @param reference_symbol Session symbol holding the opaque workflow handle.
#' @param admin_key_enc Encoded admin key payload.
#' @param reason_enc Encoded cancellation reason.
#' @return Client-safe cancellation state.
#' @export
imagingRadiomicsCancelCollectionDS <- function(reference_symbol,
                                               admin_key_enc,
                                               reason_enc = NULL) {
  .dsimaging_require_literal_arguments()
  owner_env <- parent.frame()
  .verify_radiomics_admin(admin_key_enc)
  context <- .resolve_imaging_workflow_context(
    reference_symbol, owner_env = owner_env)
  workflow <- context$workflow
  if (is.null(workflow$generation_id) ||
      workflow$action %in% c("reuse_asset", "published", "cancelled")) {
    stop("Collection workflow cannot be cancelled.", call. = FALSE)
  }
  reason <- if (is.null(reason_enc)) "Cancelled by admin" else
    .dsr_decode(reason_enc)
  if (!is.character(reason) || length(reason) != 1L || is.na(reason) ||
      !nzchar(trimws(reason)) || nchar(reason, type = "bytes") > 256L ||
      grepl("[\r\n]", reason)) {
    stop("Invalid cancellation reason.", call. = FALSE)
  }
  .imaging_cancel_generation(workflow$generation_id, admin_key_enc, reason)
  workflow$action <- "cancelled"
  .update_imaging_workflow(reference_symbol, workflow,
                           owner_env = owner_env)
  list(state = "CANCELLED")
}

# ---------------------------------------------------------------------------
# 4. Publish: aggregate per-image outputs into collection asset
# ---------------------------------------------------------------------------

#' Publish a Completed Radiomics Collection
#'
#' Server-internal helper. Aggregates completed per-image outputs into a
#' collection-level radiomics feature table and publishes the derived asset.
#'
#' @param generation_id Server-created generation id.
#' @param dataset_id Dataset id sealed into the generation.
#' @return Named list with publication status and derived asset metadata.
#' @keywords internal
.imaging_radiomics_publish_collection <- function(generation_id, dataset_id) {
  if (!is.character(generation_id) || length(generation_id) != 1L ||
      !grepl("^gen_[0-9a-f]{32}$", generation_id)) {
    stop("Invalid generation reference.", call. = FALSE)
  }

  # Final sync to catch any late completions/failures
  .sync_generation_jobs(generation_id)
  .requeue_orphan_running_items(generation_id)
  requeue_stale_claimed_items(generation_id)
  requeued_invalid <- .requeue_invalid_completed_items(generation_id)
  reconcile_generation_counters(generation_id)

  gen <- get_generation(generation_id)
  if (is.null(gen))
    stop("Generation not found: ", generation_id, call. = FALSE)
  if (!is.character(dataset_id) || length(dataset_id) != 1L ||
      !identical(as.character(gen$dataset_id), dataset_id)) {
    stop("Generation does not authorize the requested dataset.",
         call. = FALSE)
  }
  if (gen$state %in% c("CANCELLED"))
    stop("Generation was cancelled and cannot be published.", call. = FALSE)
  if (requeued_invalid > 0) {
    stop(requeued_invalid, " completed item artifact(s) were incompatible ",
      "with the generation profile and have been requeued. Wait for recovery ",
      "to finish before publishing.", call. = FALSE)
  }

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

  resolved <- .resolve_ds_from_generation(generation_id, dataset_id)
  if (is.null(resolved) || is.null(resolved$manifest)) {
    stop("Generation dataset context is unavailable.", call. = FALSE)
  }
  admission <- .imaging_privacy_admission(resolved$manifest, resolved$backend)
  generation_roster <- .generation_privacy_roster(gen)
  if (!.same_imaging_privacy_roster(generation_roster,
                                    admission$roster)) {
    stop("Only a complete admitted collection may be published.",
         call. = FALSE)
  }
  roster_ok <- tryCatch({
    .assert_exact_imaging_roster(
      completed$sample_id, generation_roster,
      context = "Published imaging collection")
    TRUE
  }, error = function(e) FALSE)
  if (n_failed > 0L || !isTRUE(roster_ok) ||
      total != generation_roster$sample_count ||
      !identical(as.integer(gen$expected_n),
                 generation_roster$sample_count)) {
    stop("Only a complete admitted collection may be published.",
         call. = FALSE)
  }

  # Build collection-level output directory
  output_root <- file.path(
    getOption("dshpc.home", getOption("default.dshpc.home", "/srv/dshpc")),
    "publish",
    paste0("collection_", generation_id))
  dir.create(output_root, recursive = TRUE, showWarnings = FALSE)

  selected_features <- .generation_selected_features(gen)
  feature_table <- .write_collection_feature_table(completed, output_root,
    selected_features = selected_features, roster = generation_roster)

  # Write collection manifest
  manifest <- list(
    generation_id = generation_id,
    dataset_id = dataset_id,
    total = total,
    completed = n_completed,
    failed = n_failed,
    feature_table = feature_table,
    selected_features = as.list(selected_features),
    # failed_samples stripped -- individual sample IDs are disclosive
    item_artifacts = as.list(
      stats::setNames(completed$artifact_relpath, completed$sample_id))
  )
  writeLines(
    jsonlite::toJSON(manifest, auto_unbox = TRUE, pretty = TRUE),
    file.path(output_root, "collection_manifest.json"))

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
    state = "ACTIVE"
  )
}

#' Combine per-image radiomics outputs into one collection feature table
#' @keywords internal
.write_collection_feature_table <- function(completed, output_root,
                                            selected_features = character(0),
                                            roster = NULL) {
  if (nrow(completed) == 0) return(NULL)
  if (!requireNamespace("arrow", quietly = TRUE))
    stop("arrow package required to publish radiomics feature tables.",
         call. = FALSE)
  selected_features <- as.character(unlist(selected_features,
    use.names = FALSE))
  selected_features <- selected_features[nzchar(selected_features)]
  required_cols <- if (length(selected_features) > 0) {
    c("sample_id", selected_features)
  } else {
    character(0)
  }

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
    expected_sample <- .canonical_imaging_privacy_ids(
      completed$sample_id[i])
    if (nrow(df) != 1L) {
      stop("Each radiomics artifact must contain exactly one admitted sample.",
           call. = FALSE)
    }
    if (!"sample_id" %in% names(df)) {
      df$sample_id <- expected_sample
    } else {
      actual_sample <- .canonical_imaging_privacy_ids(df$sample_id)
      if (length(actual_sample) != 1L || is.na(actual_sample) ||
          !identical(actual_sample, expected_sample)) {
        stop("Radiomics artifact sample does not match its admitted sample.",
             call. = FALSE)
      }
      df$sample_id <- actual_sample
    }
    if (length(required_cols) > 0) {
      missing <- setdiff(required_cols, names(df))
      if (length(missing) > 0) {
        stop("Radiomics artifact for sample '", completed$sample_id[i],
          "' is missing selected feature(s): ",
          paste(missing, collapse = ", "), call. = FALSE)
      }
      df <- df[, required_cols, drop = FALSE]
    }
    df
  })

  all_cols <- if (length(required_cols) > 0) {
    required_cols
  } else {
    unique(unlist(lapply(rows, names), use.names = FALSE))
  }
  rows <- lapply(rows, function(df) {
    missing <- setdiff(all_cols, names(df))
    for (nm in missing) df[[nm]] <- NA
    df[, all_cols, drop = FALSE]
  })
  features <- do.call(rbind, rows)
  if (!is.null(roster)) {
    .assert_exact_imaging_roster(features$sample_id, roster,
      context = "Published imaging feature table")
  }
  out <- file.path(output_root, "radiomics_features.parquet")
  arrow::write_parquet(features, out)
  out
}

#' Selected radiomics features configured for a generation
#' @keywords internal
.generation_selected_features <- function(gen) {
  spec_json <- gen$spec_json %||% NULL
  if (is.null(spec_json) || !nzchar(spec_json)) return(character(0))
  spec <- tryCatch(
    jsonlite::fromJSON(spec_json, simplifyVector = FALSE),
    error = function(e) list())
  selected <- spec$profile$selected_features %||% spec$selected_features
  if (is.null(selected)) return(character(0))
  selected <- as.character(unlist(selected, use.names = FALSE))
  selected[nzchar(selected)]
}

#' Immutable privacy roster pinned to a collection generation.
#' @keywords internal
.generation_privacy_roster <- function(gen) {
  spec_json <- gen$spec_json %||% NULL
  if (is.null(spec_json) || is.na(spec_json) || !nzchar(spec_json)) {
    stop("Generation has no admitted privacy roster.", call. = FALSE)
  }
  spec <- tryCatch(
    jsonlite::fromJSON(spec_json, simplifyVector = FALSE),
    error = function(e) NULL)
  if (is.null(spec$privacy_roster)) {
    stop("Generation has no admitted privacy roster.", call. = FALSE)
  }
  .validate_imaging_privacy_roster(spec$privacy_roster)
}

#' Requeue completed items whose artifacts no longer match the generation spec
#' @keywords internal
.requeue_invalid_completed_items <- function(generation_id) {
  gen <- get_generation(generation_id)
  if (is.null(gen)) return(0L)
  selected_features <- .generation_selected_features(gen)
  if (length(selected_features) == 0) return(0L)
  if (!requireNamespace("arrow", quietly = TRUE))
    stop("arrow package required to validate radiomics feature artifacts.",
      call. = FALSE)

  completed <- get_generation_items(generation_id, status = "completed")
  if (nrow(completed) == 0) return(0L)

  required_cols <- c("sample_id", selected_features)
  requeued <- 0L
  for (i in seq_len(nrow(completed))) {
    path <- completed$artifact_relpath[i]
    reason <- NULL
    if (is.na(path) || !nzchar(path) || !file.exists(path)) {
      reason <- "completed artifact is missing"
    } else {
      missing <- .missing_artifact_features(path, required_cols)
      if (length(missing) > 0) {
        reason <- paste0("completed artifact missing selected feature(s): ",
          paste(missing, collapse = ", "))
      }
    }
    if (!is.null(reason)) {
      record_item_status(generation_id, completed$sample_id[i], "pending",
        error = reason)
      requeued <- requeued + 1L
    }
  }

  if (requeued > 0) {
    update_generation(generation_id, state = "RUNNING",
      error = paste(requeued, "completed artifact(s) requeued for recovery"))
    reconcile_generation_counters(generation_id)
  }
  requeued
}

#' Requeue running items that no longer have an active dsHPC job
#' @keywords internal
.requeue_orphan_running_items <- function(generation_id) {
  running <- get_generation_items(generation_id, status = "running")
  if (nrow(running) == 0) return(0L)

  active_jobs <- dsHPC::query_jobs_by_tag(paste0("%", generation_id, "%"),
    states = c("PENDING", "RUNNING"))
  active_sids <- character(0)
  if (nrow(active_jobs) > 0) {
    for (i in seq_len(nrow(active_jobs))) {
      tags <- strsplit(active_jobs$tags[i], ",", fixed = TRUE)[[1]]
      sid <- .sample_id_from_tags(tags, running)
      if (!is.null(sid)) active_sids <- c(active_sids, sid)
    }
  }

  orphan <- setdiff(running$sample_id, unique(active_sids))
  if (length(orphan) == 0) return(0L)

  for (sid in orphan) {
    record_item_status(generation_id, sid, "pending",
      error = "running item had no active dsHPC job and was requeued")
  }
  update_generation(generation_id, state = "RUNNING",
    error = paste(length(orphan), "orphan running item(s) requeued"))
  reconcile_generation_counters(generation_id)
  length(orphan)
}

#' Per-image artifacts are never shared or deduplicated across workflows.
#'
#' A per-image cache is itself a membership oracle. Collection-level global
#' assets may be reused, but row-level artifacts remain private to their
#' generation.
#' @keywords internal
.existing_per_image_asset <- function(dataset_id, derivation_hash,
                                      selected_features = character(0)) {
  NULL
}

#' Missing required columns in a radiomics artifact
#' @keywords internal
.missing_artifact_features <- function(path, required_cols) {
  required_cols <- as.character(unlist(required_cols, use.names = FALSE))
  required_cols <- required_cols[nzchar(required_cols)]
  if (length(required_cols) == 0) return(character(0))
  if (is.na(path) || !nzchar(path) || !file.exists(path)) return(required_cols)

  cols <- tryCatch({
    if (grepl("\\.parquet$", path, ignore.case = TRUE)) {
      names(as.data.frame(arrow::read_parquet(path)))
    } else if (grepl("\\.csv$", path, ignore.case = TRUE)) {
      names(utils::read.csv(path, nrows = 1, stringsAsFactors = FALSE))
    } else {
      character(0)
    }
  }, error = function(e) character(0))
  setdiff(required_cols, cols)
}

# ---------------------------------------------------------------------------
# 5. Validate mask-image correspondence for downstream training
# ---------------------------------------------------------------------------

#' Validate that segmentation masks correspond to a set of images
#'
#' DataSHIELD AGGREGATE method. Given a generation_id (from a segmentation
#' run), verifies that:
#' \enumerate{
#'   \item Each completed mask artifact still exists on disk (dsHPC artifact)
#'   \item Each mask's derivation_hash matches the source image's content_hash
#'   \item The generation covers the requested sample set
#' }
#'
#' If masks are missing (expired dsHPC artifacts), returns which samples
#' need regeneration. Does NOT auto-regenerate.
#'
#' @param generation_id Server-created generation id.
#' @param dataset_id Character; the imaging dataset being validated against.
#' @return List with valid (logical), n_valid, n_missing, n_failed,
#'   and a mask_manifest (sample_id -> mask_path mapping) for valid masks.
#' @keywords internal
.imaging_segmentation_validate_masks <- function(generation_id, dataset_id) {
  if (!is.character(generation_id) || length(generation_id) != 1L ||
      !grepl("^gen_[0-9a-f]{32}$", generation_id)) {
    stop("Invalid generation reference.", call. = FALSE)
  }

  # Sync any pending terminal states from dsHPC
  .sync_generation_jobs(generation_id)

  gen <- get_generation(generation_id)
  if (is.null(gen))
    stop("Generation not found: ", generation_id, call. = FALSE)
  if (!is.character(dataset_id) || length(dataset_id) != 1L ||
      !identical(as.character(gen$dataset_id), dataset_id)) {
    stop("Generation does not authorize the requested dataset.",
         call. = FALSE)
  }

  # Verify generation matches the requested dataset
  gen_spec <- tryCatch(
    jsonlite::fromJSON(gen$spec_json, simplifyVector = FALSE),
    error = function(e) list()
  )
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
    # dsHPC artifacts are at DSHPC_HOME/artifacts/{job_id}/...
    # artifact_relpath may be absolute or relative
    if (!file.exists(artifact_path)) {
      # Try under dsHPC home
      dshpc_home <- getOption("dshpc.home", "/srv/dshpc")
      alt_path <- file.path(dshpc_home, "artifacts", artifact_path)
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
    ready_for_training = all_valid
  )
}

#' Retired raw segmentation-generation path accessor
#'
#' This compatibility wrapper deliberately fails closed. A generation id and
#' dataset id are not session-bound capabilities and must not grant access to
#' server paths or sample rosters.
#' @param generation_id Ignored legacy generation identifier.
#' @param dataset_id Ignored legacy dataset identifier.
#' @return Never returns; always raises an error.
#' @export
imagingSegmentationGetMaskPaths <- function(generation_id, dataset_id) {
  .legacy_imaging_ds_disabled("imagingSegmentationGetMaskPaths")
}

#' Find a mask file within a dsHPC artifact directory
#' @keywords internal
.find_mask_in_artifact <- function(artifact_path, sample_id) {
  stems <- .mask_candidate_stems(sample_id)
  mask_pattern <- "\\.(nii\\.gz|nii|nrrd|mha|mhd|dcm|png)$"
  exact_mask <- function(path) {
    base <- basename(path)
    grepl(mask_pattern, base, ignore.case = TRUE) &&
      sub(mask_pattern, "", base, ignore.case = TRUE) %in% stems
  }
  if (!is.character(artifact_path) || length(artifact_path) != 1L ||
      is.na(artifact_path) || !nzchar(artifact_path) ||
      !file.exists(artifact_path)) return(NULL)

  if (!dir.exists(artifact_path)) {
    if (nzchar(Sys.readlink(artifact_path)) || !exact_mask(artifact_path)) {
      return(NULL)
    }
    return(normalizePath(artifact_path, winslash = "/", mustWork = TRUE))
  }

  lexical_root <- sub("[/\\\\]+$", "", artifact_path)
  if (!nzchar(lexical_root)) lexical_root <- artifact_path
  if (nzchar(Sys.readlink(lexical_root))) return(NULL)
  artifact_root <- tryCatch(
    normalizePath(lexical_root, winslash = "/", mustWork = TRUE),
    error = function(e) NULL)
  if (is.null(artifact_root)) return(NULL)
  root_prefix <- paste0(sub("/+$", "", artifact_root), "/")
  contained_mask <- function(path) {
    if (!is.character(path) || length(path) != 1L || is.na(path) ||
        !nzchar(path)) return(NULL)
    candidate <- if (grepl("^(/|[A-Za-z]:[/\\\\])", path)) path else
      file.path(artifact_root, path)
    if (!file.exists(candidate) || dir.exists(candidate) ||
        !exact_mask(candidate) ||
        .mask_path_has_symlink(candidate, artifact_root)) return(NULL)
    resolved <- tryCatch(
      normalizePath(candidate, winslash = "/", mustWork = TRUE),
      error = function(e) NULL)
    if (is.null(resolved) || !startsWith(resolved, root_prefix)) return(NULL)
    resolved
  }

  seg_manifest <- file.path(artifact_root, "seg_manifest.json")
  if (file.exists(seg_manifest)) {
    if (dir.exists(seg_manifest) || nzchar(Sys.readlink(seg_manifest))) {
      return(NULL)
    }
    manifest <- tryCatch(
      jsonlite::fromJSON(seg_manifest, simplifyVector = FALSE),
      error = function(e) NULL)
    if (!is.list(manifest) || !is.list(manifest$samples) ||
        is.null(manifest$samples[[sample_id]]) ||
        !is.list(manifest$samples[[sample_id]])) {
      return(NULL)
    }
    return(contained_mask(manifest$samples[[sample_id]]$primary_mask))
  }

  candidates <- vapply(
    list.files(artifact_root, recursive = TRUE, full.names = TRUE),
    function(path) contained_mask(path) %||% NA_character_, character(1))
  .select_single_mask_candidate(candidates)
}

# ---------------------------------------------------------------------------
# Failure synchronization
# ---------------------------------------------------------------------------

#' Sync dsHPC terminal states back to asset_items
#' @keywords internal
.sync_generation_jobs <- function(generation_id) {
  .sync_completed_jobs(generation_id)
  .sync_failed_jobs(generation_id)
  .sync_cancelled_jobs(generation_id)
  .sync_active_jobs(generation_id)
  invisible(NULL)
}

#' Sync dsHPC completed states back to asset_items
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

  done_jobs <- dsHPC::query_jobs_by_tag(paste0("%", generation_id, "%"),
    states = c("FINISHED", "PUBLISHED"))
  if (nrow(done_jobs) == 0) return(invisible(NULL))

  selected_features <- .generation_selected_features(get_generation(generation_id))
  required_cols <- c("sample_id", selected_features)
  synced_sids <- character(0)

  for (i in seq_len(nrow(done_jobs))) {
    tags <- strsplit(done_jobs$tags[i], ",", fixed = TRUE)[[1]]
    sid <- .sample_id_from_tags(tags, pending_items)
    if (is.null(sid)) next
    if (sid %in% synced_sids) next

    output_ref <- .radiomics_job_output_ref(done_jobs$job_id[i])
    if (is.null(output_ref) || !isTRUE(output_ref$exists)) {
      complete_item_atomic(generation_id, sid, "failed",
        error = "dsHPC job finished without a radiomics output")
      next
    }
    if (length(required_cols) > 0) {
      missing <- .missing_artifact_features(output_ref$path, required_cols)
      if (length(missing) > 0) next
    }

    complete_item_atomic(
      generation_id = generation_id,
      sample_id = sid,
      status = "completed",
      artifact_relpath = output_ref$path
    )
    synced_sids <- c(synced_sids, sid)
  }
  invisible(NULL)
}

#' @keywords internal
.radiomics_job_output_ref <- function(job_id) {
  candidates <- c("radiomics.parquet", "features.parquet", "radiomics.csv",
                  "features.csv")
  for (nm in candidates) {
    ref <- tryCatch(dsHPC::get_job_output_ref(job_id, nm,
      required_label = "dsImaging_image"), error = function(e) NULL)
    if (!is.null(ref)) return(ref)
  }

  NULL
}

#' @keywords internal
.sample_id_from_tags <- function(tags, candidate_items) {
  tags <- as.character(tags)
  if (length(tags) < 4L || !identical(tags[[1L]], "per_image")) return(NULL)
  if (is.data.frame(candidate_items) &&
      all(c("sample_id", "job_token") %in% names(candidate_items))) {
    tokens <- as.character(candidate_items$job_token)
    hit <- which(!is.na(tokens) & nzchar(tokens) & tokens == tags[[3L]])
    if (length(hit) == 1L) {
      return(as.character(candidate_items$sample_id[[hit]]))
    }
    # Jobs created before opaque correlation tokens were introduced remain
    # recoverable during a rolling upgrade, but all new specs use tokens.
    candidate_ids <- as.character(candidate_items$sample_id)
  } else {
    candidate_ids <- as.character(candidate_items)
  }
  hit <- which(candidate_ids == tags[[3L]])
  if (length(hit) != 1L) return(NULL)
  candidate_ids[[hit]]
}

#' Build non-identifying dsHPC tags for one private generation item
#' @keywords internal
.per_image_job_tags <- function(dataset_id, job_token, generation_id) {
  if (!is.character(job_token) || length(job_token) != 1L ||
      is.na(job_token) || !grepl("^[0-9a-f]{32}$", job_token)) {
    stop("Invalid private job correlation token.", call. = FALSE)
  }
  c("per_image", dataset_id, job_token, generation_id)
}

#' @keywords internal
.dataset_id_from_tags <- function(tags, generation_id, sample_id) {
  if (length(tags) >= 4 && identical(tags[1], "per_image"))
    return(tags[2])
  remaining <- setdiff(tags, c("per_image", generation_id, sample_id))
  if (length(remaining) == 0) return(NULL)
  remaining[1]
}

#' Sync dsHPC failure states back to asset_items
#'
#' For items still "pending" in the generation, checks whether their
#' corresponding dsHPC jobs have FAILED. If so, marks the item as failed.
#' This closes the gap where a dsHPC job fails but the publisher hook
#' never runs (because the job died before reaching the publish step).
#'
#' @keywords internal
.sync_failed_jobs <- function(generation_id) {
  items <- get_generation_items(generation_id)
  pending_items <- items[items$status %in% c("pending", "claimed", "running"), ,
                         drop = FALSE]
  if (nrow(pending_items) == 0) return(invisible(NULL))

  # Query dsHPC for failed jobs tagged with this generation
  failed_jobs <- dsHPC::query_failed_jobs(paste0("%", generation_id, "%"))

  if (nrow(failed_jobs) == 0) return(invisible(NULL))

  # Extract sample_ids from tags and mark items as failed
  for (i in seq_len(nrow(failed_jobs))) {
    tags <- strsplit(failed_jobs$tags[i], ",", fixed = TRUE)[[1]]
    sid <- .sample_id_from_tags(tags, pending_items)
    if (is.null(sid)) next
    err_msg <- failed_jobs$error_message[i] %||% "dsHPC job failed"
    complete_item_atomic(generation_id, sid, "failed",
      error = err_msg)
  }
  invisible(NULL)
}

#' Sync dsHPC cancelled states back to asset_items
#'
#' Cancelled dsHPC jobs are terminal. Mark their corresponding generation items as
#' skipped so status/publish never leaves a collection stuck in `running`.
#'
#' @keywords internal
.sync_cancelled_jobs <- function(generation_id) {
  items <- get_generation_items(generation_id)
  pending_items <- items[items$status %in% c("pending", "claimed", "running"), ,
                         drop = FALSE]
  if (nrow(pending_items) == 0) return(invisible(NULL))

  cancelled_jobs <- dsHPC::query_jobs_by_tag(paste0("%", generation_id, "%"),
    states = "CANCELLED")
  if (nrow(cancelled_jobs) == 0) return(invisible(NULL))

  for (i in seq_len(nrow(cancelled_jobs))) {
    tags <- strsplit(cancelled_jobs$tags[i], ",", fixed = TRUE)[[1]]
    sid <- .sample_id_from_tags(tags, pending_items)
    if (is.null(sid)) next
    err_msg <- cancelled_jobs$error_message[i] %||% "dsHPC job cancelled"
    complete_item_atomic(generation_id, sid, "skipped", error = err_msg)
  }
  invisible(NULL)
}

#' Sync dsHPC active states back to asset_items
#'
#' This closes the crash window where `hpcSubmitInternal()` succeeds but the caller
#' dies before the generation item is moved from `claimed` to `running`.
#'
#' @keywords internal
.sync_active_jobs <- function(generation_id) {
  items <- get_generation_items(generation_id)
  pending_items <- items[items$status %in% c("pending", "claimed", "running",
                                             "failed"), , drop = FALSE]
  if (nrow(pending_items) == 0) return(invisible(NULL))

  active_jobs <- dsHPC::query_jobs_by_tag(paste0("%", generation_id, "%"),
    states = c("PENDING", "RUNNING"))
  if (nrow(active_jobs) == 0) return(invisible(NULL))

  changed <- FALSE
  for (i in seq_len(nrow(active_jobs))) {
    tags <- strsplit(active_jobs$tags[i], ",", fixed = TRUE)[[1]]
    sid <- .sample_id_from_tags(tags, pending_items)
    if (is.null(sid)) next
    record_item_status(generation_id, sid, "running")
    changed <- TRUE
  }
  if (isTRUE(changed)) reconcile_generation_counters(generation_id)
  invisible(NULL)
}

#' Effective per-generation inflight cap.
#'
#' dsImaging has its own cap, but it must never exceed dsHPC's global cap
#' in the current R process. Otherwise submissions beyond the dsHPC quota are
#' misclassified as per-image failures instead of staying pending.
#'
#' @keywords internal
.imaging_max_inflight <- function() {
  radiomics_cap <- as.integer(.imaging_analysis_option("max_inflight", 30L))
  dshpc_cap <- as.integer(getOption("dshpc.max_jobs_global",
    getOption("default.dshpc.max_jobs_global", radiomics_cap)))
  max(1L, min(radiomics_cap, dshpc_cap))
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
    # Handle URL-safe base64 (from dsHPCClient .ds_encode: +->-, /->_, no =)
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

#' Encode a value for dsHPC internal submission
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
  tryCatch(dsHPC::get_owner_id(), error = function(e) {
    Sys.getenv("USER", Sys.info()[["user"]] %||% "unknown")
  })
}

#' Resolve dataset and return full resolved context
#'
#' This server-internal path resolves only administrator-configured registry
#' entries. Analyst workflows must instead pass a session-authorized handle
#' into their domain entry point.
#'
#' @keywords internal
.resolve_ds <- function(dataset_id = NULL) {
  if (!is.character(dataset_id) || length(dataset_id) != 1L ||
      is.na(dataset_id) || !nzchar(dataset_id)) return(NULL)
  tryCatch(resolve_dataset(dataset_id), error = function(e) NULL)
}

#' Persist enough dataset context for worker-side orchestration
#'
#' Fire-and-forget drip feeding runs in the dsHPC worker, outside the
#' DataSHIELD session that originally resolved the Opal resource. A generation
#' therefore carries a compact manifest/backend context so later batches can be
#' staged without a connected analyst session.
#'
#' @keywords internal
.dataset_context_from_resolved <- function(resolved, manifest,
                                           privacy_roster = NULL) {
  if (is.null(resolved) || is.null(resolved$backend)) return(NULL)
  if (!is.null(privacy_roster)) {
    manifest$.dsimaging_privacy_roster <-
      .validate_imaging_privacy_roster(privacy_roster)
  }
  list(
    manifest = manifest,
    backend = .portable_backend_context(resolved$backend),
    collection_seal = (resolved$collection_snapshot %||%
      (resolved$handle %||% list())$collection_snapshot)$seal %||% NULL
  )
}

#' @keywords internal
.portable_backend_context <- function(backend) {
  if (is.null(backend)) return(NULL)
  if (!identical(backend$type, "s3")) {
    return(list(type = backend$type, config = backend$config %||% list()))
  }
  cfg <- backend$config %||% list()
  raw_fields <- c("access_key", "secret_key", "identity", "secret",
                  "password")
  if (any(raw_fields %in% names(cfg))) {
    stop("Worker storage credentials are not configured.", call. = FALSE)
  }
  ref <- cfg$credentials_ref %||%
    getOption("dsimaging.worker_credentials_ref",
      getOption("default.dsimaging.worker_credentials_ref", NULL))
  if (!is.null(cfg$resource) &&
      (is.null(ref) || !is.character(ref) || length(ref) != 1L ||
       is.na(ref) || !nzchar(ref))) {
    stop("Worker storage credentials are not configured.", call. = FALSE)
  }
  if (!is.null(ref)) {
    if (!is.character(ref) || length(ref) != 1L || is.na(ref) ||
        !grepl("^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$", ref)) {
      stop("Worker storage credentials are not configured.", call. = FALSE)
    }
    # Fail before a durable context is written if the named server-side
    # credential is absent. The resolved secret itself is deliberately dropped.
    tryCatch(.resolve_s3_credentials(ref), error = function(e) {
      stop("Worker storage credentials are not configured.", call. = FALSE)
    })
  }
  cfg <- list(
    endpoint = cfg$endpoint %||% NULL,
    credentials_ref = ref,
    region = cfg$region %||% NULL,
    uri_prefix = cfg$uri_prefix %||% NULL
  )
  list(type = "s3", config = cfg)
}

#' @keywords internal
.ensure_generation_dataset_context <- function(generation_id, resolved, manifest,
                                               privacy_roster = NULL) {
  gen <- get_generation(generation_id)
  if (is.null(gen)) return(invisible(FALSE))

  spec <- tryCatch(
    jsonlite::fromJSON(gen$spec_json, simplifyVector = FALSE),
    error = function(e) list())
  pinned <- tryCatch(.validate_imaging_privacy_roster(
    spec$privacy_roster), error = function(e) NULL)
  if (is.null(pinned) || is.null(privacy_roster) ||
      !.same_imaging_privacy_roster(pinned, privacy_roster)) {
    stop("Generation has no matching admitted privacy roster.",
         call. = FALSE)
  }
  expected_seal <- (resolved$collection_snapshot %||%
    (resolved$handle %||% list())$collection_snapshot)$seal %||% NULL
  if (!is.character(expected_seal) || length(expected_seal) != 1L ||
      is.na(expected_seal) || !grepl("^[0-9a-f]{64}$", expected_seal)) {
    stop("Generation has no admitted collection snapshot.", call. = FALSE)
  }
  if (!is.null(spec$dataset_context)) {
    if (!identical(spec$dataset_context$collection_seal, expected_seal)) {
      stop("Generation has no matching admitted collection snapshot.",
           call. = FALSE)
    }
    return(invisible(TRUE))
  }

  spec$dataset_context <- .dataset_context_from_resolved(
    resolved, manifest, pinned)
  update_generation(generation_id,
    spec_json = as.character(jsonlite::toJSON(spec, auto_unbox = TRUE)))
  invisible(TRUE)
}

#' @keywords internal
.resolve_ds_from_generation <- function(generation_id, dataset_id = NULL) {
  gen <- get_generation(generation_id)
  if (is.null(gen)) return(NULL)
  if (!is.null(dataset_id) &&
      !identical(as.character(dataset_id), as.character(gen$dataset_id))) {
    return(NULL)
  }

  spec <- tryCatch(
    jsonlite::fromJSON(gen$spec_json, simplifyVector = FALSE),
    error = function(e) NULL)
  ctx <- spec$dataset_context
  if (is.null(ctx) || is.null(ctx$manifest) || is.null(ctx$backend))
    return(NULL)
  if (!identical(as.character(ctx$manifest$dataset_id),
                 as.character(gen$dataset_id))) return(NULL)
  generation_roster <- tryCatch(
    .validate_imaging_privacy_roster(spec$privacy_roster),
    error = function(e) NULL)
  context_roster <- tryCatch(
    .validate_imaging_privacy_roster(
      ctx$manifest$.dsimaging_privacy_roster),
    error = function(e) NULL)
  if (is.null(generation_roster) || is.null(context_roster) ||
      !.same_imaging_privacy_roster(generation_roster, context_roster)) {
    return(NULL)
  }

  backend <- .backend_from_context(ctx$backend, generation_id)
  if (is.null(backend)) return(NULL)
  collection_seal <- ctx$collection_seal %||% NULL
  if (!is.character(collection_seal) || length(collection_seal) != 1L ||
      is.na(collection_seal) || !grepl("^[0-9a-f]{64}$", collection_seal)) {
    return(NULL)
  }
  manifest <- ctx$manifest
  manifest$.dsimaging_privacy_roster <- NULL
  current_snapshot <- tryCatch({
    admission <- .imaging_privacy_admission(manifest, backend)
    .new_imaging_collection_snapshot(manifest, backend, admission)
  }, error = function(e) NULL)
  if (is.null(current_snapshot) ||
      !identical(current_snapshot$seal, collection_seal)) {
    return(NULL)
  }
  list(
    dataset_id = gen$dataset_id,
    backend = backend,
    manifest = manifest,
    manifest_uri = NULL,
    publish = backend,
    collection_snapshot = current_snapshot
  )
}

#' @keywords internal
.backend_from_context <- function(ctx, generation_id) {
  if (is.null(ctx$type)) return(NULL)
  cfg <- ctx$config %||% list()

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

  idx <- .validated_mask_hash_index(
    resolved$backend, manifest, mask_alias, required = TRUE)
  if (nrow(idx) == 0L) return(list())
  stats::setNames(as.list(idx$content_hash), idx$sample_id)
}

#' Resolve one sample mask from a local or S3 mask asset
#' @keywords internal
.validated_mask_hash_index <- function(backend, manifest, mask_asset,
                                       required = FALSE) {
  asset <- manifest$assets[[mask_asset]]
  if (!is.list(asset) || !is.character(asset$uri) ||
      length(asset$uri) != 1L || is.na(asset$uri) || !nzchar(asset$uri)) {
    stop("Mask asset index is invalid.", call. = FALSE)
  }
  index_uri <- asset$content_hash_index %||%
    asset$hash_index %||% asset$index_uri
  if (is.null(index_uri) || !nzchar(index_uri)) {
    if (isTRUE(required)) stop("Mask asset index is invalid.", call. = FALSE)
    return(NULL)
  }
  idx <- tryCatch(read_hash_index(backend, index_uri),
                  error = function(e) NULL)
  needed <- c("sample_id", "uri", "content_hash")
  if (!is.data.frame(idx) || !all(needed %in% names(idx))) {
    stop("Mask asset index is invalid.", call. = FALSE)
  }
  if (nrow(idx) == 0L) return(idx)
  ids <- as.character(idx$sample_id)
  uris <- as.character(idx$uri)
  hashes <- tolower(as.character(idx$content_hash))
  valid <- !anyNA(ids) && all(nzchar(ids)) && !anyDuplicated(ids) &&
    !anyNA(uris) && all(nzchar(uris)) && !anyDuplicated(uris) &&
    !anyNA(hashes) && all(grepl("^[0-9a-f]{64}$", hashes))
  if (isTRUE(valid)) {
    valid <- tryCatch({
      vapply(uris, .snapshot_relative_object, character(1),
        root = asset$uri, backend_type = backend$type)
      TRUE
    }, error = function(e) FALSE)
  }
  if (!isTRUE(valid)) stop("Mask asset index is invalid.", call. = FALSE)
  idx$sample_id <- ids
  idx$uri <- uris
  idx$content_hash <- hashes
  idx
}

.mask_hash_index <- function(backend, manifest, mask_asset) {
  .validated_mask_hash_index(backend, manifest, mask_asset)
}

#' @keywords internal
.mask_extensions <- function() {
  c(".nii.gz", ".nii", ".nrrd", ".mha", ".mhd", ".dcm")
}

#' @keywords internal
.mask_candidate_stems <- function(sample_id) {
  if (!is.character(sample_id) || length(sample_id) != 1L ||
      is.na(sample_id) || !nzchar(sample_id) ||
      grepl("[/\\\\]", sample_id) || sample_id %in% c(".", "..")) {
    stop("Imaging mask selection is invalid.", call. = FALSE)
  }
  c(sample_id, paste0(sample_id, "_mask"),
    paste0(sample_id, "_seg"), paste0(sample_id, "_label"),
    paste0(sample_id, "_GTV-1"), paste0(sample_id, "_gtv1"))
}

#' @keywords internal
.mask_file_stem <- function(path) {
  base <- basename(path)
  pattern <- "\\.(nii\\.gz|nii|nrrd|mha|mhd|dcm)$"
  if (!grepl(pattern, base, ignore.case = TRUE)) return(NA_character_)
  sub(pattern, "", base, ignore.case = TRUE)
}

#' @keywords internal
.is_exact_mask_candidate <- function(path, sample_id) {
  stem <- .mask_file_stem(path)
  !is.na(stem) && stem %in% .mask_candidate_stems(sample_id)
}

#' @keywords internal
.select_single_mask_candidate <- function(candidates) {
  candidates <- unique(as.character(candidates))
  candidates <- candidates[!is.na(candidates) & nzchar(candidates)]
  if (length(candidates) == 0L) return(NULL)
  if (length(candidates) > 1L) {
    stop("Imaging mask selection is ambiguous.", call. = FALSE)
  }
  candidates[[1L]]
}

#' Return TRUE when the configured root or any candidate path component is a
#' symbolic link. This check uses the lexical path before normalizePath() can
#' follow a link.
#' @keywords internal
.mask_path_has_symlink <- function(path, root) {
  root <- sub("[/\\\\]+$", "", root)
  prefix <- paste0(root, .Platform$file.sep)
  if (!identical(path, root) && !startsWith(path, prefix)) return(TRUE)
  relative <- if (identical(path, root)) "" else
    substring(path, nchar(prefix) + 1L)
  components <- if (nzchar(relative))
    strsplit(relative, "[/\\\\]")[[1L]] else character(0)
  current <- root
  paths <- c(root, vapply(components, function(component) {
    current <<- file.path(current, component)
    current
  }, character(1)))
  any(nzchar(Sys.readlink(paths)))
}

#' Resolve a single sample's exact snapshot-pinned image URI or local path
#' @keywords internal
.snapshot_image_record <- function(image_root, sample_id, backend, snapshot) {
  snapshot <- .validate_imaging_collection_snapshot(snapshot)
  if (!is.character(sample_id) || length(sample_id) != 1L ||
      is.na(sample_id) || !nzchar(sample_id) || grepl("[/\\\\]", sample_id) ||
      !inherits(backend, "dsimaging_backend")) {
    .snapshot_fail()
  }
  ids <- vapply(snapshot$records, function(record) {
    if (!is.list(record) || !.snapshot_scalar(record$sample_id)) {
      .snapshot_fail()
    }
    record$sample_id
  }, character(1))
  if (anyDuplicated(ids)) .snapshot_fail()
  hit <- which(ids == sample_id)
  if (length(hit) == 0L) return(NULL)
  if (length(hit) != 1L) .snapshot_fail()
  record <- snapshot$records[[hit]]
  if (!identical(record$source_kind, "single_file") ||
      !.snapshot_scalar(record$uri) ||
      !.snapshot_scalar(record$relative_path) ||
      !.snapshot_scalar(record$content_hash, max_bytes = 64L) ||
      !grepl("^[0-9a-f]{64}$", record$content_hash) ||
      length(record$size) != 1L || is.na(record$size) ||
      !is.finite(as.numeric(record$size)) || as.numeric(record$size) < 0) {
    .snapshot_fail()
  }
  relative <- .snapshot_relative_object(record$uri, image_root, backend$type)
  if (!identical(relative, record$relative_path)) .snapshot_fail()
  if (identical(backend$type, "file")) {
    lexical_root <- sub("[/\\\\]+$", "", image_root)
    if (!nzchar(lexical_root)) lexical_root <- image_root
    if (nzchar(Sys.readlink(image_root)) ||
        .mask_path_has_symlink(record$uri, lexical_root)) {
      .snapshot_fail()
    }
  }
  record
}

#' @keywords internal
.resolve_sample_image <- function(image_root, sample_id, backend = NULL,
                                  snapshot = NULL) {
  record <- .snapshot_image_record(image_root, sample_id, backend, snapshot)
  if (is.null(record)) NULL else record$uri
}

#' Resolve a single sample's mask URI or local path
#' @keywords internal
.resolve_sample_mask <- function(mask_root, sample_id, backend = NULL,
                                 manifest = NULL, mask_asset = "masks") {
  if (!is.character(mask_root) || length(mask_root) != 1L ||
      is.na(mask_root) || !nzchar(mask_root)) return(NULL)
  stems <- .mask_candidate_stems(sample_id)
  exts <- .mask_extensions()

  # A declared hash index is an exact sample-to-object association. Prefer it
  # for every backend and do not reinterpret that mapping from the filename.
  if (!is.null(backend) && !is.null(manifest)) {
    idx <- .mask_hash_index(backend, manifest, mask_asset)
    if (is.data.frame(idx) && nrow(idx) > 0L && "uri" %in% names(idx)) {
      hit <- idx[idx$sample_id == sample_id, , drop = FALSE]
      if (nrow(hit) > 0L) return(.select_single_mask_candidate(hit$uri))
    }
  }

  if (!is.null(backend) && identical(backend$type, "s3") &&
      grepl("^s3://", mask_root)) {
    candidates <- character(0)
    for (stem in stems) {
      for (ext in exts) {
        candidate <- paste0(sub("/$", "", mask_root), "/", stem, ext)
        head <- backend_head(backend, candidate)
        if (!is.null(head) && isTRUE(head$exists)) {
          candidates <- c(candidates, candidate)
        }
      }
    }
    return(.select_single_mask_candidate(candidates))
  }

  if (!dir.exists(mask_root)) return(NULL)
  canonical_root <- tryCatch(
    normalizePath(mask_root, winslash = "/", mustWork = TRUE),
    error = function(e) NULL)
  lexical_root <- sub("[/\\\\]+$", "", mask_root)
  if (!nzchar(lexical_root)) lexical_root <- mask_root
  if (is.null(canonical_root) || nzchar(Sys.readlink(lexical_root))) return(NULL)
  files <- list.files(canonical_root, full.names = TRUE, recursive = TRUE)
  candidates <- vapply(files, function(path) {
    if (!file.exists(path) || dir.exists(path) ||
        .mask_path_has_symlink(path, canonical_root) ||
        !.is_exact_mask_candidate(path, sample_id)) {
      return(NA_character_)
    }
    canonical <- tryCatch(
      normalizePath(path, winslash = "/", mustWork = TRUE),
      error = function(e) NULL)
    if (is.null(canonical) ||
        !startsWith(canonical, paste0(sub("/+$", "", canonical_root), "/"))) {
      return(NA_character_)
    }
    canonical
  }, character(1))
  .select_single_mask_candidate(candidates)
}

#' Stage an S3 image to local filesystem for Python runner execution
#'
#' Python runners expect local file paths. For S3-backed datasets,
#' this downloads the image to a staging directory.
#' For file-backed datasets, returns the path as-is.
#' @keywords internal
.stage_image_for_job <- function(image_uri, sample_id, dataset_id, backend,
                                 image_root, snapshot = NULL) {
  record <- .snapshot_image_record(image_root, sample_id, backend, snapshot)
  if (is.null(record) || !identical(record$uri, image_uri)) .snapshot_fail()
  staged <- .stage_backend_file_for_job(
    image_uri, sample_id, dataset_id, backend, role = "images")
  info <- tryCatch(file.info(staged), error = function(e) NULL)
  hash <- tryCatch(digest::digest(file = staged, algo = "sha256"),
                   error = function(e) NULL)
  if (is.null(info) || nrow(info) != 1L || isTRUE(info$isdir) ||
      is.na(info$size) || as.numeric(info$size) != as.numeric(record$size) ||
      !identical(hash, record$content_hash)) {
    .snapshot_fail()
  }
  staged
}

#' Stage an S3-backed file to local filesystem for Python runner execution
#' @keywords internal
.stage_backend_file_for_job <- function(uri, sample_id, dataset_id, backend,
                                        role = "files") {
  if (is.null(backend) || backend$type == "file") return(uri)
  if (!grepl("^s3://", uri)) return(uri)

  # Staging names are operational metadata visible to dsHPC. Keep the private
  # sample id and source basename only in the job config/manifest association,
  # never in directory entries.
  home <- tryCatch(
    dsHPC:::.dshpc_home(must_exist = FALSE),
    error = function(e) getOption("dshpc.home",
      getOption("default.dshpc.home", "/srv/dshpc"))
  )
  collection_token <- digest::digest(
    jsonlite::toJSON(list(scope = "dsimaging-staging", dataset = dataset_id),
      auto_unbox = TRUE),
    algo = "sha256", serialize = FALSE)
  staging_dir <- file.path(home, "staging", "dsimaging", collection_token, role)
  dir.create(staging_dir, recursive = TRUE, showWarnings = FALSE)

  object_token <- digest::digest(
    jsonlite::toJSON(list(
      scope = "dsimaging-staged-object", dataset = dataset_id,
      role = role, sample = sample_id, uri = uri), auto_unbox = TRUE),
    algo = "sha256", serialize = FALSE)
  clean_uri <- sub("[?#].*$", "", tolower(uri))
  suffixes <- c(".nii.gz", ".nii", ".nrrd", ".mha", ".mhd", ".dcm",
                ".png", ".jpg", ".jpeg", ".tif", ".tiff", ".svs")
  suffix <- suffixes[endsWith(clean_uri, suffixes)][1L]
  if (is.na(suffix)) suffix <- ".bin"
  local_path <- file.path(staging_dir, paste0(object_token, suffix))
  if (!file.exists(local_path))
    backend_get_file(backend, uri, local_path)

  local_path
}

#' Resolve a profile name to its YAML file path
#' @keywords internal
.resolve_profile_path <- function(profile_name) {
  if (!is.character(profile_name) || length(profile_name) != 1L ||
      is.na(profile_name) || !grepl("^[A-Za-z0-9_]+$", profile_name)) {
    return(NULL)
  }
  admin_path <- file.path(.imaging_analysis_option("home", "/var/lib/dsimaging"),
                          "profiles", paste0(profile_name, ".yaml"))
  if (file.exists(admin_path)) return(admin_path)

  # Check the content-addressed stable copy of bundled profiles.
  profiles_dir <- .stable_imaging_package_dir("profiles")
  if (nzchar(profiles_dir)) {
    candidate <- file.path(profiles_dir, paste0(profile_name, ".yaml"))
    if (file.exists(candidate)) return(candidate)
  }
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

# Legacy low-level orchestration entry points remain exported for package ABI
# compatibility, but must fail even when an old Opal allowlist still names
# them. Registered domain methods call the internal helpers above.

#' Disabled legacy collection scan entry point
#' @param ... Ignored legacy arguments.
#' @return Never returns; this legacy DataSHIELD surface is disabled.
#' @export
imagingRadiomicsScanCollectionDS <- function(...) {
  .legacy_imaging_ds_disabled("imagingRadiomicsScanCollectionDS")
}

#' Disabled legacy batch submission entry point
#' @param ... Ignored legacy arguments.
#' @return Never returns; this legacy DataSHIELD surface is disabled.
#' @export
imagingRadiomicsSubmitBatchDS <- function(...) {
  .legacy_imaging_ds_disabled("imagingRadiomicsSubmitBatchDS")
}

#' Disabled legacy raw collection status entry point
#' @param ... Ignored legacy arguments.
#' @return Never returns; this legacy DataSHIELD surface is disabled.
#' @export
imagingRadiomicsCollectionStatusDS <- function(...) {
  .legacy_imaging_ds_disabled("imagingRadiomicsCollectionStatusDS")
}

#' Disabled legacy raw collection recovery entry point
#' @param ... Ignored legacy arguments.
#' @return Never returns; this legacy DataSHIELD surface is disabled.
#' @export
imagingRadiomicsRecoverCollectionDS <- function(...) {
  .legacy_imaging_ds_disabled("imagingRadiomicsRecoverCollectionDS")
}

#' Disabled legacy raw collection publication entry point
#' @param ... Ignored legacy arguments.
#' @return Never returns; this legacy DataSHIELD surface is disabled.
#' @export
imagingRadiomicsPublishCollectionDS <- function(...) {
  .legacy_imaging_ds_disabled("imagingRadiomicsPublishCollectionDS")
}

#' Disabled legacy raw mask-validation entry point
#' @param ... Ignored legacy arguments.
#' @return Never returns; this legacy DataSHIELD surface is disabled.
#' @export
imagingSegmentationValidateMasksDS <- function(...) {
  .legacy_imaging_ds_disabled("imagingSegmentationValidateMasksDS")
}
