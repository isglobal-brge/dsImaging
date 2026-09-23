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

  cfg <- step$config %||% list()
  deriv_hash <- step$derivation_hash %||% cfg$derivation_hash
  if (is.null(deriv_hash) && !is.null(step$config)) {
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
  scope <- .imaging_publisher_scope(db, job_id)
  collection_seal <- .assert_publishable_imaging_feature_asset(
    output_dir, asset_type, cfg, step$runner)

  asset_id <- register_derived_asset(
    dataset_id = dataset_id,
    kind = asset_type,
    path_or_root = output_dir,
    derivation_hash = deriv_hash,
    provenance = provenance,
    created_by = scope$owner_id,
    created_by_job = job_id,
    collection_seal = collection_seal,
    description = step$description %||% paste(asset_type, "from job", job_id),
    alias = step$alias,
    visibility = scope$visibility
  )
  if (isTRUE(step$tracking_output)) {
    .imaging_tracking_publish_job_asset(job_id, asset_id)
  }

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
  scope <- .imaging_publisher_scope(db, job_id)
  collection_seal <- .assert_publishable_imaging_feature_asset(
    output_dir, asset_type, cfg, step$runner)

  asset_id <- register_derived_asset(
    dataset_id = dataset_id,
    kind = asset_type,
    path_or_root = output_dir,
    derivation_hash = deriv_hash,
    provenance = provenance,
    created_by = scope$owner_id,
    created_by_job = job_id,
    collection_seal = collection_seal,
    description = step$description %||% paste(asset_type, "from job", job_id),
    alias = step$alias,
    visibility = scope$visibility
  )
  if (isTRUE(step$tracking_output)) {
    .imaging_tracking_publish_job_asset(job_id, asset_id)
  }

  list(status = "published", asset_id = asset_id,
       dataset_id = dataset_id, kind = asset_type)
}

#' Validate collection cardinality before an analytical asset enters catalog.
#' @keywords internal
.assert_publishable_imaging_feature_asset <- function(output_dir, asset_type,
                                                      config = list(),
                                                      runner = NULL) {
  feature_kinds <- c("radiomics_collection", "feature_table", "qc_table",
    "dose_table", "embedding_table")
  mapped_kinds <- c("image_root", "mask_root", "qc_visual_asset", "wsi_tile_root")
  if (!asset_type %in% c(feature_kinds, mapped_kinds)) {
    stop("Imaging publication type is not supported.", call. = FALSE)
  }

  context_id <- config$dataset_id %||% NULL
  if (!is.character(context_id) || length(context_id) != 1L ||
      is.na(context_id) || !grepl("^dsctx_[0-9a-f]{64}$", context_id)) {
    stop("Feature publication has no admitted dataset context.",
         call. = FALSE)
  }
  resolved <- tryCatch(resolve_dataset(context_id),
                       error = function(e) NULL)
  if (is.null(resolved)) {
    stop("Feature publication dataset context is unavailable.",
         call. = FALSE)
  }
  manifest <- tryCatch(
    parse_manifest(resolved$manifest_uri, resolved$backend),
    error = function(e) NULL)
  if (is.null(manifest)) {
    stop("Feature publication dataset context is unavailable.",
         call. = FALSE)
  }
  admission <- .imaging_privacy_admission(manifest, resolved$backend)
  collection_seal <- .asset_collection_seal(
    manifest$.dsimaging_collection_seal, required = TRUE)
  authorized <- list(
    dataset_id = manifest$dataset_id,
    manifest = manifest,
    backend = resolved$backend,
    privacy = admission$contract,
    privacy_roster = admission$roster)
  if (asset_type %in% mapped_kinds) {
    .assert_mapped_imaging_output(output_dir, asset_type,
                                  admission$roster, config, runner)
    return(invisible(collection_seal))
  }

  tables <- list.files(output_dir, pattern = "[.](csv|parquet)$",
                       recursive = TRUE, full.names = TRUE,
                       ignore.case = TRUE)
  if (length(tables) != 1L) {
    stop("Feature publication output is unavailable.", call. = FALSE)
  }
  asset <- list(path_or_root = output_dir, kind = asset_type)
  feature_data <- tryCatch(
    .read_feature_asset(asset, manifest$dataset_id, resolved = authorized),
    error = function(e) NULL)
  if (is.null(feature_data)) {
    stop("Feature publication output is unavailable.", call. = FALSE)
  }
  if (identical(asset_type, "dose_table")) {
    .assert_dose_asset_privacy(feature_data, authorized)
    has_masks <- any(feature_data$roi == "mask")
    if (!identical(has_masks, !is.null(config$mask_asset) &&
        nzchar(config$mask_asset))) {
      stop("Imaging dose asset ROI mapping is unavailable.", call. = FALSE)
    }
  } else {
    .assert_feature_asset_privacy(feature_data, authorized,
      context = "published imaging feature asset")
  }
  invisible(collection_seal)
}

#' Validate a runner's exact per-sample output map and artifact confinement.
#' @keywords internal
.assert_mapped_imaging_output <- function(output_dir, asset_type, roster,
                                          config = list(), runner = NULL) {
  if (!is.character(output_dir) || length(output_dir) != 1L ||
      is.na(output_dir) || !dir.exists(output_dir)) {
    stop("Imaging publication output is unavailable.", call. = FALSE)
  }
  root <- tryCatch(normalizePath(output_dir, winslash = "/", mustWork = TRUE),
                   error = function(e) NULL)
  entries <- list.files(output_dir, all.files = TRUE, no.. = TRUE,
    recursive = TRUE, full.names = TRUE, include.dirs = TRUE)
  links <- Sys.readlink(c(output_dir, entries))
  if (any(!is.na(links) & nzchar(links))) {
    stop("Imaging publication contains a symbolic link.", call. = FALSE)
  }
  path <- file.path(output_dir, "dsimaging_output_manifest.json")
  manifest <- tryCatch(
    jsonlite::fromJSON(path, simplifyVector = FALSE),
    error = function(e) NULL)
  if (is.null(root) || !is.list(manifest) ||
      !identical(as.integer(manifest$schema_version), 1L) ||
      !identical(manifest$artifact_type, asset_type) ||
      !is.list(manifest$samples)) {
    stop("Imaging publication has no exact sample mapping.", call. = FALSE)
  }
  samples <- manifest$samples
  ids <- vapply(samples, function(sample) {
    if (!is.list(sample) || !is.character(sample$sample_id) ||
        length(sample$sample_id) != 1L || is.na(sample$sample_id)) {
      return(NA_character_)
    }
    sample$sample_id
  }, character(1))
  .assert_exact_imaging_roster(ids, roster,
    context = "Published imaging asset")
  if (!identical(ids, .canonical_imaging_privacy_ids(ids))) {
    stop("Imaging publication sample mapping is invalid.", call. = FALSE)
  }

  mapped <- character(0)
  allowed <- "[.](nii([.]gz)?|nrrd|mha|mhd|dcm|png|jpe?g)$"
  if (identical(asset_type, "wsi_tile_root")) allowed <- "[.](json|png)$"
  rendered <- character(0)
  for (sample in samples) {
    files <- unlist(sample$files, use.names = FALSE)
    primary <- sample$primary %||% NULL
    integrity <- sample$file_integrity %||% NULL
    if (identical(asset_type, "qc_visual_asset") &&
        identical(sample$status, "omitted_by_cap")) {
      if (!is.null(primary) || length(files) != 0L ||
          !is.list(integrity) || length(integrity) != 0L) {
        stop("QC publication cap mapping is invalid.", call. = FALSE)
      }
      next
    }
    if (!is.null(sample$status)) {
      stop("Imaging publication sample status is invalid.", call. = FALSE)
    }
    if (!is.character(files) || length(files) == 0L || anyNA(files) ||
        anyDuplicated(files) || !is.character(primary) ||
        length(primary) != 1L || is.na(primary) || !primary %in% files ||
        !is.list(integrity) || length(integrity) != length(files)) {
      stop("Imaging publication sample mapping is invalid.", call. = FALSE)
    }
    if (identical(asset_type, "mask_root") &&
        isTRUE(runner %in% c("rt_convert", "monai_bundle_infer")) &&
        length(files) != 1L) {
      stop("Imaging publication requires one mask per sample.", call. = FALSE)
    }
    integrity_paths <- vapply(integrity, function(record) {
      if (!is.list(record) || !is.character(record$path) ||
          length(record$path) != 1L || is.na(record$path)) {
        return(NA_character_)
      }
      record$path
    }, character(1))
    if (anyNA(integrity_paths) || anyDuplicated(integrity_paths) ||
        !setequal(files, integrity_paths)) {
      stop("Imaging publication sample mapping is invalid.", call. = FALSE)
    }
    for (relative in files) {
      relative <- tryCatch(.snapshot_safe_relative_path(relative),
                           error = function(e) NULL)
      if (is.null(relative) || !grepl(allowed, relative,
                                      ignore.case = TRUE)) {
        stop("Imaging publication sample mapping is invalid.", call. = FALSE)
      }
      candidate <- tryCatch(
        normalizePath(file.path(root, relative), winslash = "/",
                      mustWork = TRUE), error = function(e) NULL)
      info <- if (is.null(candidate)) NULL else file.info(candidate)
      if (is.null(candidate) ||
          !startsWith(candidate, paste0(sub("/+$", "", root), "/")) ||
          nrow(info) != 1L || is.na(info$isdir) || isTRUE(info$isdir)) {
        stop("Imaging publication artifact is unavailable.", call. = FALSE)
      }
      integrity_record <- integrity[[match(relative, integrity_paths)]]
      expected_size <- suppressWarnings(as.numeric(integrity_record$size))
      expected_hash <- tolower(as.character(integrity_record$sha256))
      actual_hash <- tryCatch(digest::digest(
        file = candidate, algo = "sha256"), error = function(e) NA_character_)
      if (length(expected_size) != 1L || is.na(expected_size) ||
          !is.finite(expected_size) || expected_size < 0 ||
          expected_size %% 1 != 0 || as.numeric(info$size) != expected_size ||
          length(expected_hash) != 1L || is.na(expected_hash) ||
          !grepl("^[0-9a-f]{64}$", expected_hash) ||
          !identical(actual_hash, expected_hash)) {
        stop("Imaging publication artifact failed integrity verification.",
             call. = FALSE)
      }
      mapped <- c(mapped, candidate)
    }
    if (identical(asset_type, "wsi_tile_root")) {
      .assert_wsi_sample_output(root, sample, config)
    }
    rendered <- c(rendered, sample$sample_id)
  }
  if (anyDuplicated(mapped)) {
    stop("Imaging publication attributes an artifact more than once.",
         call. = FALSE)
  }
  payload <- list.files(root, pattern = allowed, recursive = TRUE,
                        full.names = TRUE, ignore.case = TRUE,
                        all.files = TRUE, no.. = TRUE)
  if (identical(asset_type, "wsi_tile_root")) {
    payload <- list.files(root, all.files = TRUE, no.. = TRUE,
      recursive = TRUE, full.names = TRUE)
    payload <- setdiff(payload, file.path(root,
      c("dsimaging_output_manifest.json", "wsi_tiling_summary.json")))
  }
  payload <- vapply(payload, normalizePath, character(1), winslash = "/",
                    mustWork = TRUE)
  if (!setequal(mapped, payload)) {
    stop("Imaging publication contains unmapped sample artifacts.",
         call. = FALSE)
  }
  if (identical(asset_type, "qc_visual_asset")) {
    max_tiles <- config$max_tiles %||% 64L
    if (!is.numeric(max_tiles) || length(max_tiles) != 1L ||
        is.na(max_tiles) || max_tiles < 1 || max_tiles > 1024 ||
        max_tiles %% 1 != 0 ||
        !identical(sort(rendered, method = "radix"),
          head(sort(ids, method = "radix"), max_tiles))) {
      stop("QC publication cap mapping is invalid.", call. = FALSE)
    }
    table <- tryCatch(utils::read.csv(
      file.path(root, "qc_visual_manifest.csv"),
      stringsAsFactors = FALSE, check.names = FALSE,
      colClasses = c("character", "character", "character", NA, NA)),
      error = function(e) NULL)
    if (!is.data.frame(table) ||
        !identical(names(table), c("sample_id", "qc_id", "file",
                                  "has_mask", "width_px")) ||
        anyNA(table$sample_id) || anyDuplicated(table$sample_id) ||
        !identical(as.character(table$sample_id),
                   sort(rendered, method = "radix"))) {
      stop("QC publication has no exact sample table.", call. = FALSE)
    }
    for (i in seq_len(nrow(table))) {
      sample <- samples[[match(table$sample_id[[i]], ids)]]
      if (length(sample$files) != 1L ||
          !identical(table$file[[i]], sample$primary) ||
          !identical(paste0(table$qc_id[[i]], ".png"), sample$primary) ||
          !grepl("^case_[0-9]{4,}_[0-9a-f]{12}[.]png$", sample$primary)) {
        stop("QC publication thumbnail mapping is invalid.", call. = FALSE)
      }
    }
  }
  invisible(TRUE)
}

#' Bind each private slide manifest to exactly its own tile files.
#' @keywords internal
.assert_wsi_sample_output <- function(root, sample, config) {
  fail <- function() stop("WSI publication tile mapping is invalid.",
                          call. = FALSE)
  if (!grepl("[.]json$", sample$primary)) fail()
  manifest <- tryCatch(jsonlite::fromJSON(
    file.path(root, sample$primary), simplifyVector = FALSE),
    error = function(e) NULL)
  if (!is.list(manifest) ||
      !identical(manifest$sample_id, sample$sample_id) ||
      !is.numeric(manifest$n_tiles) || length(manifest$n_tiles) != 1L ||
      is.na(manifest$n_tiles) || manifest$n_tiles < 0 ||
      manifest$n_tiles %% 1 != 0 || !is.list(manifest$tiles) ||
      manifest$n_tiles != length(manifest$tiles) ||
      manifest$n_tiles > (config$max_tiles %||% 2048L)) fail()
  write_tiles <- config$write_tiles %||% TRUE
  tile_files <- character(0)
  positions <- character(0)
  for (tile in manifest$tiles) {
    if (!is.list(tile) || !setequal(names(tile),
        c("tile_file", "x", "y", "tile_size", "tissue_fraction")) ||
        !is.character(tile$tile_file) || length(tile$tile_file) != 1L ||
        is.na(tile$tile_file)) fail()
    for (field in c("x", "y", "tile_size", "tissue_fraction")) {
      value <- tile[[field]]
      if (!is.numeric(value) || length(value) != 1L || is.na(value) ||
          !is.finite(value) || value < 0) fail()
    }
    if (tile$x %% 1 != 0 || tile$y %% 1 != 0 || tile$tile_size < 1 ||
        tile$tile_size %% 1 != 0 || tile$tissue_fraction > 1 ||
        (isTRUE(write_tiles) && !grepl("[.]png$", tile$tile_file)) ||
        (!isTRUE(write_tiles) && nzchar(tile$tile_file))) fail()
    tile_files <- c(tile_files, tile$tile_file[nzchar(tile$tile_file)])
    positions <- c(positions, paste(tile$x, tile$y, sep = ":"))
  }
  files <- unlist(sample$files, use.names = FALSE)
  if (anyDuplicated(tile_files) || anyDuplicated(positions) ||
      !setequal(files, c(sample$primary, tile_files))) fail()
  invisible(TRUE)
}

#' Per-image result publisher (dsHPC plugin)
#'
#' Called when a per-image job completes its publish step.
#' Four responsibilities:
#'   1. Record item as completed in the generation
#'   2. Atomically increment completed_n counter
#'   3. Keep the per-image artifact private to its generation
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

  complete_item_atomic(
    generation_id = generation_id,
    sample_id = sample_id,
    status = "completed",
    artifact_relpath = artifact_relpath
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

  list(status = "published")
}

#' Resolve immutable publication scope from the dsHPC job row
#' @keywords internal
.imaging_publisher_scope <- function(db, job_id) {
  row <- tryCatch(
    DBI::dbGetQuery(db,
      "SELECT owner_id, visibility FROM jobs WHERE job_id = ?",
      params = list(job_id)),
    error = function(e) data.frame())
  if (nrow(row) != 1L ||
      !as.character(row$visibility[1]) %in% c("private", "global")) {
    return(list(owner_id = NA_character_, visibility = "private"))
  }
  list(owner_id = as.character(row$owner_id[1]),
       visibility = as.character(row$visibility[1]))
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
  files <- list.files(output_dir, pattern = "(summary|manifest)\\.json$",
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
    compact <- obj[setdiff(names(obj), c("columns", "samples", "images", "tiles"))]
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
  if (!is.character(dataset_id) || length(dataset_id) != 1L ||
      !identical(as.character(gen$dataset_id), as.character(dataset_id))) {
    stop("Generation does not authorize the requested dataset.",
         call. = FALSE)
  }
  dataset_id <- as.character(gen$dataset_id)
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
  tracking_id <- .imaging_tracking_id(
    spec$tracking_id %||% NULL, required = FALSE)
  runtime_identity <- .imaging_generation_runtime_identity(spec)

  segmenter <- .imaging_segmenter_spec(spec$segmenter)
  profile <- spec$profile
  if (!is.list(profile)) {
    profile <- list(name = spec$profile_name %||% profile, bin_width = 25L)
  }
  profile <- .imaging_profile_spec(profile)
  profile_name <- profile$name %||% spec$profile_name
  processor <- paste0(segmenter$provider, "_", segmenter$task %||% "default")
  profile_signature <- .generation_profile_signature(spec, profile)

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
  mask_hashes <- .generation_mask_hashes(spec,
    .existing_mask_hashes(resolved, manifest, segmenter))
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
  content_hashes <- get_content_hashes(
    dataset_id, gen$collection_seal, batch_ids)

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
        mask_content_hash = mask_ch,
        runtime_identity = runtime_identity,
        execution_unit = spec$dshpc_unit %||% NULL
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

    image_uri <- .resolve_sample_image(image_root, sid, backend = backend,
      snapshot = resolved$collection_snapshot)
    if (is.null(image_uri)) {
      complete_item_atomic(generation_id, sid, "failed",
        error = "Image file not found")
      next
    }
    job_token <- .item_job_token(generation_id, sid)
    image_path <- .stage_image_for_job(image_uri, sid, dataset_id, backend,
      image_root = image_root, snapshot = resolved$collection_snapshot)

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

    extract_config <- profile
    extract_config$image <- image_path
    extract_config$sample_id <- sid
    extract_config$generation_id <- generation_id
    extract_config$settings_file <- settings_path %||% "default"
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
      actual_mask_hash <- tryCatch(digest::digest(
        file = extract_config$mask, algo = "sha256"),
        error = function(e) NULL)
      if (is.null(actual_mask_hash) ||
          !identical(actual_mask_hash, mask_hashes[[sid]])) {
        complete_item_atomic(generation_id, sid, "failed",
          error = "Mask integrity verification failed")
        next
      }
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
      tags = .per_image_job_tags(dataset_id, job_token, generation_id),
      visibility = "private", steps = steps,
      .owner = gen$owner_id)

    tryCatch({
      spec_enc <- .dsr_encode(job_spec)
      dsHPC::hpcSubmitInternal(spec_enc,
        unit_selection = spec$dshpc_unit %||% NULL,
        tracking_id = tracking_id)
      record_item_status(
        generation_id, sid, "running", job_token = job_token)
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
