#' Extract Radiomic Features from Vault Collection
#'
#' Main function to extract radiomic features from medical images stored in a
#' vault collection, processed via HPC. Automatically syncs files to HPC before
#' submitting jobs.
#'
#' @param vault A DSVaultCollection object (from dsVault package).
#' @param hpc_config An HPC API configuration (from dsHPC::create_api_config).
#' @param lungmask_model Lungmask model to use. Options: "R231" (default),
#'   "R231CovidWeb", "LTRCLobes", "LTRCLobes_R231".
#' @param feature_classes Radiomic feature classes to extract. Options:
#'   "firstorder", "shape", "glcm", "glrlm", "glszm", "gldm", "ngtdm".
#'   Default extracts all.
#' @param bin_width Bin width for intensity discretization (default: 25).
#' @param normalize Normalize image before feature extraction (default: FALSE).
#' @param force_cpu Force CPU execution for lungmask (default: TRUE).
#' @param wait If TRUE (default), wait for all jobs to complete and return
#'   features data frame. If FALSE, return job status immediately after submit.
#' @param timeout Maximum wait time in seconds when wait=TRUE (default: 600).
#' @param polling_interval Polling interval in seconds (default: 10).
#' @param on_error How to handle failed jobs. Options:
#'   - "exclude" (default): Exclude failed images from results, return partial table
#'   - "stop": Stop with error if any job fails
#' @param verbose Print progress messages (default: TRUE).
#'
#' @return Depends on `wait` parameter:
#'   - If `wait=TRUE`: Data frame with radiomic features (columns: filename,
#'     image_hash, pipeline_hash, and feature columns)
#'   - If `wait=FALSE`: List with job status:
#'     - total: Total jobs submitted
#'     - pending: Jobs still running/pending
#'     - completed: Jobs completed successfully
#'     - failed: Jobs that failed
#'     - tracker: DSJobTracker object for manual polling
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Create connections
#' vault <- dsVault::DSVaultCollection$new(
#'   endpoint = "http://localhost:8000",
#'   collection = "ct-scans",
#'   api_key = "your-vault-key"
#' )
#' hpc <- dsHPC::create_api_config(
#'   base_url = "http://localhost",
#'   port = 8001,
#'   api_key = "your-hpc-key"
#' )
#'
#' # Full extraction (wait for results)
#' features <- RadiomicsDS(vault, hpc)
#'
#' # Submit only, check status later
#' status <- RadiomicsDS(vault, hpc, wait = FALSE)
#' status$pending   # How many still running
#' status$completed # How many done
#'
#' # Later: poll and get results
#' status$tracker$poll()
#' if (status$tracker$is_done()) {
#'   features <- status$tracker$extract_features()
#' }
#'
#' # With custom parameters
#' features <- RadiomicsDS(
#'   vault, hpc,
#'   lungmask_model = "R231CovidWeb",
#'   feature_classes = c("firstorder", "shape"),
#'   bin_width = 32,
#'   on_error = "stop"  # Fail if any image fails
#' )
#' }
RadiomicsDS <- function(vault,
                          hpc_config,
                          lungmask_model = "R231",
                          feature_classes = c("firstorder", "shape", "glcm",
                                              "glrlm", "glszm", "gldm", "ngtdm"),
                          bin_width = 25,
                          normalize = FALSE,
                          force_cpu = TRUE,
                          wait = TRUE,
                          timeout = 600,
                          polling_interval = 10,
                          on_error = c("exclude", "stop"),
                          verbose = TRUE) {


  on_error <- match.arg(on_error)

  # Step 0: Validate inputs (vault, image formats, HPC methods)
  validation <- validate_radiomics_inputs(vault, hpc_config, verbose = verbose)
  vault_hashes <- validation$vault_hashes

  # Disclosure check: ensure image count meets threshold before processing
  check_image_disclosure(nrow(vault_hashes), verbose = verbose)

  if (verbose) {
    message(sprintf("\nProcessing %d valid images from collection '%s'...",
                    nrow(vault_hashes), vault$collection))
  }

  # Step 1: Sync vault files to HPC
  if (verbose) message("\n[1/3] Syncing vault files to HPC...")
  sync_result <- sync_to_hpc(vault, hpc_config, verbose = verbose)

  if (sync_result$failed > 0) {
    warning(sprintf("%d files failed to sync to HPC", sync_result$failed))
  }

  # Convert feature_classes to comma-separated string if vector
  if (is.character(feature_classes) && length(feature_classes) > 1) {
    feature_classes <- paste(feature_classes, collapse = ",")
  }

  # Step 2: Submit all pipelines
  if (verbose) message("\n[2/3] Submitting pipelines...")
  submission_results <- submit_batch_pipelines(
    hpc_config = hpc_config,
    image_hashes = vault_hashes$hash_sha256,
    filenames = vault_hashes$name,
    lungmask_model = lungmask_model,
    feature_classes = feature_classes,
    bin_width = bin_width,
    normalize = normalize,
    force_cpu = force_cpu,
    verbose = verbose
  )

  # Create job tracker
  tracker <- DSJobTracker$new(submission_results, hpc_config)

  # Summary after submit
  counts <- tracker$status_counts()
  get_count_init <- function(name) {
    val <- counts[name]
    if (is.na(val) || is.null(val)) 0L else as.integer(val)
  }
  n_submitted <- get_count_init("pending")
  n_failed_submit <- get_count_init("submit_failed")

  if (verbose) {
    message(sprintf("\nSubmitted: %d, Failed to submit: %d", n_submitted, n_failed_submit))
  }

  if (n_submitted == 0) {
    stop("No pipelines were submitted successfully")
  }

  # Step 3: Wait or poll once
  if (wait) {
    if (verbose) message("\n[3/3] Waiting for pipeline results...")
    tracker$wait(timeout = timeout, interval = polling_interval, verbose = verbose)
  } else {
    # Poll once to get current status
    if (verbose) message("\n[3/3] Checking job status...")
    tracker$poll(verbose = verbose)
  }

  # Get current counts (handle NA from table())
  counts <- tracker$status_counts()
  get_count <- function(name) {
    val <- counts[name]
    if (is.na(val) || is.null(val)) 0L else as.integer(val)
  }
  n_pending <- get_count("pending") + get_count("running")
  n_completed <- get_count("completed")
  n_failed <- get_count("failed") + get_count("error") + get_count("submit_failed")

  # If not waiting and jobs still pending, return status
  if (!wait && n_pending > 0) {
    if (verbose) {
      message(sprintf("\nJobs still running: %d pending, %d completed, %d failed",
                      n_pending, n_completed, n_failed))
      message("Use status$tracker$poll() to check progress, or call again with wait=TRUE")
    }
    return(list(
      total = nrow(vault_hashes),
      pending = as.integer(n_pending),
      completed = as.integer(n_completed),
      failed = as.integer(n_failed),
      tracker = tracker
    ))
  }

  # Check for errors
  if (tracker$has_errors()) {
    failed_jobs <- tracker$get_failed()
    n_failed <- nrow(failed_jobs)

    if (on_error == "stop") {
      stop(sprintf("%d jobs failed:\n%s",
                   n_failed,
                   paste(sprintf("  - %s: %s",
                                 failed_jobs$filename,
                                 failed_jobs$error_message),
                         collapse = "\n")))
    } else {
      # on_error == "exclude"
      if (verbose) {
        warning(sprintf("%d jobs failed and will be excluded from results", n_failed))
      }
    }
  }

  # Extract features
  features_df <- tracker$extract_features(exclude_errors = TRUE)

  if (nrow(features_df) == 0) {
    warning("No features extracted from pipeline results")
    return(data.frame())
  }

  # Disclosure check: ensure result row count meets threshold before returning
  check_result_disclosure(features_df, verbose = verbose)

  if (verbose) {
    id_cols <- c("filename", "image_hash", "pipeline_hash")
    feature_cols <- setdiff(names(features_df), id_cols)
    message(sprintf("\nExtracted %d features from %d images",
                    length(feature_cols), nrow(features_df)))
  }

  return(features_df)
}


# Internal: Submit batch of pipelines
submit_batch_pipelines <- function(hpc_config,
                                    image_hashes,
                                    filenames = NULL,
                                    lungmask_model = "R231",
                                    feature_classes = "firstorder,shape,glcm,glrlm,glszm,gldm,ngtdm",
                                    bin_width = 25,
                                    normalize = FALSE,
                                    force_cpu = TRUE,
                                    verbose = TRUE) {
  results <- list()

  for (i in seq_along(image_hashes)) {
    image_hash <- image_hashes[i]
    filename <- if (!is.null(filenames)) filenames[i] else NA_character_

    if (verbose) {
      message(sprintf("  [%d/%d] %s",
                      i, length(image_hashes),
                      if (!is.na(filename)) filename else substr(image_hash, 1, 16)))
    }

    tryCatch({
      pipeline <- create_lungmask_pyradiomics_pipeline(
        image_hash = image_hash,
        lungmask_model = lungmask_model,
        feature_classes = feature_classes,
        bin_width = bin_width,
        normalize = normalize,
        force_cpu = force_cpu
      )

      response <- dsHPC::submit_pipeline(hpc_config, pipeline)

      results[[length(results) + 1]] <- data.frame(
        image_hash = image_hash,
        filename = filename,
        pipeline_hash = response$pipeline_hash,
        status = "submitted",
        error = NA_character_,
        stringsAsFactors = FALSE
      )

    }, error = function(e) {
      results[[length(results) + 1]] <<- data.frame(
        image_hash = image_hash,
        filename = filename,
        pipeline_hash = NA_character_,
        status = "failed",
        error = e$message,
        stringsAsFactors = FALSE
      )

      if (verbose) {
        message(sprintf("    ERROR: %s", e$message))
      }
    })
  }

  do.call(rbind, results)
}


# Null coalescing operator
`%||%` <- function(x, y) if (is.null(x)) y else x


# ============================================================================
# Internal sync functions
# ============================================================================

# Sync vault files to HPC
sync_to_hpc <- function(vault, hpc_config, verbose = TRUE) {
  # Get vault hashes
  if (verbose) message("Checking vault files...")
  vault_hashes <- vault$list_hashes()

  if (nrow(vault_hashes) == 0) {
    if (verbose) message("No files in vault.")
    return(list(total = 0, existing = 0, missing = 0, uploaded = 0, failed = 0))
  }

  if (verbose) {
    message(sprintf("Found %d files in vault", nrow(vault_hashes)))
  }

  # Check which exist in HPC
  if (verbose) message("Checking HPC file storage...")
  check_result <- check_hpc_hashes(hpc_config, vault_hashes$hash_sha256)

  n_existing <- length(check_result$existing)
  n_missing <- length(check_result$missing)

  if (verbose) {
    message(sprintf("  Already in HPC: %d", n_existing))
    message(sprintf("  Missing in HPC: %d", n_missing))
  }

  # Nothing to upload
  if (n_missing == 0) {
    if (verbose) message("All files already available in HPC.")
    return(list(
      total = nrow(vault_hashes),
      existing = n_existing,
      missing = 0,
      uploaded = 0,
      failed = 0
    ))
  }

  # Upload missing files
  if (verbose) message("\nUploading missing files...")
  upload_result <- upload_vault_to_hpc(
    vault = vault,
    hpc_config = hpc_config,
    vault_hashes = vault_hashes,
    missing_hashes = check_result$missing,
    verbose = verbose
  )

  if (verbose) {
    message(sprintf("\nSync complete: %d uploaded, %d failed",
                    upload_result$uploaded, upload_result$failed))
  }

  list(
    total = nrow(vault_hashes),
    existing = n_existing,
    missing = n_missing,
    uploaded = upload_result$uploaded,
    failed = upload_result$failed
  )
}


# Check which hashes exist in HPC
check_hpc_hashes <- function(hpc_config, hashes) {
  if (length(hashes) == 0) {
    return(list(existing = character(), missing = character()))
  }

  result <- dsHPC::check_existing_hashes(hpc_config, unique(hashes))

  list(
    existing = result$existing_hashes %||% character(),
    missing = result$missing_hashes %||% character()
  )
}


# Upload missing files from vault to HPC
upload_vault_to_hpc <- function(vault, hpc_config, vault_hashes, missing_hashes,
                                 verbose = TRUE) {
  if (length(missing_hashes) == 0) {
    return(list(uploaded = 0, failed = 0, errors = list()))
  }

  # Filter to only missing files
  missing_files <- vault_hashes[vault_hashes$hash_sha256 %in% missing_hashes, ]

  uploaded <- 0
  failed <- 0
  errors <- list()

  for (i in seq_len(nrow(missing_files))) {
    row <- missing_files[i, ]
    filename <- row$name
    expected_hash <- row$hash_sha256

    if (verbose) {
      message(sprintf("  [%d/%d] Uploading %s...",
                      i, nrow(missing_files), filename))
    }

    tryCatch({
      # Download from vault (in memory)
      content <- vault$download(filename)

      # Upload to HPC
      result_hash <- dsHPC::upload_file(hpc_config, content, filename)

      # Verify hash
      if (result_hash != expected_hash) {
        msg <- sprintf("Hash mismatch: expected %s, got %s",
                       substr(expected_hash, 1, 16),
                       substr(result_hash, 1, 16))
        warning(msg)
        failed <- failed + 1
        errors[[filename]] <- msg
      } else {
        uploaded <- uploaded + 1
      }

    }, error = function(e) {
      if (verbose) message(sprintf("    ERROR: %s", e$message))
      failed <<- failed + 1
      errors[[filename]] <<- e$message
    })
  }

  list(uploaded = uploaded, failed = failed, errors = errors)
}
