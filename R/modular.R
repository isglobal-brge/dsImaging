#' Modular Functions for Step-by-Step Workflow Control
#'
#' These functions expose individual steps of the imaging pipeline,
#' allowing users to have fine-grained control over the workflow.
#' For most use cases, use RadiomicsDS() which handles the full pipeline.


#' Sync Vault Images to HPC Storage
#'
#' Server-side assign function to synchronize medical images from a vault
#' collection to HPC storage. This is typically the first step before
#' running any processing pipelines.
#'
#' @param collection A DSVaultCollection object (from dsVault package).
#' @param hpc_unit An HPC API configuration (from dsHPC::create_api_config).
#' @param verbose Print progress messages (default: TRUE).
#'
#' @return A list with sync results:
#'   - total: Total files in vault
#'   - existing: Files already in HPC
#'   - uploaded: Files uploaded this run
#'   - failed: Files that failed to upload
#'   - hashes: Data frame with file names and their SHA-256 hashes
#'
#' @export
#'
#' @examples
#' \dontrun{
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
#' # Sync images to HPC
#' sync_result <- ImageSyncDS(collection, hpc_unit)
#' sync_result$uploaded  # How many were uploaded
#' sync_result$hashes    # Data frame with file info
#'
#' # Then use the hashes for lungmask or other processing
#' }
ImageSyncDS <- function(collection, hpc_unit, verbose = TRUE) {
  # Validate collection
  if (!inherits(collection, "DSVaultCollection")) {
    stop("collection must be a DSVaultCollection object", call. = FALSE)
  }

  # Get collection hashes
  if (verbose) message("Checking collection files...")
  collection_hashes <- collection$list_hashes()

  if (nrow(collection_hashes) == 0) {
    if (verbose) message("No files in collection.")
    return(list(
      total = 0,
      existing = 0,
      uploaded = 0,
      failed = 0,
      hashes = data.frame(
        name = character(),
        hash_sha256 = character(),
        stringsAsFactors = FALSE
      )
    ))
  }

  # Disclosure check
  check_image_disclosure(nrow(collection_hashes), verbose = verbose)

  if (verbose) {
    message(sprintf("Found %d files in collection '%s'",
                    nrow(collection_hashes), collection$collection))
  }

  # Sync to HPC (reuse existing internal function)
  sync_result <- sync_to_hpc(collection, hpc_unit, verbose = verbose)

  # Return result with hashes for subsequent operations
  list(
    total = sync_result$total,
    existing = sync_result$existing,
    uploaded = sync_result$uploaded,
    failed = sync_result$failed,
    hashes = collection_hashes
  )
}


#' Run Lungmask Segmentation
#'
#' Server-side assign function to run lung segmentation on medical images
#' using lungmask. Images must already be available in HPC storage
#' (use ImageSyncDS first).
#'
#' @param hpc_unit An HPC API configuration (from dsHPC::create_api_config).
#' @param image_hashes Character vector of SHA-256 hashes for images to process.
#' @param filenames Optional character vector of filenames (for tracking).
#' @param model Lungmask model to use. Options: "R231" (default),
#'   "R231CovidWeb", "LTRCLobes", "LTRCLobes_R231".
#' @param force_cpu Force CPU execution (default: TRUE).
#' @param wait If TRUE (default), wait for all jobs to complete and return
#'   mask hashes. If FALSE, return job tracker immediately.
#' @param timeout Maximum wait time in seconds when wait=TRUE (default: 600).
#' @param polling_interval Polling interval in seconds (default: 10).
#' @param on_error How to handle failed jobs: "exclude" (default) or "stop".
#' @param verbose Print progress messages (default: TRUE).
#'
#' @return Depends on `wait` parameter:
#'   - If `wait=TRUE`: Data frame with columns:
#'     - filename: Original filename
#'     - image_hash: SHA-256 hash of input image
#'     - mask_hash: SHA-256 hash of generated mask
#'     - pipeline_hash: Pipeline execution hash
#'   - If `wait=FALSE`: List with job status and tracker object
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # First sync images
#' sync_result <- ImageSyncDS(vault, hpc)
#'
#' # Then run lungmask
#' masks <- LungmaskDS(
#'   hpc_unit,
#'   image_hashes = sync_result$hashes$hash_sha256,
#'   filenames = sync_result$hashes$name
#' )
#'
#' # masks$mask_hash can now be used with pyradiomics
#' }
LungmaskDS <- function(hpc_unit,
                       image_hashes,
                       filenames = NULL,
                       model = "R231",
                       force_cpu = TRUE,
                       wait = TRUE,
                       timeout = 600,
                       polling_interval = 10,
                       on_error = c("exclude", "stop"),
                       verbose = TRUE) {

  on_error <- match.arg(on_error)

  # Validate inputs
  if (!is.character(image_hashes) || length(image_hashes) == 0) {
    stop("image_hashes must be a non-empty character vector", call. = FALSE)
  }

  # Check HPC has lungmask method
  methods_check <- dsHPC::has_methods(hpc_unit, "lungmask")
  if (!methods_check$all_available) {
    stop("HPC does not have 'lungmask' method available", call. = FALSE)
  }

  # Disclosure check
  check_image_disclosure(length(image_hashes), verbose = verbose)

  if (is.null(filenames)) {
    filenames <- rep(NA_character_, length(image_hashes))
  }

  if (verbose) {
    message(sprintf("Submitting %d lungmask jobs (model: %s)...",
                    length(image_hashes), model))
  }

  # Submit lungmask pipelines
  submission_results <- submit_lungmask_batch(
    hpc_unit = hpc_unit,
    image_hashes = image_hashes,
    filenames = filenames,
    model = model,
    force_cpu = force_cpu,
    verbose = verbose
  )

  # Create tracker
  tracker <- DSLungmaskTracker$new(submission_results, hpc_unit)

  # Summary after submit
  counts <- tracker$status_counts()
  get_count <- function(name) {
    val <- counts[name]
    if (is.na(val) || is.null(val)) 0L else as.integer(val)
  }
  n_submitted <- get_count("pending")
  n_failed_submit <- get_count("submit_failed")

  if (verbose) {
    message(sprintf("Submitted: %d, Failed to submit: %d",
                    n_submitted, n_failed_submit))
  }

  if (n_submitted == 0) {
    stop("No lungmask jobs were submitted successfully", call. = FALSE)
  }

  # Wait or return tracker
  if (wait) {
    if (verbose) message("\nWaiting for lungmask results...")
    tracker$wait(timeout = timeout, interval = polling_interval, verbose = verbose)
  } else {
    if (verbose) message("\nChecking initial status...")
    tracker$poll(verbose = verbose)
  }

  # Get current counts
  counts <- tracker$status_counts()
  n_pending <- get_count("pending") + get_count("running")
  n_completed <- get_count("completed")
  n_failed <- get_count("failed") + get_count("error") + get_count("submit_failed")

  # If not waiting and jobs still pending, return status
  if (!wait && n_pending > 0) {
    if (verbose) {
      message(sprintf("\nJobs still running: %d pending, %d completed, %d failed",
                      n_pending, n_completed, n_failed))
      message("Use status$tracker$poll() to check progress")
    }
    return(list(
      total = length(image_hashes),
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
      stop(sprintf("%d lungmask jobs failed:\n%s",
                   n_failed,
                   paste(sprintf("  - %s: %s",
                                 failed_jobs$filename,
                                 failed_jobs$error_message),
                         collapse = "\n")), call. = FALSE)
    } else {
      if (verbose) {
        warning(sprintf("%d jobs failed and will be excluded from results", n_failed))
      }
    }
  }

  # Extract mask results
  masks_df <- tracker$extract_masks(exclude_errors = TRUE)

  if (nrow(masks_df) == 0) {
    warning("No masks extracted from lungmask results")
    return(data.frame())
  }

  # Disclosure check on results
  check_result_disclosure(masks_df, verbose = verbose)

  if (verbose) {
    message(sprintf("\nExtracted %d lung masks", nrow(masks_df)))
  }

  return(masks_df)
}


# ===========================================================================
# Internal: Submit batch of lungmask pipelines
# ===========================================================================

submit_lungmask_batch <- function(hpc_unit,
                                   image_hashes,
                                   filenames,
                                   model,
                                   force_cpu,
                                   verbose) {
  results <- list()

  for (i in seq_along(image_hashes)) {
    image_hash <- image_hashes[i]
    filename <- filenames[i]

    if (verbose) {
      message(sprintf("  [%d/%d] %s",
                      i, length(image_hashes),
                      if (!is.na(filename)) filename else substr(image_hash, 1, 16)))
    }

    tryCatch({
      pipeline <- create_lungmask_pipeline(
        image_hash = image_hash,
        model = model,
        force_cpu = force_cpu
      )

      response <- dsHPC::submit_pipeline(hpc_unit, pipeline)

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


# ===========================================================================
# DSLungmaskTracker: Specialized tracker for lungmask jobs
# ===========================================================================

#' @importFrom R6 R6Class
DSLungmaskTracker <- R6::R6Class(

  classname = "DSLungmaskTracker",

  public = list(
    jobs = NULL,
    hpc_unit = NULL,
    created_at = NULL,

    initialize = function(submission_results, hpc_unit) {
      self$jobs <- submission_results
      self$jobs$status <- ifelse(
        is.na(self$jobs$pipeline_hash),
        "submit_failed",
        "pending"
      )
      self$jobs$result <- vector("list", nrow(self$jobs))
      self$jobs$mask_hash <- NA_character_
      self$jobs$error_message <- NA_character_
      self$jobs$completed_at <- as.POSIXct(NA)

      self$hpc_unit <- hpc_unit
      self$created_at <- Sys.time()
    },

    is_done = function() {
      all(self$jobs$status %in% c("completed", "failed", "submit_failed", "error"))
    },

    status_counts = function() {
      table(self$jobs$status)
    },

    poll = function(verbose = TRUE) {
      pending_idx <- which(self$jobs$status == "pending")

      if (length(pending_idx) == 0) {
        if (verbose) message("No pending jobs to poll.")
        return(self$status_counts())
      }

      if (verbose) {
        message(sprintf("Polling %d pending jobs...", length(pending_idx)))
      }

      for (i in pending_idx) {
        pipeline_hash <- self$jobs$pipeline_hash[i]

        tryCatch({
          result <- dsHPC::get_pipeline_status(self$hpc_unit, pipeline_hash)

          if (result$status == "completed") {
            self$jobs$status[i] <- "completed"
            self$jobs$completed_at[i] <- Sys.time()

            if (!is.null(result$final_output) && nchar(result$final_output) > 0) {
              parsed_output <- tryCatch(
                jsonlite::fromJSON(result$final_output, simplifyVector = FALSE),
                error = function(e) list(error = e$message)
              )
              self$jobs$result[[i]] <- parsed_output

              # Extract mask hash from lungmask output
              mask_hash <- NULL
              if (!is.null(parsed_output$data$mask_hash)) {
                mask_hash <- parsed_output$data$mask_hash
              } else if (!is.null(parsed_output$mask_hash)) {
                mask_hash <- parsed_output$mask_hash
              }
              if (!is.null(mask_hash)) {
                self$jobs$mask_hash[i] <- mask_hash
              }
            }

          } else if (result$status == "failed") {
            self$jobs$status[i] <- "failed"
            self$jobs$error_message[i] <- result$error %||% "Unknown error"
            self$jobs$completed_at[i] <- Sys.time()
          }

        }, error = function(e) {
          self$jobs$status[i] <<- "error"
          self$jobs$error_message[i] <<- e$message
        })
      }

      counts <- self$status_counts()
      if (verbose) {
        get_count <- function(name) {
          val <- counts[name]
          if (is.na(val) || is.null(val)) 0L else as.integer(val)
        }
        message(sprintf("  Completed: %d, Failed: %d, Pending: %d",
                        get_count("completed"),
                        get_count("failed"),
                        get_count("pending")))
      }

      return(counts)
    },

    wait = function(timeout = 600, interval = 10, verbose = TRUE) {
      start_time <- Sys.time()

      while (!self$is_done()) {
        elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
        if (elapsed > timeout) {
          warning(sprintf("Timeout after %.0f seconds. %d jobs still pending.",
                          elapsed, sum(self$jobs$status == "pending")))
          break
        }

        self$poll(verbose = verbose)

        if (!self$is_done()) {
          if (verbose) message(sprintf("Waiting %d seconds...", interval))
          Sys.sleep(interval)
        }
      }

      invisible(self)
    },

    get_completed = function() {
      self$jobs[self$jobs$status == "completed", ]
    },

    get_failed = function() {
      self$jobs[self$jobs$status %in% c("failed", "error", "submit_failed"), ]
    },

    has_errors = function() {
      nrow(self$get_failed()) > 0
    },

    extract_masks = function(exclude_errors = TRUE) {
      completed <- self$get_completed()

      if (nrow(completed) == 0) {
        return(data.frame(
          filename = character(),
          image_hash = character(),
          mask_hash = character(),
          pipeline_hash = character(),
          stringsAsFactors = FALSE
        ))
      }

      data.frame(
        filename = completed$filename,
        image_hash = completed$image_hash,
        mask_hash = completed$mask_hash,
        pipeline_hash = completed$pipeline_hash,
        stringsAsFactors = FALSE
      )
    },

    print = function() {
      counts <- self$status_counts()
      cat("DSLungmaskTracker\n")
      cat(sprintf("  Created: %s\n", format(self$created_at)))
      cat(sprintf("  Total jobs: %d\n", nrow(self$jobs)))
      cat("  Status:\n")
      for (s in names(counts)) {
        cat(sprintf("    - %s: %d\n", s, counts[s]))
      }
      invisible(self)
    }
  )
)

# Null coalescing operator
`%||%` <- function(x, y) if (is.null(x)) y else x
