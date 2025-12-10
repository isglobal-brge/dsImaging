#' Modular Functions for Step-by-Step Workflow Control
#'
#' These functions expose individual steps of the imaging pipeline,
#' allowing users to have fine-grained control over the workflow.
#' For most use cases, use RadiomicsDS() which handles the full pipeline.


#' Sync Vault Images to HPC Storage
#'
#' Server-side AGGREGATE function to synchronize medical images from a vault
#' collection to HPC storage. Returns only aggregate counts for disclosure
#' control - no individual file information or hashes are returned to the client.
#'
#' @param collection A DSVaultCollection object (from dsVault package).
#' @param hpc_unit An HPC API configuration (from dsHPC::create_api_config).
#' @param verbose Print progress messages (default: FALSE).
#'
#' @return A list with aggregate sync status (safe for disclosure):
#'   - total: Total files in vault
#'   - existing: Files already in HPC
#'   - uploaded: Files uploaded this run
#'   - failed: Files that failed to upload
#'   - success: TRUE if no failures, FALSE otherwise
#'
#' @details
#' This is an AGGREGATE function - it returns only safe aggregate counts to
#' the client. Individual file names, hashes, and error details stay on the
#' server for disclosure control.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # From client side:
#' status <- ds.image.sync(
#'   collection.resource = "project.vault",
#'   hpc.resource = "project.hpc"
#' )
#' status$total     # Total files processed
#' status$uploaded  # How many were uploaded
#' status$failed    # How many failed
#' status$success   # TRUE if all succeeded
#' }
ImageSyncDS <- function(collection, hpc_unit, verbose = FALSE) {
  # Validate collection
  if (!inherits(collection, "DSVaultCollection")) {
    stop("collection must be a DSVaultCollection object", call. = FALSE)
  }

  # Get collection hashes
  collection_hashes <- collection$list_hashes()

  if (nrow(collection_hashes) == 0) {
    return(list(
      total = 0L,
      existing = 0L,
      uploaded = 0L,
      failed = 0L,
      success = TRUE
    ))
  }

  # Disclosure check
  check_image_disclosure(nrow(collection_hashes), verbose = verbose)

  # Sync to HPC (reuse existing internal function)
  sync_result <- sync_to_hpc(collection, hpc_unit, verbose = verbose)

  # Return ONLY safe aggregate counts - no hashes, no filenames
  list(
    total = as.integer(sync_result$total),
    existing = as.integer(sync_result$existing),
    uploaded = as.integer(sync_result$uploaded),
    failed = as.integer(sync_result$failed),
    success = sync_result$failed == 0
  )
}


#' Run Lungmask Segmentation
#'
#' Server-side AGGREGATE function to run lung segmentation on medical images
#' using lungmask. Requires that images have been synced to HPC first via
#' ImageSyncDS. Returns only aggregate counts for disclosure control.
#'
#' @param collection A DSVaultCollection object (from dsVault package).
#' @param hpc_unit An HPC API configuration (from dsHPC::create_api_config).
#' @param model Lungmask model to use. Options: "R231" (default),
#'   "R231CovidWeb", "LTRCLobes", "LTRCLobes_R231".
#' @param force_cpu Force CPU execution (default: FALSE). When FALSE, GPU is
#'   used if available, with automatic fallback to CPU if not.
#' @param timeout Maximum wait time in seconds (default: 600).
#' @param polling_interval Polling interval in seconds (default: 10).
#' @param verbose Print progress messages (default: FALSE).
#'
#' @return A list with aggregate status (safe for disclosure):
#'   - total: Total images processed
#'   - completed: Successfully processed
#'   - failed: Failed to process
#'   - success: TRUE if no failures, FALSE otherwise
#'
#' @details
#' This is an AGGREGATE function - it returns only safe aggregate counts to
#' the client. Individual file names, hashes, and mask results stay on the
#' server for disclosure control.
#'
#' The function internally syncs images if needed, then runs lungmask on all
#' images in the collection.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # From client side:
#' status <- ds.lungmask(
#'   collection.resource = "project.vault",
#'   hpc.resource = "project.hpc",
#'   model = "R231"
#' )
#' status$total      # Total images
#' status$completed  # Successful
#' status$failed     # Failed
#' status$success    # TRUE if all succeeded
#' }
LungmaskDS <- function(collection,
                       hpc_unit,
                       model = "R231",
                       force_cpu = FALSE,
                       timeout = 600,
                       polling_interval = 10,
                       verbose = FALSE) {

  # Validate collection
  if (!inherits(collection, "DSVaultCollection")) {
    stop("collection must be a DSVaultCollection object", call. = FALSE)
  }

  # Check HPC has lungmask method
  methods_check <- dsHPC::has_methods(hpc_unit, "lungmask")
  if (!methods_check$all_available) {
    stop("HPC does not have 'lungmask' method available", call. = FALSE)
  }

  # Get collection hashes (stays on server)
  collection_hashes <- collection$list_hashes()

  if (nrow(collection_hashes) == 0) {
    return(list(
      total = 0L,
      completed = 0L,
      failed = 0L,
      success = TRUE
    ))
  }

  # Disclosure check
  check_image_disclosure(nrow(collection_hashes), verbose = verbose)

  # Sync to HPC first (internal, no return to client)
  sync_to_hpc(collection, hpc_unit, verbose = verbose)

  # Submit lungmask pipelines
  submission_results <- submit_lungmask_batch(
    hpc_unit = hpc_unit,
    image_hashes = collection_hashes$hash_sha256,
    filenames = collection_hashes$name,
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

  if (n_submitted == 0) {
    return(list(
      total = as.integer(nrow(collection_hashes)),
      completed = 0L,
      failed = as.integer(nrow(collection_hashes)),
      success = FALSE
    ))
  }

  # Wait for completion
  tracker$wait(timeout = timeout, interval = polling_interval, verbose = verbose)

  # Get final counts
  counts <- tracker$status_counts()
  n_completed <- get_count("completed")
  n_failed <- get_count("failed") + get_count("error") + get_count("submit_failed")

  # Return ONLY safe aggregate counts
  list(
    total = as.integer(nrow(collection_hashes)),
    completed = as.integer(n_completed),
    failed = as.integer(n_failed),
    success = n_failed == 0
  )
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
