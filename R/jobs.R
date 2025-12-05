#' Job Tracker for Pipeline Submissions
#'
#' R6 class to track submitted pipeline jobs and their status.
#'
#' @export
#' @importFrom R6 R6Class
DSJobTracker <- R6::R6Class(

  classname = "DSJobTracker",

  public = list(
    #' @field jobs Data frame tracking all submitted jobs.
    jobs = NULL,

    #' @field hpc_config HPC API configuration for polling.
    hpc_config = NULL,

    #' @field created_at Timestamp when tracker was created.
    created_at = NULL,

    #' @description
    #' Create a new job tracker.
    #'
    #' @param submission_results Data frame from pipeline submission.
    #' @param hpc_config HPC API configuration.
    #'
    #' @return A new `DSJobTracker` object.
    initialize = function(submission_results, hpc_config) {
      self$jobs <- submission_results
      self$jobs$status <- ifelse(
        is.na(self$jobs$pipeline_hash),
        "submit_failed",
        "pending"
      )
      self$jobs$result <- vector("list", nrow(self$jobs))
      self$jobs$error_message <- NA_character_
      self$jobs$completed_at <- as.POSIXct(NA)

      self$hpc_config <- hpc_config
      self$created_at <- Sys.time()
    },

    #' @description
    #' Check if all jobs are done (completed, failed, or submit_failed).
    #'
    #' @return TRUE if all jobs are done, FALSE otherwise.
    is_done = function() {
      all(self$jobs$status %in% c("completed", "failed", "submit_failed", "error"))
    },

    #' @description
    #' Get count of jobs by status.
    #'
    #' @return Named vector with counts per status.
    status_counts = function() {
      table(self$jobs$status)
    },

    #' @description
    #' Poll HPC for job status updates.
    #'
    #' @param verbose Print progress (default: TRUE).
    #'
    #' @return Updated status counts.
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
          result <- dsHPC::get_pipeline_status(self$hpc_config, pipeline_hash)

          if (result$status == "completed") {
            self$jobs$status[i] <- "completed"
            self$jobs$completed_at[i] <- Sys.time()

            # final_output is already in status response as JSON string
            if (!is.null(result$final_output) && nchar(result$final_output) > 0) {
              parsed_output <- tryCatch(
                jsonlite::fromJSON(result$final_output, simplifyVector = FALSE),
                error = function(e) list(error = e$message)
              )
              self$jobs$result[[i]] <- parsed_output
            } else {
              self$jobs$result[[i]] <- list()
            }

          } else if (result$status == "failed") {
            self$jobs$status[i] <- "failed"
            self$jobs$error_message[i] <- result$error %||% "Unknown error"
            self$jobs$completed_at[i] <- Sys.time()

          }
          # else: still pending, do nothing

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

    #' @description
    #' Wait for all jobs to complete with polling.
    #'
    #' @param timeout Maximum wait time in seconds (default: 600).
    #' @param interval Polling interval in seconds (default: 10).
    #' @param verbose Print progress (default: TRUE).
    #'
    #' @return Self (for chaining).
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

    #' @description
    #' Get jobs that completed successfully.
    #'
    #' @return Data frame of completed jobs.
    get_completed = function() {
      self$jobs[self$jobs$status == "completed", ]
    },

    #' @description
    #' Get jobs that failed or had errors.
    #'
    #' @return Data frame of failed jobs.
    get_failed = function() {
      self$jobs[self$jobs$status %in% c("failed", "error", "submit_failed"), ]
    },

    #' @description
    #' Check if there are any errors and optionally stop.
    #'
    #' @param stop_on_error If TRUE, throws error when failures exist (default: FALSE).
    #'
    #' @return TRUE if errors exist, FALSE otherwise.
    has_errors = function(stop_on_error = FALSE) {
      failed <- self$get_failed()
      has_err <- nrow(failed) > 0

      if (has_err && stop_on_error) {
        stop(sprintf("%d jobs failed:\n%s",
                     nrow(failed),
                     paste(sprintf("  - %s: %s",
                                   failed$filename,
                                   failed$error_message),
                           collapse = "\n")))
      }

      return(has_err)
    },

    #' @description
    #' Extract features from completed jobs.
    #'
    #' @param exclude_errors If TRUE, silently exclude failed jobs (default: TRUE).
    #'   If FALSE and there are errors, throws a warning.
    #'
    #' @return Data frame with radiomic features.
    extract_features = function(exclude_errors = TRUE) {
      if (!exclude_errors && self$has_errors()) {
        warning(sprintf("%d jobs failed and will be excluded from results",
                        nrow(self$get_failed())))
      }

      completed <- self$get_completed()

      if (nrow(completed) == 0) {
        warning("No completed jobs to extract features from")
        return(data.frame())
      }

      features_list <- list()

      for (i in seq_len(nrow(completed))) {
        output <- completed$result[[i]]

        # Try different paths where features might be stored
        features <- NULL
        if (!is.null(output$data) && !is.null(output$data$all_features)) {
          features <- output$data$all_features
        } else if (!is.null(output$all_features)) {
          features <- output$all_features
        } else if (!is.null(output$data) && !is.null(output$data$features)) {
          features <- output$data$features
        }

        if (!is.null(features)) {
          if (is.list(features) && !is.data.frame(features)) {
            features_df <- as.data.frame(features, stringsAsFactors = FALSE)
          } else if (is.data.frame(features)) {
            features_df <- features
          } else {
            features_df <- as.data.frame(t(features), stringsAsFactors = FALSE)
          }

          features_df$pipeline_hash <- completed$pipeline_hash[i]
          features_df$image_hash <- completed$image_hash[i]
          features_df$filename <- completed$filename[i]

          features_list[[length(features_list) + 1]] <- features_df
        }
      }

      if (length(features_list) == 0) {
        warning("No features found in completed job results")
        return(data.frame())
      }

      # Combine all features
      all_cols <- unique(unlist(lapply(features_list, names)))
      features_list <- lapply(features_list, function(df) {
        missing_cols <- setdiff(all_cols, names(df))
        for (col in missing_cols) {
          df[[col]] <- NA
        }
        df[, all_cols, drop = FALSE]
      })

      result <- do.call(rbind, features_list)

      # Reorder columns
      id_cols <- c("filename", "image_hash", "pipeline_hash")
      feature_cols <- setdiff(names(result), id_cols)
      result <- result[, c(id_cols, sort(feature_cols)), drop = FALSE]

      # Convert feature columns to numeric
      for (col in feature_cols) {
        result[[col]] <- as.numeric(result[[col]])
      }

      return(result)
    },

    #' @description
    #' Print method for DSJobTracker.
    print = function() {
      counts <- self$status_counts()
      cat("DSJobTracker\n")
      cat(sprintf("  Created: %s\n", format(self$created_at)))
      cat(sprintf("  Total jobs: %d\n", nrow(self$jobs)))
      cat("  Status:\n")
      for (s in names(counts)) {
        cat(sprintf("    - %s: %d\n", s, counts[s]))
      }
      if (self$is_done()) {
        cat("  [All jobs done]\n")
      } else {
        cat("  [Jobs still pending - use $poll() or $wait()]\n")
      }
      invisible(self)
    }
  )
)


# Null coalescing operator (internal)
`%||%` <- function(x, y) if (is.null(x)) y else x
