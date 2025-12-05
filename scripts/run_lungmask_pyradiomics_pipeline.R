#!/usr/bin/env Rscript
#
# run_lungmask_pyradiomics_pipeline.R
#
# Creates and submits pipelines for processing CT images through:
#   1. lungmask - Lung segmentation
#   2. pyradiomics - Radiomic feature extraction
#
# Uses dsVault to get file hashes and dsHPC to submit pipelines.
#

library(dsVault)
library(dsHPC)

# ============================================================================
# Configuration
# ============================================================================

# Vault configuration
VAULT_ENDPOINT <- "http://localhost:8000"
VAULT_COLLECTION <- "images"
VAULT_API_KEY <- "0bbbdae3a5a82c7944f5010083aaa227ba8f337ddd8c8c34c224723cebc608db"

# HPC configuration
HPC_BASE_URL <- "http://localhost"
HPC_PORT <- 8001
HPC_API_KEY <- "lXCXTxsK6JK8aeGSAZkAI8FGLYug8H9u"

# Pipeline parameters
LUNGMASK_MODEL <- "R231"
PYRADIOMICS_FEATURES <- "firstorder,shape,glcm,glrlm,glszm,gldm,ngtdm"
PYRADIOMICS_BIN_WIDTH <- 25

# ============================================================================
# Core Functions
# ============================================================================

#' Create a lungmask -> pyradiomics pipeline for a single image
#'
#' @param image_hash SHA-256 hash of the CT image
#' @param lungmask_model Lungmask model to use (default: "R231")
#' @param feature_classes Comma-separated feature classes for pyradiomics
#' @param bin_width Bin width for intensity discretization
#'
#' @return Pipeline definition list ready for submission
#'
#' @examples
#' \dontrun{
#' pipeline <- create_lungmask_pyradiomics_pipeline("abc123...")
#' result <- execute_pipeline(config, pipeline)
#' }
create_lungmask_pyradiomics_pipeline <- function(
  image_hash,
  lungmask_model = "R231",
  feature_classes = "firstorder,shape,glcm,glrlm,glszm,gldm,ngtdm",
  bin_width = 25
) {
  # Single node with a meta-job chain: lungmask -> pyradiomics
  #
  # Step 1: lungmask - receives CT image, outputs JSON with mask_base64
  # Step 2: pyradiomics - receives CT image as input_file, mask from $ref:prev
  #
  # Using $ref:prev/data/mask_base64 in file_inputs extracts the mask_base64
  # value from lungmask output and injects it as parameter for pyradiomics
  processing_node <- create_pipeline_node(
    chain = list(
      # Step 1: Lungmask segmentation
      list(
        method_name = "lungmask",
        parameters = list(
          model = lungmask_model,
          force_cpu = TRUE
        )
      ),
      # Step 2: PyRadiomics feature extraction
      # file_inputs: mask file is extracted from prev step's data.mask_base64
      # pyradiomics will read the file, detect base64 content, and decode it
      list(
        method_name = "pyradiomics",
        parameters = list(
          feature_classes = feature_classes,
          bin_width = bin_width,
          normalize = FALSE
        ),
        file_inputs = list(
          image = "$ref:initial",
          mask = "$ref:prev/data/mask_base64"
        )
      )
    ),
    dependencies = character(0),
    input_file_hash = image_hash
  )

  # Create pipeline definition
  pipeline <- list(
    nodes = list(
      processing = processing_node
    )
  )

  return(pipeline)
}


#' Submit pipelines for multiple images
#'
#' @param hpc_config dsHPC API configuration
#' @param image_hashes Character vector of image SHA-256 hashes
#' @param filenames Optional character vector of filenames (same length as image_hashes)
#' @param lungmask_model Lungmask model to use
#' @param feature_classes Feature classes for pyradiomics
#' @param bin_width Bin width for pyradiomics
#' @param verbose Print progress messages
#'
#' @return Data frame with columns: image_hash, filename, pipeline_hash, status, error
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 8001, "api_key",
#'                             auth_header = "X-API-Key", auth_prefix = "")
#' results <- submit_batch_pipelines(config, c("hash1", "hash2", "hash3"))
#' }
submit_batch_pipelines <- function(
  hpc_config,
  image_hashes,
  filenames = NULL,
  lungmask_model = "R231",
  feature_classes = "firstorder,shape,glcm,glrlm,glszm,gldm,ngtdm",
  bin_width = 25,
  verbose = TRUE
) {
  results <- list()

  for (i in seq_along(image_hashes)) {
    image_hash <- image_hashes[i]
    filename <- if (!is.null(filenames)) filenames[i] else NA_character_

    if (verbose) {
      message(sprintf("[%d/%d] Submitting pipeline for %s...",
                      i, length(image_hashes), substr(image_hash, 1, 16)))
    }

    tryCatch({
      # Create pipeline
      pipeline <- create_lungmask_pyradiomics_pipeline(
        image_hash = image_hash,
        lungmask_model = lungmask_model,
        feature_classes = feature_classes,
        bin_width = bin_width
      )

      # Submit pipeline
      response <- submit_pipeline(hpc_config, pipeline)

      results[[length(results) + 1]] <- data.frame(
        image_hash = image_hash,
        filename = filename,
        pipeline_hash = response$pipeline_hash,
        status = "submitted",
        error = NA_character_,
        stringsAsFactors = FALSE
      )

      if (verbose) {
        message(sprintf("  -> Pipeline: %s", response$pipeline_hash))
      }

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
        message(sprintf("  -> ERROR: %s", e$message))
      }
    })
  }

  do.call(rbind, results)
}


#' Wait for multiple pipelines to complete
#'
#' @param hpc_config dsHPC API configuration
#' @param pipeline_hashes Character vector of pipeline hashes
#' @param timeout Maximum time to wait per pipeline in seconds
#' @param interval Polling interval in seconds
#' @param verbose Print progress messages
#'
#' @return List of results (one per pipeline)
wait_for_batch_pipelines <- function(
  hpc_config,
  pipeline_hashes,
  timeout = 600,
  interval = 10,
  verbose = TRUE
) {
  results <- list()

  for (i in seq_along(pipeline_hashes)) {
    pipeline_hash <- pipeline_hashes[i]

    if (is.na(pipeline_hash)) {
      results[[i]] <- list(
        pipeline_hash = NA,
        status = "skipped",
        error = "Pipeline was not submitted"
      )
      next
    }

    if (verbose) {
      message(sprintf("\n[%d/%d] Waiting for pipeline %s...",
                      i, length(pipeline_hashes), substr(pipeline_hash, 1, 16)))
    }

    tryCatch({
      result <- wait_for_pipeline(
        hpc_config,
        pipeline_hash,
        timeout = timeout,
        interval = interval,
        verbose = verbose,
        parse_json = TRUE
      )

      results[[i]] <- list(
        pipeline_hash = pipeline_hash,
        status = result$status,
        final_output = result$final_output,
        error = NULL
      )

    }, error = function(e) {
      results[[i]] <<- list(
        pipeline_hash = pipeline_hash,
        status = "error",
        final_output = NULL,
        error = e$message
      )

      if (verbose) {
        message(sprintf("  -> ERROR: %s", e$message))
      }
    })
  }

  names(results) <- pipeline_hashes
  return(results)
}


#' Extract radiomic features from pipeline results
#'
#' @param pipeline_results List of pipeline results from wait_for_batch_pipelines
#' @param submission_results Data frame with image_hash and pipeline_hash mapping
#'
#' @return Data frame with all features, one row per image
extract_features_from_results <- function(pipeline_results, submission_results = NULL) {
  features_list <- list()

  for (pipeline_hash in names(pipeline_results)) {
    result <- pipeline_results[[pipeline_hash]]

    if (result$status == "completed" && !is.null(result$final_output)) {
      output <- result$final_output

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
        # Convert to data frame (handle both list and named vector)
        if (is.list(features) && !is.data.frame(features)) {
          features_df <- as.data.frame(features, stringsAsFactors = FALSE)
        } else if (is.data.frame(features)) {
          features_df <- features
        } else {
          # Named vector
          features_df <- as.data.frame(t(features), stringsAsFactors = FALSE)
        }

        features_df$pipeline_hash <- pipeline_hash

        # Add image_hash and filename if we have the mapping
        if (!is.null(submission_results)) {
          idx <- which(submission_results$pipeline_hash == pipeline_hash)
          if (length(idx) > 0) {
            features_df$image_hash <- submission_results$image_hash[idx[1]]
            features_df$filename <- submission_results$filename[idx[1]]
          }
        }

        features_list[[length(features_list) + 1]] <- features_df
      }
    }
  }

  if (length(features_list) > 0) {
    # Combine all features, handling different column sets
    all_cols <- unique(unlist(lapply(features_list, names)))

    # Ensure all data frames have all columns
    features_list <- lapply(features_list, function(df) {
      missing_cols <- setdiff(all_cols, names(df))
      for (col in missing_cols) {
        df[[col]] <- NA
      }
      df[, all_cols, drop = FALSE]
    })

    do.call(rbind, features_list)
  } else {
    data.frame()
  }
}


# ============================================================================
# Main Execution
# ============================================================================

if (!interactive()) {
  message("=== Lungmask -> PyRadiomics Pipeline Submission ===\n")

  # Create vault connection
  vault <- DSVaultCollection$new(
    endpoint = VAULT_ENDPOINT,
    collection = VAULT_COLLECTION,
    api_key = VAULT_API_KEY
  )

  # Create HPC config
  hpc_config <- create_api_config(
    base_url = HPC_BASE_URL,
    port = HPC_PORT,
    api_key = HPC_API_KEY,
    auth_header = "X-API-Key",
    auth_prefix = ""
  )

  # Get all image hashes from vault
  message("Getting image hashes from vault...")
  vault_hashes <- vault$list_hashes()
  message(sprintf("Found %d images in vault\n", nrow(vault_hashes)))

  # Submit pipelines for all images
  message("Submitting pipelines...")
  submission_results <- submit_batch_pipelines(
    hpc_config = hpc_config,
    image_hashes = vault_hashes$hash_sha256,
    filenames = vault_hashes$name,
    lungmask_model = LUNGMASK_MODEL,
    feature_classes = PYRADIOMICS_FEATURES,
    bin_width = PYRADIOMICS_BIN_WIDTH,
    verbose = TRUE
  )

  # Summary
  message("\n=== Submission Summary ===")
  message(sprintf("Total images: %d", nrow(submission_results)))
  message(sprintf("Submitted: %d", sum(submission_results$status == "submitted")))
  message(sprintf("Failed: %d", sum(submission_results$status == "failed")))

  # Show failed submissions
  failed <- submission_results[submission_results$status == "failed", ]
  if (nrow(failed) > 0) {
    message("\nFailed submissions:")
    for (i in seq_len(nrow(failed))) {
      message(sprintf("  - %s: %s",
                      substr(failed$image_hash[i], 1, 16),
                      failed$error[i]))
    }
  }

  # Save submission results
  output_file <- "pipeline_submissions.csv"
  write.csv(submission_results, output_file, row.names = FALSE)
  message(sprintf("\nSubmission results saved to: %s", output_file))

  # Wait for all pipelines to complete
  submitted_pipelines <- submission_results$pipeline_hash[submission_results$status == "submitted"]

  if (length(submitted_pipelines) > 0) {
    message("\n=== Waiting for Pipeline Results ===\n")
    pipeline_results <- wait_for_batch_pipelines(
      hpc_config = hpc_config,
      pipeline_hashes = submitted_pipelines,
      timeout = 600,
      interval = 10,
      verbose = TRUE
    )

    # Summary of results
    completed <- sum(sapply(pipeline_results, function(x) x$status == "completed"))
    failed <- sum(sapply(pipeline_results, function(x) x$status %in% c("failed", "error")))

    message("\n=== Pipeline Results Summary ===")
    message(sprintf("Completed: %d", completed))
    message(sprintf("Failed: %d", failed))

    # Extract features from completed pipelines
    features_df <- extract_features_from_results(pipeline_results, submission_results)

    if (nrow(features_df) > 0) {
      # Reorder columns: filename, image_hash, pipeline_hash, then features
      id_cols <- c("filename", "image_hash", "pipeline_hash")
      feature_cols <- setdiff(names(features_df), id_cols)
      features_df <- features_df[, c(id_cols, sort(feature_cols)), drop = FALSE]

      # Convert feature columns to numeric
      for (col in feature_cols) {
        features_df[[col]] <- as.numeric(features_df[[col]])
      }

      # Save as RDS (native R format with proper types)
      rds_file <- "radiomic_features.rds"
      saveRDS(features_df, rds_file)
      message(sprintf("\nFeatures saved to: %s", rds_file))
      message(sprintf("  - Images: %d", nrow(features_df)))
      message(sprintf("  - Features: %d (numeric)", length(feature_cols)))

      # Also save CSV for compatibility
      csv_file <- "radiomic_features.csv"
      write.csv(features_df, csv_file, row.names = FALSE)
      message(sprintf("  - Also saved as: %s", csv_file))

      # Print first few feature names
      message("\nFeature categories extracted:")
      feature_prefixes <- unique(sub("_.*", "", feature_cols))
      for (prefix in head(feature_prefixes, 10)) {
        count <- sum(grepl(paste0("^", prefix), feature_cols))
        message(sprintf("  - %s: %d features", prefix, count))
      }
    } else {
      message("\nNo features extracted from pipeline results.")
    }
  } else {
    message("\nNo pipelines were submitted successfully.")
  }
}
