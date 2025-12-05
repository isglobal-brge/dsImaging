#!/usr/bin/env Rscript
#
# test_pipeline_dedup_two_nodes.R
#
# Test that pipeline deduplication works correctly with 2-node pipelines:
# - Node A produces output
# - Node B references Node A's output
#
# The test verifies that job hash is calculated on RESOLVED values,
# not on the reference syntax ($ref:nodeA vs direct hash).

library(dsVault)
library(dsHPC)

# ============================================================================
# Configuration
# ============================================================================

VAULT_ENDPOINT <- "http://localhost:8000"
VAULT_COLLECTION <- "images"
VAULT_API_KEY <- "0bbbdae3a5a82c7944f5010083aaa227ba8f337ddd8c8c34c224723cebc608db"

HPC_BASE_URL <- "http://localhost"
HPC_PORT <- 8001
HPC_API_KEY <- "lXCXTxsK6JK8aeGSAZkAI8FGLYug8H9u"

# ============================================================================
# Test Functions
# ============================================================================

#' Create a 2-node pipeline where node B depends on node A
#'
#' @param image_hash Input image hash for node A
#' @param use_ref If TRUE, node B uses $ref:nodeA. If FALSE, needs resolved_hash.
#' @param resolved_hash Direct hash to use instead of $ref (when use_ref=FALSE)
#'
#' @return Pipeline definition
create_two_node_pipeline <- function(
  image_hash,
  use_ref = TRUE,
  resolved_hash = NULL
) {
  # Node A: lungmask segmentation
  node_a <- create_pipeline_node(
    chain = list(
      list(
        method_name = "lungmask",
        parameters = list(
          model = "R231",
          force_cpu = TRUE
        )
      )
    ),
    dependencies = character(0),
    input_file_hash = image_hash
  )

  # Node B: pyradiomics - references node A's mask output
  # This is where we test: $ref:nodeA/data/mask_base64 vs direct hash
  if (use_ref) {
    # Use $ref syntax
    node_b <- create_pipeline_node(
      chain = list(
        list(
          method_name = "pyradiomics",
          parameters = list(
            feature_classes = "firstorder,shape,glcm,glrlm,glszm,gldm,ngtdm",
            bin_width = 25,
            normalize = FALSE
          ),
          file_inputs = list(
            image = image_hash,  # Direct hash for image
            mask = "$ref:nodeA/data/mask_base64"  # Reference to node A's output
          )
        )
      ),
      dependencies = c("nodeA")
    )
  } else {
    # Use direct resolved hash
    if (is.null(resolved_hash)) {
      stop("resolved_hash required when use_ref=FALSE")
    }
    node_b <- create_pipeline_node(
      chain = list(
        list(
          method_name = "pyradiomics",
          parameters = list(
            feature_classes = "firstorder,shape,glcm,glrlm,glszm,gldm,ngtdm",
            bin_width = 25,
            normalize = FALSE
          ),
          file_inputs = list(
            image = image_hash,
            mask = resolved_hash  # Direct hash instead of $ref
          )
        )
      ),
      dependencies = c("nodeA")  # Still depends on nodeA for execution order
    )
  }

  pipeline <- list(
    nodes = list(
      nodeA = node_a,
      nodeB = node_b
    )
  )

  return(pipeline)
}


# ============================================================================
# Main Test
# ============================================================================

if (!interactive()) {
  message("=== Testing 2-Node Pipeline Deduplication ===\n")

  # Create HPC config
  hpc_config <- create_api_config(
    base_url = HPC_BASE_URL,
    port = HPC_PORT,
    api_key = HPC_API_KEY,
    auth_header = "X-API-Key",
    auth_prefix = ""
  )

  # Create vault connection and get first image
  vault <- DSVaultCollection$new(
    endpoint = VAULT_ENDPOINT,
    collection = VAULT_COLLECTION,
    api_key = VAULT_API_KEY
  )

  vault_hashes <- vault$list_hashes()
  if (nrow(vault_hashes) == 0) {
    stop("No images in vault")
  }

  test_image_hash <- vault_hashes$hash_sha256[1]
  message(sprintf("Using test image: %s\n", substr(test_image_hash, 1, 16)))

  # -------------------------------------------------------------------------
  # Step 1: Submit pipeline with $ref syntax
  # -------------------------------------------------------------------------
  message("Step 1: Submitting pipeline with $ref:nodeA syntax...")

  pipeline_ref <- create_two_node_pipeline(
    image_hash = test_image_hash,
    use_ref = TRUE
  )

  response_ref <- submit_pipeline(hpc_config, pipeline_ref)
  message(sprintf("  Pipeline 1 (with $ref): %s\n", response_ref$pipeline_hash))

  # Wait for completion to get the resolved mask hash
  message("Waiting for pipeline 1 to complete...")
  result_ref <- wait_for_pipeline(
    hpc_config,
    response_ref$pipeline_hash,
    timeout = 300,
    interval = 5,
    verbose = TRUE
  )

  if (result_ref$status != "completed") {
    stop(sprintf("Pipeline 1 failed: %s", result_ref$error))
  }

  # Get the resolved mask hash from nodeA's output
  node_a_output <- result_ref$nodes$nodeA
  message(sprintf("\nNode A completed. Output hash: %s",
                  substr(node_a_output$output_hash, 1, 16)))

  # The mask hash was extracted from nodeA's output by the orchestrator
  # We need to find what hash was actually used for nodeB's mask input
  # This is stored in the job's file_inputs

  # -------------------------------------------------------------------------
  # Step 2: Get job details to find resolved mask hash
  # -------------------------------------------------------------------------
  message("\nStep 2: Getting job details from pipeline 1...")

  # Get nodeB's meta-job hash
  node_b_info <- result_ref$nodes$nodeB
  node_b_meta_job <- node_b_info$meta_job_hash

  message(sprintf("  NodeB meta-job: %s", substr(node_b_meta_job, 1, 16)))

  # Get meta-job details to find the job hash
  meta_job_status <- get_meta_job_status(hpc_config, node_b_meta_job)
  node_b_job_hash <- meta_job_status$chain[[1]]$job_hash

  message(sprintf("  NodeB job hash: %s", substr(node_b_job_hash, 1, 16)))

  # Get job details to see the resolved file_inputs
  job_details <- get_job_status(hpc_config, node_b_job_hash)
  resolved_mask_hash <- job_details$file_inputs$mask

  message(sprintf("  Resolved mask hash: %s", substr(resolved_mask_hash, 1, 16)))

  # -------------------------------------------------------------------------
  # Step 3: Submit identical pipeline but with direct hash
  # -------------------------------------------------------------------------
  message("\n\nStep 3: Submitting pipeline with DIRECT hash (not $ref)...")

  pipeline_direct <- create_two_node_pipeline(
    image_hash = test_image_hash,
    use_ref = FALSE,
    resolved_hash = resolved_mask_hash
  )

  response_direct <- submit_pipeline(hpc_config, pipeline_direct)
  message(sprintf("  Pipeline 2 (with direct hash): %s\n", response_direct$pipeline_hash))

  # Wait for completion
  message("Waiting for pipeline 2 to complete...")
  result_direct <- wait_for_pipeline(
    hpc_config,
    response_direct$pipeline_hash,
    timeout = 300,
    interval = 5,
    verbose = TRUE
  )

  if (result_direct$status != "completed") {
    stop(sprintf("Pipeline 2 failed: %s", result_direct$error))
  }

  # -------------------------------------------------------------------------
  # Step 4: Compare job hashes
  # -------------------------------------------------------------------------
  message("\n\n=== Results ===")

  # Get nodeB job hash from pipeline 2
  node_b_info_2 <- result_direct$nodes$nodeB
  node_b_meta_job_2 <- node_b_info_2$meta_job_hash
  meta_job_status_2 <- get_meta_job_status(hpc_config, node_b_meta_job_2)
  node_b_job_hash_2 <- meta_job_status_2$chain[[1]]$job_hash

  message(sprintf("Pipeline 1 (with $ref):     nodeB job = %s", node_b_job_hash))
  message(sprintf("Pipeline 2 (direct hash):   nodeB job = %s", node_b_job_hash_2))

  if (node_b_job_hash == node_b_job_hash_2) {
    message("\n*** SUCCESS: Job hashes are IDENTICAL! ***")
    message("This proves that deduplication works on RESOLVED values,")
    message("not on the reference syntax ($ref vs direct hash).")
  } else {
    message("\n*** FAILURE: Job hashes are DIFFERENT! ***")
    message("The deduplication is incorrectly using the reference syntax")
    message("instead of the resolved values.")
  }

  # Also check if nodeA was deduplicated in pipeline 2
  node_a_job_1 <- get_meta_job_status(hpc_config, result_ref$nodes$nodeA$meta_job_hash)$chain[[1]]$job_hash
  node_a_job_2 <- get_meta_job_status(hpc_config, result_direct$nodes$nodeA$meta_job_hash)$chain[[1]]$job_hash

  message(sprintf("\nNodeA deduplication: %s",
                  if(node_a_job_1 == node_a_job_2) "YES (same job)" else "NO (different jobs)"))
}
