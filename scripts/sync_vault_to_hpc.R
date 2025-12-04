#!/usr/bin/env Rscript
#
# sync_vault_to_hpc.R
#
# Functions for synchronizing files from dsMinIO-vault collections to dsHPC.
# Uses dsVault and dsHPC packages for all operations.
#
# These functions are designed to be modular and can be integrated into a
# package.

library(dsVault)
library(dsHPC)

# ============================================================================
# Core Functions
# ============================================================================

#' Get hashes from one or more vault collections
#'
#' @param vaults A single DSVaultCollection object or a list of
#'   DSVaultCollection objects
#'
#' @return A data frame with columns: collection, name, hash_sha256
#'
#' @examples
#' \dontrun{
#' # Single collection
#' vault <- DSVaultCollection$new(endpoint, "images", api_key)
#' hashes <- get_vault_hashes(vault)
#'
#' # Multiple collections
#' vaults <- list(
#'   DSVaultCollection$new(endpoint, "images", key1),
#'   DSVaultCollection$new(endpoint, "masks", key2)
#' )
#' hashes <- get_vault_hashes(vaults)
#' }
get_vault_hashes <- function(vaults) {
  # Normalize to list
 if (inherits(vaults, "DSVaultCollection")) {
    vaults <- list(vaults)
  }

  # Validate all elements are DSVaultCollection
 for (i in seq_along(vaults)) {
    if (!inherits(vaults[[i]], "DSVaultCollection")) {
      stop(sprintf("Element %d is not a DSVaultCollection object", i))
    }
  }

  # Collect hashes from all collections
  all_hashes <- lapply(vaults, function(vault) {
    hashes <- vault$list_hashes()
    if (nrow(hashes) > 0) {
      hashes$collection <- vault$collection
      hashes[, c("collection", "name", "hash_sha256")]
    } else {
      data.frame(
        collection = character(),
        name = character(),
        hash_sha256 = character()
      )
    }
  })

  do.call(rbind, all_hashes)
}

#' Check which hashes exist in dsHPC
#'
#' @param hpc_config dsHPC API configuration created by create_api_config()
#' @param hashes Character vector of SHA-256 hashes to check
#'
#' @return A list with:
#'   - existing: character vector of hashes that exist in dsHPC
#'   - missing: character vector of hashes that don't exist in dsHPC
#'
#' @examples
#' \dontrun{
#' config <- create_api_config("http://localhost", 8001, "api_key",
#'                             auth_header = "X-API-Key", auth_prefix = "")
#' result <- check_hashes_in_hpc(config, c("hash1", "hash2", "hash3"))
#' result$existing
#' result$missing
#' }
check_hashes_in_hpc <- function(hpc_config, hashes) {
  if (length(hashes) == 0) {
    return(list(existing = character(), missing = character()))
  }

  # Use dsHPC's check_existing_hashes
  result <- check_existing_hashes(hpc_config, unique(hashes))

  list(
    existing = result$existing_hashes %||% character(),
    missing = result$missing_hashes %||% character()
  )
}

#' Upload missing files from vault collections to dsHPC
#'
#' @param vaults A single DSVaultCollection object or a list of
#'   DSVaultCollection objects
#' @param hpc_config dsHPC API configuration created by create_api_config()
#' @param missing_hashes Character vector of hashes to upload (from
#'   check_hashes_in_hpc)
#' @param vault_hashes Data frame with columns: collection, name, hash_sha256
#'   (from get_vault_hashes). Used to map hashes back to files.
#' @param verbose If TRUE, print progress messages
#'
#' @return A list with:
#'   - uploaded: number of successfully uploaded files
#'   - failed: number of failed uploads
#'   - failed_files: data frame of failed files (collection, name, error)
#'
#' @examples
#' \dontrun{
#' vaults <- list(
#'   images = DSVaultCollection$new(endpoint, "images", key1),
#'   masks = DSVaultCollection$new(endpoint, "masks", key2)
#' )
#' vault_hashes <- get_vault_hashes(vaults)
#' check_result <- check_hashes_in_hpc(hpc_config, vault_hashes$hash_sha256)
#' upload_result <- upload_missing_to_hpc(
#'   vaults, hpc_config, check_result$missing, vault_hashes
#' )
#' }
upload_missing_to_hpc <- function(vaults, hpc_config, missing_hashes,
                                   vault_hashes, verbose = TRUE) {
  if (length(missing_hashes) == 0) {
    return(list(uploaded = 0, failed = 0, failed_files = NULL))
  }

  # Normalize vaults to named list
  if (inherits(vaults, "DSVaultCollection")) {
    vaults <- setNames(list(vaults), vaults$collection)
  } else if (is.list(vaults) && is.null(names(vaults))) {
    # Unnamed list - create names from collection names
    names(vaults) <- sapply(vaults, function(v) v$collection)
  }

  # Filter to only missing files
  missing_files <- vault_hashes[vault_hashes$hash_sha256 %in% missing_hashes, ]

  uploaded <- 0
  failed <- 0
  failed_files <- list()

  for (i in seq_len(nrow(missing_files))) {
    row <- missing_files[i, ]
    collection <- row$collection
    filename <- row$name
    expected_hash <- row$hash_sha256

    if (verbose) {
      message(sprintf("[%d/%d] %s/%s",
                      i, nrow(missing_files), collection, filename))
    }

    # Get the vault for this collection
    vault <- vaults[[collection]]
    if (is.null(vault)) {
      msg <- sprintf("No vault found for collection '%s'", collection)
      if (verbose) message(sprintf("  ERROR: %s", msg))
      failed <- failed + 1
      failed_files[[length(failed_files) + 1]] <- data.frame(
        collection = collection, name = filename, error = msg
      )
      next
    }

    tryCatch({
      # Download from vault
      content <- vault$download(filename)
      if (verbose) {
        message(sprintf("  Downloaded: %.2f MB", length(content) / (1024^2)))
      }

      # Upload to dsHPC
      result_hash <- upload_file(hpc_config, content, filename)

      # Verify hash
      if (result_hash != expected_hash) {
        msg <- sprintf("Hash mismatch: expected %s, got %s",
                       expected_hash, result_hash)
        warning(sprintf("  %s", msg))
        failed <- failed + 1
        failed_files[[length(failed_files) + 1]] <- data.frame(
          collection = collection, name = filename, error = msg
        )
      } else {
        if (verbose) message("  OK")
        uploaded <- uploaded + 1
      }

    }, error = function(e) {
      if (verbose) message(sprintf("  ERROR: %s", e$message))
      failed <<- failed + 1
      failed_files[[length(failed_files) + 1]] <<- data.frame(
        collection = collection, name = filename, error = e$message
      )
    })
  }

  list(
    uploaded = uploaded,
    failed = failed,
    failed_files = if (length(failed_files) > 0) {
      do.call(rbind, failed_files)
    } else {
      NULL
    }
  )
}

# ============================================================================
# High-level Function
# ============================================================================

#' Sync files from vault collections to dsHPC
#'
#' High-level function that combines get_vault_hashes, check_hashes_in_hpc,
#' and upload_missing_to_hpc.
#'
#' @param vaults A single DSVaultCollection object or a list of
#'   DSVaultCollection objects
#' @param hpc_config dsHPC API configuration created by create_api_config()
#' @param dry_run If TRUE, only check without uploading
#' @param verbose If TRUE, print progress messages
#'
#' @return A list with sync results:
#'   - total: total files in vault(s)
#'   - existing: files already in dsHPC
#'   - uploaded: files successfully uploaded
#'   - failed: files that failed to upload
#'   - failed_files: data frame of failed files (if any)
#'
#' @examples
#' \dontrun{
#' # Single collection
#' vault <- DSVaultCollection$new(
#'   endpoint = "http://localhost:8000",
#'   collection = "images",
#'   api_key = "vault_key"
#' )
#' hpc_config <- create_api_config(
#'   "http://localhost", 8001, "hpc_key",
#'   auth_header = "X-API-Key", auth_prefix = ""
#' )
#' result <- sync_vaults_to_hpc(vault, hpc_config)
#'
#' # Multiple collections
#' vaults <- list(
#'   DSVaultCollection$new(endpoint, "images", key1),
#'   DSVaultCollection$new(endpoint, "masks", key2)
#' )
#' result <- sync_vaults_to_hpc(vaults, hpc_config)
#' }
sync_vaults_to_hpc <- function(vaults, hpc_config,
                                dry_run = FALSE, verbose = TRUE) {

  # Step 1: Get all hashes from vault(s)
  if (verbose) message("=== Getting hashes from vault(s) ===")
  vault_hashes <- get_vault_hashes(vaults)

  if (nrow(vault_hashes) == 0) {
    if (verbose) message("No files found in vault(s).")
    return(list(total = 0, existing = 0, uploaded = 0, failed = 0))
  }

  if (verbose) {
    collections <- unique(vault_hashes$collection)
    message(sprintf("Found %d files in %d collection(s): %s",
                    nrow(vault_hashes), length(collections),
                    paste(collections, collapse = ", ")))
  }

  # Step 2: Check which hashes exist in dsHPC
  if (verbose) message("\n=== Checking hashes in dsHPC ===")
  check_result <- check_hashes_in_hpc(hpc_config, vault_hashes$hash_sha256)

  if (verbose) {
    message(sprintf("  Already in dsHPC: %d", length(check_result$existing)))
    message(sprintf("  Missing in dsHPC: %d", length(check_result$missing)))
  }

  # Early return if nothing to upload
  if (length(check_result$missing) == 0) {
    if (verbose) message("\nAll files already exist in dsHPC.")
    return(list(
      total = nrow(vault_hashes),
      existing = length(check_result$existing),
      uploaded = 0,
      failed = 0
    ))
  }

  # Dry run - just report what would be uploaded
  if (dry_run) {
    missing_files <- vault_hashes[
      vault_hashes$hash_sha256 %in% check_result$missing,
    ]
    if (verbose) {
      message("\n[DRY RUN] Would upload:")
      for (i in seq_len(nrow(missing_files))) {
        message(sprintf("  - %s/%s",
                        missing_files$collection[i],
                        missing_files$name[i]))
      }
    }
    return(list(
      total = nrow(vault_hashes),
      existing = length(check_result$existing),
      uploaded = 0,
      failed = 0,
      would_upload = nrow(missing_files)
    ))
  }

  # Step 3: Upload missing files
  if (verbose) message("\n=== Uploading missing files ===")
  upload_result <- upload_missing_to_hpc(
    vaults, hpc_config, check_result$missing, vault_hashes, verbose
  )

  # Summary
  if (verbose) {
    message("\n=== Summary ===")
    message(sprintf("Total files in vault(s): %d", nrow(vault_hashes)))
    message(sprintf("Already in dsHPC:        %d", length(check_result$existing)))
    message(sprintf("Uploaded:                %d", upload_result$uploaded))
    message(sprintf("Failed:                  %d", upload_result$failed))
  }

  list(
    total = nrow(vault_hashes),
    existing = length(check_result$existing),
    uploaded = upload_result$uploaded,
    failed = upload_result$failed,
    failed_files = upload_result$failed_files
  )
}

# ============================================================================
# Utility: Null coalescing operator
# ============================================================================

`%||%` <- function(x, y) if (is.null(x)) y else x

# ============================================================================
# Main execution (example)
# ============================================================================

if (!interactive()) {
  # Example: Single collection
  vault <- DSVaultCollection$new(
    endpoint = "http://localhost:8000",
    collection = "images",
    api_key = "0bbbdae3a5a82c7944f5010083aaa227ba8f337ddd8c8c34c224723cebc608db"
  )

  hpc_config <- create_api_config(
    base_url = "http://localhost",
    port = 8001,
    api_key = "lXCXTxsK6JK8aeGSAZkAI8FGLYug8H9u",
    auth_header = "X-API-Key",
    auth_prefix = ""
  )

  result <- sync_vaults_to_hpc(vault, hpc_config)
}
