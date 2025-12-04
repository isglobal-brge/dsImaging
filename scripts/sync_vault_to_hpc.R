#!/usr/bin/env Rscript
#
# sync_vault_to_hpc.R
#
# This script synchronizes files from a dsMinIO-vault collection to dsHPC.
# It fetches the list of files and their hashes from the vault, checks which
# ones are missing in dsHPC, and uploads the missing files.
#
# Usage:
#   Rscript sync_vault_to_hpc.R
#
# Or source it in R and call sync_vault_to_hpc() with custom parameters.

library(httr)
library(jsonlite)
library(digest)
library(base64enc)

# ============================================================================
# dsHPC functions (sourced directly to avoid dependency issues)
# ============================================================================

#' Create an API client configuration
create_api_config <- function(base_url, port, api_key,
                              auth_header = "Authorization",
                              auth_prefix = "Bearer", timeout = 300) {
  base_url <- sub("/$", "", base_url)
  full_url <- paste0(base_url, ":", port)

  list(
    base_url = full_url,
    api_key = api_key,
    auth_header = auth_header,
    auth_prefix = auth_prefix,
    timeout = timeout
  )
}

#' Make an API request
api_request <- function(config, endpoint, method = "GET",
                        params = list(), body = NULL) {
  if (!startsWith(endpoint, "/")) {
    endpoint <- paste0("/", endpoint)
  }

  url <- paste0(config$base_url, endpoint)

  headers <- c(
    "Content-Type" = "application/json",
    "Accept" = "application/json"
  )

  if (!is.null(config$api_key) && config$api_key != "") {
    if (is.null(config$auth_prefix) || config$auth_prefix == "") {
      auth_value <- config$api_key
    } else {
      auth_value <- paste(config$auth_prefix, config$api_key)
    }
    headers[config$auth_header] <- auth_value
  }

  if (!is.null(body)) {
    body <- toJSON(body, auto_unbox = TRUE)
  }

  response <- switch(
    toupper(method),
    "GET" = GET(url = url, query = params,
                add_headers(.headers = headers), timeout(config$timeout)),
    "POST" = POST(url = url, query = params,
                  add_headers(.headers = headers), body = body,
                  encode = "json", timeout(config$timeout)),
    stop("Unsupported HTTP method: ", method)
  )

  if (http_error(response)) {
    status <- status_code(response)
    content <- tryCatch({
      content(response, "text", encoding = "UTF-8")
    }, error = function(e) "Could not parse error response")
    stop(paste0(http_status(response)$message, " (HTTP ", status, ").\n",
                "URL: ", url, "\n", "Response: ", content))
  }

  resp_content <- content(response, "text", encoding = "UTF-8")
  if (resp_content == "") return(list())

  tryCatch({
    fromJSON(resp_content)
  }, error = function(e) {
    warning("Could not parse JSON response: ", e$message)
    resp_content
  })
}

api_post <- function(config, endpoint, body, params = list()) {
  api_request(config, endpoint, "POST", params, body)
}

#' Hash content using SHA-256
hash_content <- function(content) {
  if (!is.raw(content)) {
    if (is.character(content)) {
      content <- charToRaw(paste(content, collapse = "\n"))
    } else {
      content <- serialize(content, NULL)
    }
  }
  digest(content, algo = "sha256", serialize = FALSE)
}

#' Check which hashes exist in dsHPC
check_existing_hashes <- function(config, hashes) {
  if (!is.character(hashes)) stop("Hashes must be a character vector")
  body <- list(hashes = as.list(hashes))
  api_post(config, "/files/check-hashes", body = body)
}

#' Check if a hash exists
hash_exists <- function(config, hash) {
  result <- check_existing_hashes(config, c(hash))
  hash %in% result$existing_hashes
}

#' Upload file content to dsHPC
upload_file <- function(config, content, filename,
                        chunk_size_mb = 10, show_progress = TRUE) {
  if (!is.raw(content)) {
    if (is.character(content) && length(content) == 1 && file.exists(content)) {
      content <- readBin(content, "raw", file.info(content)$size)
    } else {
      content <- serialize(content, NULL)
    }
  }

  file_hash <- hash_content(content)
  total_size <- length(content)

  if (show_progress) {
    message(sprintf("  Hash: %s", file_hash))
    message(sprintf("  Size: %.2f MB", total_size / (1024^2)))
  }

  # Check if already exists
  if (hash_exists(config, file_hash)) {
    if (show_progress) message("  Already exists (skipping upload)")
    return(file_hash)
  }

  # Initialize chunked upload
  chunk_size <- chunk_size_mb * 1024 * 1024
  init_body <- list(
    file_hash = file_hash,
    filename = filename,
    content_type = "application/octet-stream",
    total_size = total_size,
    chunk_size = chunk_size
  )

  init_response <- api_post(config, "/files/upload-chunked/init", body = init_body)
  session_id <- init_response$session_id

  # Upload chunks

  total_chunks <- ceiling(total_size / chunk_size)
  chunk_num <- 0
  bytes_uploaded <- 0

  tryCatch({
    while (bytes_uploaded < total_size) {
      start <- bytes_uploaded + 1
      end <- min(bytes_uploaded + chunk_size, total_size)
      chunk_data <- content[start:end]

      chunk_base64 <- base64encode(chunk_data)
      chunk_body <- list(chunk_number = chunk_num, chunk_data = chunk_base64)

      api_post(config,
               paste0("/files/upload-chunked/", session_id, "/chunk"),
               body = chunk_body)

      bytes_uploaded <- end
      chunk_num <- chunk_num + 1

      if (show_progress) {
        pct <- (bytes_uploaded / total_size) * 100
        message(sprintf("  Chunk %d/%d (%.1f%%)", chunk_num, total_chunks, pct))
      }
    }

    # Finalize
    api_post(config,
             paste0("/files/upload-chunked/", session_id, "/finalize"),
             body = list(total_chunks = chunk_num))

    if (show_progress) message("  Upload complete!")
    return(file_hash)

  }, error = function(e) {
    tryCatch({
      api_request(config, "DELETE",
                  paste0("/files/upload-chunked/", session_id))
    }, error = function(e2) NULL)
    stop(paste0("Upload failed: ", e$message))
  })
}

# ============================================================================
# Vault functions
# ============================================================================

#' Fetch hashes from dsMinIO-vault collection
get_vault_hashes <- function(vault_url, vault_port, collection, api_key) {
  url <- sprintf("%s:%d/api/v1/collections/%s/hashes",
                 vault_url, vault_port, collection)

  response <- GET(url, add_headers("X-Collection-Key" = api_key))

  if (http_error(response)) {
    stop(sprintf("Failed to fetch vault hashes: %s",
                 http_status(response)$message))
  }

  items <- content(response, "parsed")$items
  if (length(items) == 0) {
    return(data.frame(name = character(), hash_sha256 = character()))
  }

  do.call(rbind, lapply(items, as.data.frame))
}

#' Download a file from dsMinIO-vault
download_vault_file <- function(vault_url, vault_port,
                                collection, api_key, filename) {
  url <- sprintf("%s:%d/api/v1/collections/%s/objects/%s",
                 vault_url, vault_port, collection, filename)

  response <- GET(url, add_headers("X-Collection-Key" = api_key))

  if (http_error(response)) {
    stop(sprintf("Failed to download '%s': %s",
                 filename, http_status(response)$message))
  }

  content(response, "raw")
}

# ============================================================================
# Main sync function
# ============================================================================

#' Sync files from dsMinIO-vault to dsHPC
#'
#' @param vault_url Base URL of the vault API (default: "http://localhost")
#' @param vault_port Port of the vault API (default: 8000)
#' @param collection Name of the vault collection (default: "images")
#' @param vault_api_key Collection API key
#' @param hpc_url Base URL of the dsHPC API (default: "http://localhost")
#' @param hpc_port Port of the dsHPC API (default: 8001)
#' @param hpc_api_key dsHPC API key
#' @param dry_run If TRUE, only report without uploading
#' @param verbose If TRUE, print progress messages
#'
#' @return A list with sync results
sync_vault_to_hpc <- function(
    vault_url = "http://localhost",
    vault_port = 8000,
    collection = "images",
    vault_api_key = NULL,
    hpc_url = "http://localhost",
    hpc_port = 8001,
    hpc_api_key = NULL,
    dry_run = FALSE,
    verbose = TRUE
) {

  if (is.null(vault_api_key)) stop("vault_api_key is required")
  if (is.null(hpc_api_key)) stop("hpc_api_key is required")

  # Step 1: Get all hashes from vault
  if (verbose) message("\n=== Step 1: Fetching file list from vault ===")
  vault_files <- get_vault_hashes(vault_url, vault_port, collection, vault_api_key)

  if (nrow(vault_files) == 0) {
    message("No files found in vault collection.")
    return(list(total = 0, existing = 0, uploaded = 0, failed = 0))
  }

  if (verbose) {
    message(sprintf("Found %d files in vault collection '%s'",
                    nrow(vault_files), collection))
  }

  # Step 2: Create dsHPC config and check which hashes exist
  if (verbose) message("\n=== Step 2: Checking which files exist in dsHPC ===")

  hpc_config <- create_api_config(
    base_url = hpc_url,
    port = hpc_port,
    api_key = hpc_api_key,
    auth_header = "X-API-Key",
    auth_prefix = ""
  )

  all_hashes <- vault_files$hash_sha256
  hash_check <- check_existing_hashes(hpc_config, all_hashes)

  existing_hashes <- hash_check$existing_hashes
  missing_hashes <- hash_check$missing_hashes

  if (verbose) {
    message(sprintf("  Already in dsHPC: %d files", length(existing_hashes)))
    message(sprintf("  Missing in dsHPC: %d files", length(missing_hashes)))
  }

  if (length(missing_hashes) == 0) {
    message("\nAll files already exist in dsHPC. Nothing to upload.")
    return(list(
      total = nrow(vault_files),
      existing = length(existing_hashes),
      uploaded = 0,
      failed = 0
    ))
  }

  # Step 3: Upload missing files
  if (verbose) message("\n=== Step 3: Uploading missing files to dsHPC ===")

  missing_files <- vault_files[vault_files$hash_sha256 %in% missing_hashes, ]

  if (dry_run) {
    message("\n[DRY RUN] Would upload the following files:")
    for (i in seq_len(nrow(missing_files))) {
      message(sprintf("  - %s (%s...)",
                      missing_files$name[i],
                      substr(missing_files$hash_sha256[i], 1, 16)))
    }
    return(list(
      total = nrow(vault_files),
      existing = length(existing_hashes),
      uploaded = 0,
      failed = 0,
      would_upload = nrow(missing_files)
    ))
  }

  uploaded <- 0
  failed <- 0
  failed_files <- character()

  for (i in seq_len(nrow(missing_files))) {
    filename <- missing_files$name[i]
    expected_hash <- missing_files$hash_sha256[i]

    if (verbose) {
      message(sprintf("\n[%d/%d] Uploading: %s",
                      i, nrow(missing_files), filename))
    }

    tryCatch({
      # Download from vault
      file_content <- download_vault_file(
        vault_url, vault_port, collection, vault_api_key, filename
      )

      if (verbose) {
        message(sprintf("  Downloaded: %.2f MB", length(file_content) / (1024^2)))
      }

      # Upload to dsHPC
      result_hash <- upload_file(hpc_config, file_content, filename)

      if (result_hash != expected_hash) {
        warning(sprintf("  Hash mismatch! Expected: %s, Got: %s",
                        expected_hash, result_hash))
        failed <- failed + 1
        failed_files <- c(failed_files, filename)
      } else {
        uploaded <- uploaded + 1
      }

    }, error = function(e) {
      message(sprintf("  ERROR: %s", e$message))
      failed <<- failed + 1
      failed_files <<- c(failed_files, filename)
    })
  }

  # Summary
  message("\n=== Sync Complete ===")
  message(sprintf("Total files in vault:    %d", nrow(vault_files)))
  message(sprintf("Already in dsHPC:        %d", length(existing_hashes)))
  message(sprintf("Successfully uploaded:   %d", uploaded))
  message(sprintf("Failed:                  %d", failed))

  if (failed > 0) {
    message("\nFailed files:")
    for (f in failed_files) message(sprintf("  - %s", f))
  }

  list(
    total = nrow(vault_files),
    existing = length(existing_hashes),
    uploaded = uploaded,
    failed = failed,
    failed_files = if (failed > 0) failed_files else NULL
  )
}

# ============================================================================
# Main execution
# ============================================================================

if (!interactive()) {
  result <- sync_vault_to_hpc(
    vault_url = "http://localhost",
    vault_port = 8000,
    collection = "images",
    vault_api_key = "0bbbdae3a5a82c7944f5010083aaa227ba8f337ddd8c8c34c224723cebc608db",
    hpc_url = "http://localhost",
    hpc_port = 8001,
    hpc_api_key = "lXCXTxsK6JK8aeGSAZkAI8FGLYug8H9u",
    dry_run = FALSE,
    verbose = TRUE
  )
}
