#' Validation Functions for dsImaging
#'
#' Internal functions to validate inputs before processing.


# ============================================================================
# File Format Validation
# ============================================================================

#' Valid Medical Image Extensions
#'
#' Returns a character vector of valid medical image file extensions
#' supported by the imaging pipelines.
#'
#' @return Character vector of valid extensions (lowercase, with dot).
#' @export
#'
#' @examples
#' valid_image_extensions()
valid_image_extensions <- function() {
  c(
    # NIfTI formats
    ".nii", ".nii.gz",
    # DICOM (single file)
    ".dcm", ".dicom",
    # Analyze format
    ".hdr", ".img",
    # MetaImage
    ".mha", ".mhd",
    # NRRD
    ".nrrd", ".nhdr"
  )
}


#' Check if filename has valid medical image extension
#'
#' @param filename Character string with filename to check.
#'
#' @return TRUE if valid, FALSE otherwise.
#' @keywords internal
is_valid_image_format <- function(filename) {
  filename_lower <- tolower(filename)
  extensions <- valid_image_extensions()

  # Check each extension (handle compound extensions like .nii.gz)
  for (ext in extensions) {
    if (endsWith(filename_lower, ext)) {
      return(TRUE)
    }
  }
  return(FALSE)
}


#' Filter vault hashes to valid image formats
#'
#' Filters a data frame of vault hashes to include only files with
#' valid medical image extensions.
#'
#' @param vault_hashes Data frame with columns 'name' and 'hash_sha256'.
#' @param stop_if_empty If TRUE (default), throws an error if no valid
#'   images are found. If FALSE, returns empty data frame with warning.
#' @param verbose Print information about filtered files (default: TRUE).
#'
#' @return Filtered data frame with only valid image files.
#' @export
#'
#' @examples
#' \dontrun{
#' vault <- dsVault::DSVaultCollection$new(...)
#' hashes <- vault$list_hashes()
#' valid_hashes <- filter_valid_images(hashes)
#' }
filter_valid_images <- function(vault_hashes,
                                 stop_if_empty = TRUE,
                                 verbose = TRUE) {
  if (nrow(vault_hashes) == 0) {
    if (stop_if_empty) {
      stop("No files found in vault collection")
    }
    return(vault_hashes)
  }

  # Check each file
  is_valid <- vapply(vault_hashes$name, is_valid_image_format, logical(1))
  valid_hashes <- vault_hashes[is_valid, ]
  invalid_hashes <- vault_hashes[!is_valid, ]

  n_valid <- nrow(valid_hashes)
  n_invalid <- nrow(invalid_hashes)
  n_total <- nrow(vault_hashes)

  if (verbose && n_invalid > 0) {
    message(sprintf("Filtered %d/%d files: %d valid image formats, %d skipped",
                    n_valid, n_total, n_valid, n_invalid))
    if (n_invalid <= 5) {
      message(sprintf("  Skipped: %s", paste(invalid_hashes$name, collapse = ", ")))
    } else {
      message(sprintf("  Skipped (first 5): %s, ...",
                      paste(head(invalid_hashes$name, 5), collapse = ", ")))
    }
  }

  if (n_valid == 0) {
    valid_exts <- paste(valid_image_extensions(), collapse = ", ")
    msg <- sprintf(
      "No valid image files found in vault collection. Found %d files but none have valid extensions.\nValid extensions: %s",
      n_total, valid_exts
    )
    if (stop_if_empty) {
      stop(msg)
    } else {
      warning(msg)
    }
  }

  return(valid_hashes)
}


# ============================================================================
# HPC Methods Validation
# ============================================================================

#' Required methods for radiomics pipeline
#'
#' @return Character vector of required method names.
#' @keywords internal
required_radiomics_methods <- function() {
  c("lungmask", "pyradiomics")
}


#' Check if required HPC methods are available
#'
#' Queries the HPC API to verify that all methods required by the
#' radiomics pipeline are available. Uses dsHPC::has_methods() internally.
#'
#' @param hpc_unit HPC API configuration from dsHPC::create_api_config().
#' @param required_methods Character vector of method names to check.
#'   Defaults to methods required for radiomics pipeline.
#' @param stop_if_missing If TRUE (default), throws an error if any
#'   required methods are missing. If FALSE, returns result silently.
#' @param verbose Print information about available methods (default: TRUE).
#'
#' @return A list with:
#'   - available: Character vector of available required methods
#'   - missing: Character vector of missing required methods
#'   - all_methods: Character vector of all methods available on HPC
#'   - ok: Logical, TRUE if all required methods are available
#'
#' @export
#'
#' @examples
#' \dontrun{
#' hpc <- dsHPC::create_api_config(...)
#' check_hpc_methods(hpc)
#' }
check_hpc_methods <- function(hpc_unit,
                               required_methods = required_radiomics_methods(),
                               stop_if_missing = TRUE,
                               verbose = TRUE) {
  # Use dsHPC::has_methods() to check availability
  check_result <- tryCatch(
    dsHPC::has_methods(hpc_unit, required_methods),
    error = function(e) {
      stop(sprintf("Failed to query HPC methods: %s", e$message))
    }
  )

  result <- list(
    available = check_result$available,
    missing = check_result$missing,
    all_methods = check_result$all_methods,
    ok = check_result$all_available
  )

  if (verbose) {
    if (result$ok) {
      message(sprintf("HPC methods check: all %d required methods available (%s)",
                      length(required_methods),
                      paste(required_methods, collapse = ", ")))
    } else {
      message(sprintf("HPC methods check: %d/%d required methods available",
                      length(result$available), length(required_methods)))
      message(sprintf("  Available: %s",
                      if (length(result$available) > 0) paste(result$available, collapse = ", ") else "(none)"))
      message(sprintf("  Missing: %s", paste(result$missing, collapse = ", ")))
    }
  }

  if (!result$ok && stop_if_missing) {
    stop(sprintf(
      "Required HPC methods not available: %s\nAvailable methods: %s",
      paste(result$missing, collapse = ", "),
      if (length(result$all_methods) > 0) paste(result$all_methods, collapse = ", ") else "(none)"
    ))
  }

  return(result)
}


#' Validate all prerequisites for radiomics extraction
#'
#' Performs all validation checks before starting a radiomics pipeline:
#' - Validates vault connection and filters valid image formats
#' - Checks HPC connection and required methods availability
#'
#' @param collection A DSVaultCollection object.
#' @param hpc_unit HPC API configuration.
#' @param verbose Print validation progress (default: TRUE).
#'
#' @return A list with:
#'   - collection_hashes: Filtered data frame of valid image hashes
#'   - n_images: Number of valid images
#'   - hpc_methods: Result from check_hpc_methods()
#'
#' @keywords internal
validate_radiomics_inputs <- function(collection, hpc_unit, verbose = TRUE) {
  # Step 1: Validate collection and filter images
  if (verbose) message("Validating inputs...")

  if (!inherits(collection, "DSVaultCollection")) {
    stop("collection must be a DSVaultCollection object from dsVault package")
  }

  collection_hashes <- collection$list_hashes()

  if (verbose) {
    message(sprintf("  Found %d files in collection '%s'",
                    nrow(collection_hashes), collection$collection))
  }

  # Filter to valid image formats
  valid_hashes <- filter_valid_images(
    collection_hashes,
    stop_if_empty = TRUE,
    verbose = verbose
  )

  # Step 2: Check HPC methods
  if (verbose) message("  Checking HPC methods availability...")

  methods_check <- check_hpc_methods(
    hpc_unit,
    required_methods = required_radiomics_methods(),
    stop_if_missing = TRUE,
    verbose = FALSE  # We handle messaging
  )

  if (verbose) {
    message(sprintf("  HPC methods OK: %s",
                    paste(methods_check$available, collapse = ", ")))
  }

  list(
    collection_hashes = valid_hashes,
    n_images = nrow(valid_hashes),
    hpc_methods = methods_check
  )
}
