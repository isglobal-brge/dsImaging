#' Disclosure Control Functions for dsImaging
#'
#' Functions to enforce DataSHIELD's privacy-preserving subset filter
#' to prevent disclosure of sensitive patient information.


#' Retrieve DataSHIELD's Privacy-Preserving Subset Filter Value
#'
#' @title Get DataSHIELD Subset Filter Threshold
#' @description
#' Retrieves the subset filter threshold from DataSHIELD's disclosure control
#' settings. This value represents the minimum allowable number of patients/images
#' in any data subset to maintain privacy and prevent potential re-identification.
#'
#' @details
#' The function follows a hierarchical approach to determine the filter value:
#' 1. Checks for 'nfilter.subset' setting (primary)
#' 2. Falls back to 'datashield.nfilter.subset' if primary not found
#' 3. Uses 'default.nfilter.subset' as final fallback
#' 4. Returns 0 if no valid setting is found (no filtering applied)
#'
#' @return A numeric value representing the minimum required count for subset
#'   operations. Returns 0 if no filter setting is found.
#'
#' @keywords internal
get_subset_filter <- function() {

  # Check if dsBase is available

if (!requireNamespace("dsBase", quietly = TRUE)) {
    # dsBase not available - likely running outside DataSHIELD context
    # Return 0 to disable filtering (for local testing)
    return(0)
  }

  # Retrieve disclosure settings from dsBase
  nFilter <- tryCatch(
    dsBase::listDisclosureSettingsDS(),
    error = function(e) {
      # If we can't get settings, return empty list
      list()
    }
  )

  subsetFilter <- 0

  # Hierarchical check for filter value
  if (!is.null(nFilter$nfilter.subset)) {
    subsetFilter <- nFilter$nfilter.subset
  } else if (!is.null(nFilter$datashield.nfilter.subset)) {
    subsetFilter <- nFilter$datashield.nfilter.subset
  } else if (!is.null(nFilter$default.nfilter.subset)) {
    subsetFilter <- nFilter$default.nfilter.subset
  }

  return(as.numeric(subsetFilter))
}


#' Check if count meets disclosure threshold
#'
#' @title Validate Count Against Disclosure Threshold
#' @description
#' Checks if a count value meets the minimum disclosure threshold.
#' Throws an error if the count is below the threshold.
#'
#' @param count Numeric value to check.
#' @param context Character string describing what is being counted
#'   (e.g., "images in collection", "rows in result table").
#' @param threshold Optional explicit threshold. If NULL, uses get_subset_filter().
#'
#' @return Invisibly returns TRUE if check passes. Throws error otherwise.
#'
#' @keywords internal
check_disclosure_threshold <- function(count, context, threshold = NULL) {
  if (is.null(threshold)) {
    threshold <- get_subset_filter()
  }

  # If threshold is 0 or NA, no filtering applied
  if (is.na(threshold) || threshold <= 0) {
    return(invisible(TRUE))
  }

  if (count < threshold) {
    stop(sprintf(
      "DISCLOSURE CONTROL: The number of %s (%d) is below the minimum "
      , context, count
    ), sprintf(
      "disclosure threshold (nfilter.subset = %d). "
      , threshold
    ), "This operation cannot be completed to protect patient privacy.",
    call. = FALSE)
  }

  return(invisible(TRUE))
}


#' Validate image count before processing
#'
#' @title Check Image Count Against Disclosure Threshold
#' @description
#' Validates that the number of images to be processed meets the minimum
#' disclosure threshold. Should be called before processing begins.
#'
#' @param n_images Number of images to be processed.
#' @param verbose Print message if check passes (default: TRUE).
#'
#' @return Invisibly returns TRUE if check passes. Throws error otherwise.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Will pass if n_images >= nfilter.subset
#' check_image_disclosure(100)
#'
#' # Will throw error if below threshold
#' check_image_disclosure(3)  # Error if nfilter.subset > 3
#' }
check_image_disclosure <- function(n_images, verbose = TRUE) {
  threshold <- get_subset_filter()

  if (threshold > 0 && verbose) {
    message(sprintf("  Disclosure check: %d images (threshold: %d)",
                    n_images, threshold))
  }

  check_disclosure_threshold(
    count = n_images,
    context = "images in collection",
    threshold = threshold
  )

  return(invisible(TRUE))
}


#' Validate result table before returning
#'
#' @title Check Result Row Count Against Disclosure Threshold
#' @description
#' Validates that the number of rows in the result table meets the minimum
#' disclosure threshold. Should be called before returning results to user.
#' This is critical when using on_error="exclude" which may reduce row count.
#'
#' @param result_df Data frame with results to be returned.
#' @param verbose Print message if check passes (default: TRUE).
#'
#' @return Invisibly returns TRUE if check passes. Throws error otherwise.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Check before returning results
#' features_df <- extract_features(...)
#' check_result_disclosure(features_df)
#' return(features_df)
#' }
check_result_disclosure <- function(result_df, verbose = TRUE) {
  n_rows <- nrow(result_df)
  threshold <- get_subset_filter()

  if (threshold > 0 && verbose) {
    message(sprintf("  Disclosure check: %d result rows (threshold: %d)",
                    n_rows, threshold))
  }

  check_disclosure_threshold(
    count = n_rows,
    context = "rows in result table",
    threshold = threshold
  )

  return(invisible(TRUE))
}
