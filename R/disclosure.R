# Module: Metadata Disclosure Control
# Shared helpers for safe metadata output across all DS packages.
# Applies DataSHIELD nfilter rules to count-bearing metadata.

#' Apply disclosure control to a count value
#'
#' Returns the count as-is, bucketed (power-of-2), or NA depending
#' on the active trust profile and nfilter settings.
#'
#' @param n Integer; the raw count.
#' @param profile Character; the active trust profile name.
#' @return Integer (exact or bucketed) or NA_integer_ (hidden).
#' @export
safe_metadata_count <- function(n, profile = NULL) {
  if (is.null(profile)) profile <- .get_active_profile()
  nfilter <- .get_nfilter_subset()

  # Tier A profiles: exact counts OK
  if (profile %in% c("sandbox_open", "trusted_internal"))
    return(as.integer(n))

  # Tier D profiles: hide very small counts
  if (profile %in% c("clinical_hardened", "high_sensitivity_dp")) {
    if (!is.na(n) && n < nfilter) return(NA_integer_)
    return(.bucket_count(n))
  }

  # Tier B/C profiles: bucketed counts
  .bucket_count(n)
}

#' Apply disclosure control to a distribution table
#'
#' Buckets or hides per-class counts depending on the trust profile.
#'
#' @param counts Named integer vector (class -> count).
#' @param profile Character; the active trust profile name.
#' @return Named vector with safe counts, or NULL if hidden.
#' @export
safe_metadata_distribution <- function(counts, profile = NULL) {
  if (is.null(profile)) profile <- .get_active_profile()

  # Hidden in hardened/DP profiles
  if (profile %in% c("clinical_hardened", "high_sensitivity_dp"))
    return(NULL)

  # Exact in sandbox/trusted
  if (profile %in% c("sandbox_open", "trusted_internal"))
    return(counts)

  # Bucketed in consortium/clinical/update_noise
  nfilter <- .get_nfilter_tab()
  safe <- vapply(counts, function(x) {
    if (x < nfilter) NA_integer_ else .bucket_count(x)
  }, integer(1))
  safe
}

#' Apply disclosure control to a list of discovered levels
#'
#' Suppresses level lists that are too large or too dense.
#'
#' @param levels Character vector of discovered level names.
#' @param n_total Integer; total sample count.
#' @param profile Character; the active trust profile name.
#' @return Character vector (possibly truncated) or NULL if suppressed.
#' @export
safe_metadata_levels <- function(levels, n_total = NULL, profile = NULL) {
  if (is.null(profile)) profile <- .get_active_profile()

  max_levels <- as.integer(getOption("nfilter.levels.max",
    getOption("default.nfilter.levels.max", 40)))
  max_density <- as.numeric(getOption("nfilter.levels.density",
    getOption("default.nfilter.levels.density", 0.33)))

  n_levels <- length(levels)
  if (n_levels > max_levels) return(NULL)
  if (!is.null(n_total) && n_total > 0 && (n_levels / n_total) > max_density)
    return(NULL)

  levels
}

#' Sanitize a user-supplied string (nfilter.string)
#' @param x Character.
#' @return Character (truncated if too long) or NULL.
#' @export
safe_metadata_string <- function(x) {
  max_len <- as.integer(getOption("nfilter.string",
    getOption("default.nfilter.string", 80)))
  if (is.null(x) || !is.character(x)) return(NULL)
  if (nchar(x) > max_len) return(substr(x, 1, max_len))
  x
}

#' Power-of-2 bucketing for counts
#' @keywords internal
.bucket_count <- function(n) {
  if (is.na(n) || n <= 0) return(0L)
  if (n < 4) return(as.integer(n))
  as.integer(2^round(log2(n)))
}

#' Get the active trust profile name
#' @keywords internal
.get_active_profile <- function() {
  getOption("dsflower.privacy_profile",
    getOption("default.dsflower.privacy_profile", "clinical_default"))
}

#' Get nfilter.subset threshold
#' @keywords internal
.get_nfilter_subset <- function() {
  as.integer(getOption("nfilter.subset",
    getOption("default.nfilter.subset", 3)))
}

#' Get nfilter.tab threshold
#' @keywords internal
.get_nfilter_tab <- function() {
  as.integer(getOption("nfilter.tab",
    getOption("default.nfilter.tab", 3)))
}
