# Module: Metadata Disclosure Control
# Shared helpers for safe metadata output across all DS packages.
# Applies DataSHIELD nfilter rules to count-bearing metadata.

#' Project one manifest/configuration value as a public identifier
#'
#' Invalid values are replaced rather than echoed because node-owned free-form
#' strings can otherwise smuggle storage paths or URIs through fields such as
#' asset kind, provider, model task, or modality.
#' @keywords internal
.safe_public_identifier <- function(value, default = NA_character_) {
  if (is.null(value) || is.list(value) || length(value) != 1L) return(default)
  value <- enc2utf8(as.character(value))
  valid <- !is.na(value) && nzchar(value) &&
    nchar(value, type = "bytes") <= 128L &&
    grepl("^[A-Za-z0-9][A-Za-z0-9._:-]*$", value) &&
    !grepl("..", value, fixed = TRUE) &&
    !grepl("^[A-Za-z][A-Za-z0-9+.-]*:", value)
  if (isTRUE(valid)) unname(value) else default
}

#' @keywords internal
.safe_public_identifiers <- function(values) {
  values <- unlist(values, use.names = FALSE)
  if (length(values) == 0L) return(character(0))
  projected <- vapply(values, .safe_public_identifier, character(1))
  unname(unique(projected[!is.na(projected)]))
}

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

  # The DataSHIELD minimum cell-size floor applies to every trust profile.
  # A profile may choose exact versus bucketed admitted counts, but may not
  # expose a count below nfilter.
  if (is.na(n) || n < nfilter) return(NA_integer_)

  # Tier A profiles: exact counts OK
  if (profile %in% c("sandbox_open", "trusted_internal"))
    return(as.integer(n))

  # Tier D profiles: hide very small counts
  if (profile %in% c("clinical_hardened", "high_sensitivity_dp")) {
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
  nfilter <- .get_nfilter_tab()
  admitted <- !is.na(counts) & counts >= nfilter

  # Suppressing only the small cell is insufficient when another Aggregate
  # releases the exact cohort total: subtraction would reconstruct it. Refuse
  # the whole distribution whenever any cell is below the invariant floor.
  if (length(counts) == 0L || any(!admitted)) return(NULL)

  # Hidden in hardened/DP profiles
  if (profile %in% c("clinical_hardened", "high_sensitivity_dp"))
    return(NULL)

  # Exact counts are available only when every cell is independently admitted.
  if (profile %in% c("sandbox_open", "trusted_internal")) {
    return(counts)
  }

  # Bucketed in consortium/clinical/update_noise
  safe <- vapply(counts, function(x) {
    if (is.na(x) || x < nfilter) NA_integer_ else .bucket_count(x)
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
  if (is.na(n) || n <= 0) return(NA_integer_)
  # Use upper power-of-two bounds and a minimum bucket of four. In particular,
  # admitted counts such as 3 are never returned unchanged.
  as.integer(2^ceiling(log2(max(as.numeric(n), 4))))
}

#' Get the active trust profile name
#' @keywords internal
.get_active_profile <- function() {
  profile <- getOption("dsimaging.privacy_profile",
    getOption("default.dsimaging.privacy_profile", NULL))
  if (!is.null(profile)) return(profile)

  # Compatibility for existing node profiles. This reads an option only and
  # does not require dsFlower; the dsImaging-specific key always takes priority.
  getOption("dsflower.privacy_profile",
    getOption("default.dsflower.privacy_profile", "clinical_default"))
}

#' Get nfilter.subset threshold
#' @keywords internal
.get_nfilter_subset <- function() {
  .normalise_nfilter_threshold(getOption("dsimaging.nfilter.subset",
    getOption("default.dsimaging.nfilter.subset",
      getOption("nfilter.subset", getOption("default.nfilter.subset", 3)))))
}

#' Get nfilter.tab threshold
#' @keywords internal
.get_nfilter_tab <- function() {
  .normalise_nfilter_threshold(getOption("dsimaging.nfilter.tab",
    getOption("default.dsimaging.nfilter.tab",
      getOption("nfilter.tab", getOption("default.nfilter.tab", 3)))))
}

#' Validate a DataSHIELD count threshold without allowing configuration to
#' weaken the minimum privacy floor.
#' @keywords internal
.normalise_nfilter_threshold <- function(value, fallback = 3L) {
  numeric_value <- tryCatch(
    suppressWarnings(as.numeric(value)),
    error = function(e) numeric(0))
  if (length(numeric_value) != 1L || is.na(numeric_value) ||
      !is.finite(numeric_value)) {
    return(as.integer(fallback))
  }
  numeric_value <- ceiling(numeric_value)
  if (numeric_value < fallback) return(as.integer(fallback))
  if (numeric_value > .Machine$integer.max) return(.Machine$integer.max)
  as.integer(numeric_value)
}
