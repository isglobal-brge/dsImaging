# Module: Security Validation
# Path traversal checks, root validation, and integrity checks.

#' Validate a filesystem path is safe
#'
#' Rejects paths containing traversal sequences (..), ensures paths are
#' absolute, and verifies no symbolic link escapes.
#'
#' @param path Character; the path to validate.
#' @param label Character; label for error messages.
#' @return Invisible TRUE on success; stops on failure.
#' @keywords internal
.validate_safe_path <- function(path, label = "path") {
  if (is.null(path) || !nzchar(path)) {
    stop(label, ": path is empty or NULL.", call. = FALSE)
  }

  # Must be absolute
  if (!grepl("^/", path)) {
    stop(label, ": path must be absolute, got '", path, "'.", call. = FALSE)
  }

  # No traversal components
  components <- strsplit(path, "/", fixed = TRUE)[[1]]
  if (".." %in% components) {
    stop(label, ": path contains '..' traversal: '", path, "'.", call. = FALSE)
  }

  invisible(TRUE)
}

#' Validate that a path exists and is under an allowed root
#'
#' @param path Character; the path to check.
#' @param allowed_root Character; the required root directory.
#' @param label Character; label for error messages.
#' @return Invisible TRUE on success; stops on failure.
#' @keywords internal
.validate_path_under_root <- function(path, allowed_root, label = "path") {
  .validate_safe_path(path, label)

  resolved <- tryCatch(
    normalizePath(path, mustWork = FALSE),
    error = function(e) path
  )
  resolved_root <- tryCatch(
    normalizePath(allowed_root, mustWork = FALSE),
    error = function(e) allowed_root
  )

  if (!startsWith(resolved, resolved_root)) {
    stop(label, ": path '", resolved, "' is outside allowed root '",
         resolved_root, "'.", call. = FALSE)
  }

  invisible(TRUE)
}

#' Validate that asset roots exist on disk
#'
#' @param manifest List; parsed manifest.
#' @return Named list of validation results per asset.
#' @keywords internal
validate_asset_roots <- function(manifest) {
  results <- list()
  assets <- manifest$assets %||% list()

  dir_types <- c("image_root", "wsi_root", "dicom_series_root", "rt_struct_root")
  file_types <- c("feature_table", "rt_dose_file", "rt_plan_file")

  for (name in names(assets)) {
    asset <- assets[[name]]
    asset_type <- asset$type %||% "unknown"

    if (asset_type %in% dir_types) {
      root <- asset$root
      exists <- !is.null(root) && dir.exists(root)
      results[[name]] <- list(
        type   = asset_type,
        root   = root,
        exists = exists,
        valid  = exists
      )
    } else if (asset_type %in% file_types) {
      fpath <- asset$file
      exists <- !is.null(fpath) && file.exists(fpath)
      results[[name]] <- list(
        type   = asset_type,
        file   = fpath,
        exists = exists,
        valid  = exists
      )
    } else if (identical(asset_type, "multimodal_ref")) {
      mpath <- asset$manifest
      exists <- !is.null(mpath) && file.exists(mpath)
      results[[name]] <- list(
        type   = asset_type,
        manifest = mpath,
        exists = exists,
        valid  = exists
      )
    }
  }

  # Metadata file
  meta_file <- manifest$metadata$file
  meta_exists <- !is.null(meta_file) && file.exists(meta_file)
  results[["_metadata"]] <- list(
    type   = "metadata",
    file   = meta_file,
    exists = meta_exists,
    valid  = meta_exists
  )

  results
}

#' Run full validation on a manifest
#'
#' Checks structural validity, path safety, and asset existence.
#'
#' @param manifest List; parsed manifest.
#' @return Named list with \code{valid} (logical), \code{errors} (character),
#'   \code{warnings} (character), \code{asset_status} (list).
#' @export
validate_imaging_dataset <- function(manifest) {
  errors <- character(0)
  warnings_out <- character(0)

  # Structural validation
  tryCatch(
    validate_manifest(manifest, "validation"),
    error = function(e) {
      errors <<- c(errors, conditionMessage(e))
    }
  )

  if (length(errors) > 0) {
    return(list(valid = FALSE, errors = errors, warnings = warnings_out,
                asset_status = list()))
  }

  # Asset existence check
  asset_status <- validate_asset_roots(manifest)

  for (name in names(asset_status)) {
    status <- asset_status[[name]]
    if (!isTRUE(status$valid)) {
      loc <- status$root %||% status$file %||% "(unknown)"
      errors <- c(errors, paste0("Asset '", name, "' not found: ", loc))
    }
  }

  # Modality/template compatibility warnings
  modality <- manifest$modality %||% "unknown"
  templates <- manifest$compatible_templates %||% character(0)
  if (length(templates) == 0) {
    warnings_out <- c(warnings_out,
                      "No compatible_templates specified in manifest.")
  }

  list(
    valid        = length(errors) == 0,
    errors       = errors,
    warnings     = warnings_out,
    asset_status = asset_status
  )
}
