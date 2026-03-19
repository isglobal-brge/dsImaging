# Module: YAML Manifest Parsing and Validation
# Parses dataset manifest files that describe imaging datasets.

#' Parse an imaging dataset manifest
#'
#' Reads and validates a YAML manifest file describing an imaging dataset.
#'
#' @param path Character; path to the manifest YAML file.
#' @return A validated manifest list.
#' @keywords internal
parse_manifest <- function(path) {
  if (!file.exists(path)) {
    stop("Manifest file not found: ", path, call. = FALSE)
  }

  manifest <- yaml::read_yaml(path)
  validate_manifest(manifest, path)
  manifest
}

#' Validate a parsed manifest
#'
#' Checks required fields, types, and security constraints.
#'
#' @param manifest List; parsed manifest.
#' @param source Character; source path for error messages.
#' @return Invisible TRUE on success; stops on failure.
#' @keywords internal
validate_manifest <- function(manifest, source = "manifest") {
  # Required top-level fields

  required <- c("version", "dataset_id", "modality", "metadata")
  missing <- setdiff(required, names(manifest))
  if (length(missing) > 0) {
    stop("Manifest '", source, "' missing required fields: ",
         paste(missing, collapse = ", "), call. = FALSE)
  }

  # Version check
  if (!identical(as.integer(manifest$version), 1L)) {
    stop("Unsupported manifest version: ", manifest$version,
         ". Only version 1 is supported.", call. = FALSE)
  }

  # dataset_id format: dotted segments

  if (!grepl("^[a-z0-9][a-z0-9._-]*$", manifest$dataset_id)) {
    stop("Invalid dataset_id format: '", manifest$dataset_id, "'. ",
         "Use lowercase alphanumeric with dots/hyphens/underscores.",
         call. = FALSE)
  }

  # Metadata validation
  meta <- manifest$metadata
  if (is.null(meta$file)) {
    stop("Manifest metadata must include 'file' field.", call. = FALSE)
  }
  .validate_safe_path(meta$file, "metadata.file")

  if (!is.null(meta$format)) {
    valid_formats <- c("parquet", "csv")
    if (!meta$format %in% valid_formats) {
      stop("Unsupported metadata format: '", meta$format, "'. ",
           "Supported: ", paste(valid_formats, collapse = ", "), call. = FALSE)
    }
  }

  # Asset validation
  if (!is.null(manifest$assets)) {
    for (name in names(manifest$assets)) {
      validate_asset(manifest$assets[[name]], name, source)
    }
  }

  invisible(TRUE)
}

#' Validate a single asset entry
#'
#' @param asset List; the asset definition.
#' @param name Character; asset name for error messages.
#' @param source Character; manifest source for error messages.
#' @keywords internal
validate_asset <- function(asset, name, source = "manifest") {
  asset_type <- asset$type %||% "unknown"

  # Directory-based assets (images, masks, WSI, DICOM series, RT objects)
  dir_types <- c("image_root", "wsi_root", "dicom_series_root", "rt_struct_root")
  if (asset_type %in% dir_types) {
    if (is.null(asset$root)) {
      stop("Asset '", name, "' in ", source, " missing 'root' field.",
           call. = FALSE)
    }
    .validate_safe_path(asset$root, paste0("assets.", name, ".root"))

    # WSI-specific: validate optional tile/magnification params
    if (identical(asset_type, "wsi_root")) {
      if (!is.null(asset$tile_size) && (!is.numeric(asset$tile_size) ||
          asset$tile_size < 1)) {
        stop("Asset '", name, "': tile_size must be a positive integer.",
             call. = FALSE)
      }
      if (!is.null(asset$magnification) && !is.numeric(asset$magnification)) {
        stop("Asset '", name, "': magnification must be numeric.",
             call. = FALSE)
      }
    }
    return(invisible(TRUE))
  }

  # File-based assets (feature tables, RT dose/plan files)
  file_types <- c("feature_table", "rt_dose_file", "rt_plan_file")
  if (asset_type %in% file_types) {
    if (is.null(asset$file)) {
      stop("Asset '", name, "' in ", source, " missing 'file' field.",
           call. = FALSE)
    }
    .validate_safe_path(asset$file, paste0("assets.", name, ".file"))
    return(invisible(TRUE))
  }

  # Multimodal reference: points to another manifest
  if (identical(asset_type, "multimodal_ref")) {
    if (is.null(asset$manifest)) {
      stop("Asset '", name, "' in ", source,
           " missing 'manifest' field (path to referenced manifest).",
           call. = FALSE)
    }
    .validate_safe_path(asset$manifest, paste0("assets.", name, ".manifest"))
    return(invisible(TRUE))
  }

  stop("Unknown asset type '", asset_type, "' for asset '", name,
       "' in ", source, ". Supported types: ",
       paste(c(dir_types, file_types, "multimodal_ref"), collapse = ", "),
       ".", call. = FALSE)
}
