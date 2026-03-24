# Module: Manifest Parsing and Validation
#
# Manifests describe imaging datasets: assets (images, masks),
# metadata, content hash index, and sample manifests.
# All paths are URIs (s3:// or local absolute paths).

#' Parse an imaging dataset manifest
#'
#' Fetches the manifest YAML from the given backend and validates it.
#'
#' @param manifest_uri Character; URI of the manifest YAML.
#' @param backend A dsimaging_backend object (from resolve_dataset).
#' @return A validated manifest list.
#' @keywords internal
parse_manifest <- function(manifest_uri, backend) {
  manifest <- backend_fetch_manifest(backend, manifest_uri)
  validate_manifest(manifest, manifest_uri)
  manifest
}

#' Validate a parsed manifest
#'
#' @param manifest List; parsed manifest.
#' @param source Character; source URI for error messages.
#' @keywords internal
validate_manifest <- function(manifest, source = "manifest") {
  # schema_version
  sv <- manifest$schema_version
  if (is.null(sv) || as.integer(sv) < 1L)
    stop("Manifest '", source, "' missing or invalid schema_version.",
         call. = FALSE)

  # dataset_id
  did <- manifest$dataset_id
  if (is.null(did) || !grepl("^[a-z0-9][a-z0-9._-]*$", did))
    stop("Manifest '", source, "': invalid or missing dataset_id.",
         call. = FALSE)

  # metadata
  meta <- manifest$metadata
  if (is.null(meta) || is.null(meta$uri))
    stop("Manifest '", source, "': metadata.uri required.", call. = FALSE)
  .validate_uri(meta$uri, "metadata.uri")

  if (!is.null(meta$format)) {
    if (!meta$format %in% c("parquet", "csv"))
      stop("Unsupported metadata format: ", meta$format, call. = FALSE)
  }

  # Assets
  if (!is.null(manifest$assets)) {
    for (name in names(manifest$assets)) {
      validate_asset(manifest$assets[[name]], name, source)
    }
  }

  # Labels (optional)
  if (!is.null(manifest$labels)) {
    for (lbl in manifest$labels) {
      if (is.null(lbl$name))
        stop("Label set in ", source, " missing 'name'.", call. = FALSE)
      if (is.null(lbl$uri))
        stop("Label set '", lbl$name, "' in ", source, " missing 'uri'.",
             call. = FALSE)
      .validate_uri(lbl$uri, paste0("labels.", lbl$name, ".uri"))
    }
  }

  invisible(TRUE)
}

#' Validate a single asset entry
#' @keywords internal
validate_asset <- function(asset, name, source = "manifest") {
  if (is.null(asset$uri))
    stop("Asset '", name, "' in ", source, ": uri required.", call. = FALSE)
  .validate_uri(asset$uri, paste0("assets.", name, ".uri"))

  kind <- asset$kind
  valid_kinds <- c("image_root", "mask_root", "feature_table",
                    "wsi_root", "dicom_series_root",
                    "rt_struct_root", "rt_dose_file", "rt_plan_file",
                    "multimodal_ref")
  if (!is.null(kind) && !kind %in% valid_kinds)
    stop("Asset '", name, "': unknown kind '", kind, "'.", call. = FALSE)

  invisible(TRUE)
}

#' Validate a URI (s3:// or local absolute path)
#' @keywords internal
.validate_uri <- function(uri, label = "uri") {
  if (is.null(uri) || !nzchar(uri))
    stop(label, ": URI is empty.", call. = FALSE)
  if (grepl("^s3://", uri)) {
    parsed <- .parse_s3_uri(uri)
    if (!nzchar(parsed$bucket))
      stop(label, ": invalid S3 URI: ", uri, call. = FALSE)
    return(invisible(TRUE))
  }
  if (!startsWith(uri, "/"))
    stop(label, ": must be absolute path or s3:// URI: ", uri, call. = FALSE)
  invisible(TRUE)
}
