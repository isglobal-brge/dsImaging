# Module: Imaging Dataset Descriptor
# Package-independent descriptor for an imaging dataset.

#' Create an ImagingDatasetDescriptor
#'
#' Wraps a parsed manifest into dsImaging's package-independent dataset
#' descriptor. Downstream consumers may adapt this public structure without
#' dsImaging importing or inheriting their classes.
#'
#' @param manifest List; a parsed and validated imaging manifest.
#' @return An \code{ImagingDatasetDescriptor}.
#' @export
imaging_dataset_descriptor <- function(manifest) {
  if (is.null(manifest$dataset_id)) {
    stop("Manifest must include dataset_id.", call. = FALSE)
  }

  desc <- list(
    dataset_id  = manifest$dataset_id,
    source_kind = "image_bundle",
    metadata    = manifest$metadata,
    assets      = manifest$assets %||% list(),
    manifest    = manifest,
    table_data  = NULL
  )

  structure(desc, class = "ImagingDatasetDescriptor")
}

#' Create a descriptor from a dataset_id
#'
#' Resolves the dataset_id via the registry, parses the manifest, and
#' returns a descriptor.
#'
#' @param dataset_id Character; the dataset identifier.
#' @return An \code{ImagingDatasetDescriptor}.
#' @export
imaging_descriptor_from_id <- function(dataset_id) {
  resolved <- resolve_dataset(dataset_id)
  manifest <- parse_manifest(resolved$manifest_uri, resolved$backend)
  imaging_dataset_descriptor(manifest)
}

#' Print method for ImagingDatasetDescriptor
#' @param x An ImagingDatasetDescriptor.
#' @param ... Ignored.
#' @export
print.ImagingDatasetDescriptor <- function(x, ...) {
  cat("ImagingDatasetDescriptor\n")
  cat("  dataset_id: ", x$dataset_id, "\n")
  cat("  modality:   ", x$manifest$modality %||% "unknown", "\n")
  cat("  title:      ", x$manifest$title %||% "(untitled)", "\n")
  if (!is.null(x$manifest$task_types)) {
    cat("  task_types: ", paste(x$manifest$task_types, collapse = ", "), "\n")
  }
  if (length(x$assets) > 0) {
    cat("  assets:     ", paste(names(x$assets), collapse = ", "), "\n")
  }
  invisible(x)
}
