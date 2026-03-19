# Module: Dataset Registry
# Maps dataset_id to manifest path via server-side YAML configuration.

#' Load the dataset registry
#'
#' Reads the registry YAML file from the path specified in the server
#' option \code{dsimaging.registry_path}.
#'
#' @return Named list mapping dataset_id to registry entry.
#' @keywords internal
.load_registry <- function() {
  registry_path <- getOption("dsimaging.registry_path",
                    getOption("default.dsimaging.registry_path", NULL))

  # Batteries-included: try default path if no option set
  if (is.null(registry_path) || !nzchar(registry_path)) {
    registry_path <- "/var/lib/dsimaging/registry.yaml"
  }

  if (!file.exists(registry_path)) {
    stop("Dataset registry not found at: ", registry_path,
         ". Create it or set dsimaging.registry_path option.", call. = FALSE)
  }

  yaml::read_yaml(registry_path)
}

#' Resolve a dataset_id to its manifest path
#'
#' Looks up the dataset_id in the registry and returns the manifest path
#' if the dataset is enabled.
#'
#' @param dataset_id Character; the dataset identifier.
#' @return Character; path to the manifest YAML file.
#' @keywords internal
resolve_dataset <- function(dataset_id) {
  registry <- .load_registry()

  entry <- registry[[dataset_id]]
  if (is.null(entry)) {
    available <- names(registry)
    stop("Dataset '", dataset_id, "' not found in registry. ",
         "Available: ", paste(available, collapse = ", "), call. = FALSE)
  }

  if (!isTRUE(entry$enabled)) {
    stop("Dataset '", dataset_id, "' is disabled in the registry.",
         call. = FALSE)
  }

  manifest_path <- entry$manifest
  if (is.null(manifest_path) || !file.exists(manifest_path)) {
    stop("Manifest for '", dataset_id, "' not found at: ",
         manifest_path %||% "(not specified)", call. = FALSE)
  }

  manifest_path
}

#' List all enabled datasets in the registry
#'
#' @return Named list of dataset entries that are enabled.
#' @keywords internal
list_datasets <- function() {
  registry <- .load_registry()
  enabled <- Filter(function(entry) isTRUE(entry$enabled), registry)
  enabled
}
