# Module: Dataset Registry
#
# Maps dataset_id to manifest URI + backend config via YAML.
# The registry itself can live locally or in S3.

#' Load the dataset registry
#'
#' @return Named list mapping dataset_id to registry entries.
#' @keywords internal
.load_registry <- function() {
  backend_type <- getOption("dsimaging.registry_backend",
    getOption("default.dsimaging.registry_backend", "file"))
  registry_uri <- getOption("dsimaging.registry_uri",
    getOption("dsimaging.registry_path",
      getOption("default.dsimaging.registry_path",
                "/var/lib/dsimaging/registry.yaml")))

  if (identical(backend_type, "s3")) {
    backend <- storage_backend("s3", config = list(
      endpoint = getOption("dsimaging.s3_endpoint"),
      credentials_ref = getOption("dsimaging.s3_credentials_ref")))
    raw <- backend_fetch_manifest(backend, registry_uri)
  } else {
    if (!file.exists(registry_uri))
      stop("Registry not found: ", registry_uri, call. = FALSE)
    raw <- yaml::yaml.load_file(registry_uri)
  }

  sv <- raw$schema_version
  if (is.null(sv) || as.integer(sv) < 1L)
    stop("Registry missing or invalid schema_version.", call. = FALSE)

  # Remove schema_version from entries
  raw$schema_version <- NULL
  raw
}

#' Resolve a dataset_id to its backend, manifest, and publish config
#'
#' @param dataset_id Character.
#' @return Named list: dataset_id, backend, manifest_uri, publish, entry.
#' @keywords internal
resolve_dataset <- function(dataset_id) {
  registry <- .load_registry()

  entry <- registry[[dataset_id]]
  if (is.null(entry)) {
    available <- names(registry)
    stop("Dataset '", dataset_id, "' not found. Available: ",
         paste(available, collapse = ", "), call. = FALSE)
  }

  backend_type <- entry$backend %||% "file"
  backend <- storage_backend(backend_type, config = list(
    endpoint = entry$endpoint,
    credentials_ref = entry$credentials_ref,
    region = entry$region
  ))

  # Build publish backend (may differ from source backend)
  publish <- NULL
  if (!is.null(entry$publish)) {
    pub_type <- entry$publish$backend %||% backend_type
    publish <- storage_backend(pub_type, config = list(
      endpoint = entry$publish$endpoint %||% entry$endpoint,
      credentials_ref = entry$publish$credentials_ref %||% entry$credentials_ref,
      region = entry$publish$region %||% entry$region,
      uri_prefix = entry$publish$uri_prefix
    ))
  }

  list(
    dataset_id = dataset_id,
    backend = backend,
    manifest_uri = entry$manifest_uri,
    publish = publish,
    entry = entry
  )
}

#' List all datasets in the registry
#'
#' @return Named list of dataset entries.
#' @keywords internal
list_datasets <- function() {
  .load_registry()
}
