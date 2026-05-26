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
  for (id in names(raw)) {
    if (is.null(raw[[id]]$manifest_uri) && !is.null(raw[[id]]$manifest))
      raw[[id]]$manifest_uri <- raw[[id]]$manifest
  }
  raw
}

#' Persist or update a dataset registry entry
#'
#' Server-side resources can be resolved from an Opal/Armadillo session before a
#' dsHPC worker sees them. Persisting the compact registry entry lets later
#' worker processes resolve the same dataset without depending on the original
#' R session.
#'
#' @keywords internal
register_dataset <- function(dataset_id, manifest_uri, backend,
                             publish = NULL, enabled = TRUE) {
  if (is.null(dataset_id) || !nzchar(dataset_id))
    stop("dataset_id is required.", call. = FALSE)
  if (is.null(manifest_uri) || !nzchar(manifest_uri))
    stop("manifest_uri is required.", call. = FALSE)
  if (is.null(backend) || is.null(backend$type))
    stop("backend is required.", call. = FALSE)

  registry_path <- getOption("dsimaging.registry_path",
    getOption("default.dsimaging.registry_path",
              "/var/lib/dsimaging/registry.yaml"))
  if (identical(getOption("dsimaging.registry_backend",
    getOption("default.dsimaging.registry_backend", "file")), "s3")) {
    return(invisible(FALSE))
  }

  registry <- tryCatch(.load_registry(), error = function(e) list())
  cfg <- backend$config %||% list()
  entry <- list(
    enabled = isTRUE(enabled),
    backend = backend$type,
    manifest_uri = manifest_uri,
    endpoint = cfg$endpoint %||% NULL,
    credentials_ref = cfg$credentials_ref %||% NULL,
    region = cfg$region %||% NULL
  )

  if (identical(backend$type, "s3")) {
    creds <- tryCatch(.resolve_s3_credentials(cfg$credentials_ref),
      error = function(e) NULL)
    if (!is.null(creds)) {
      entry$endpoint <- cfg$endpoint %||% creds$endpoint
      entry$region <- cfg$region %||% creds$region
      entry$credentials_ref <- cfg$credentials_ref %||%
        paste0("resource_", dataset_id)
      store <- getOption("dsimaging.credentials", list())
      store[[entry$credentials_ref]] <- creds
      options(dsimaging.credentials = store)
      .persist_s3_credential(entry$credentials_ref, creds)
    }
  }

  if (!is.null(publish)) {
    pcfg <- publish$config %||% list()
    entry$publish <- list(
      backend = publish$type,
      endpoint = pcfg$endpoint %||% entry$endpoint,
      credentials_ref = pcfg$credentials_ref %||% entry$credentials_ref,
      region = pcfg$region %||% entry$region,
      uri_prefix = pcfg$uri_prefix %||% NULL
    )
  }

  registry[[dataset_id]] <- entry
  raw <- c(list(schema_version = 1L), registry)
  dir.create(dirname(registry_path), recursive = TRUE, showWarnings = FALSE)
  yaml::write_yaml(raw, registry_path)
  tryCatch(Sys.chmod(registry_path, "0660"), error = function(e) NULL)
  invisible(TRUE)
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
    stop("Dataset '", dataset_id, "' not found in registry. Available: ",
         paste(available, collapse = ", "), call. = FALSE)
  }
  if (isFALSE(entry$enabled))
    stop("Dataset '", dataset_id, "' is disabled in registry.",
         call. = FALSE)

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
  registry <- .load_registry()
  registry[vapply(registry, function(entry) !isFALSE(entry$enabled), logical(1))]
}
