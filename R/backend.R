# Module: Storage Backend Abstraction
#
# All dataset storage operations (read, write, list, head) go through this
# layer. The backend type (file or s3) is resolved from the registry and
# hidden from the user behind imaging+dataset://dataset_id.
#
# Dispatch is by backend$type. Each function has a file and s3 implementation.

#' Create a storage backend object
#'
#' @param type Character; "file" or "s3".
#' @param config Named list; backend-specific configuration.
#'   For s3: endpoint, credentials_ref, region (optional).
#'   For file: empty list.
#' @return A list with class "dsimaging_backend".
#' @export
storage_backend <- function(type, config = list()) {
  type <- match.arg(type, c("file", "s3"))
  if (identical(type, "s3")) {
    if (!requireNamespace("aws.s3", quietly = TRUE))
      stop("aws.s3 package required for S3 backend. ",
           "Install with: install.packages('aws.s3')", call. = FALSE)
  }
  structure(list(type = type, config = config), class = "dsimaging_backend")
}

#' Download a file from the backend to a local destination
#'
#' @param backend A dsimaging_backend object.
#' @param uri Character; the URI to fetch (s3://... or /local/path).
#' @param dest Character; local destination path.
#' @param overwrite Logical.
#' @return Character; the dest path (invisibly).
#' @export
backend_get_file <- function(backend, uri, dest, overwrite = FALSE) {
  if (!overwrite && file.exists(dest)) return(invisible(dest))
  dir.create(dirname(dest), recursive = TRUE, showWarnings = FALSE)
  switch(backend$type,
    file = .backend_get_file_local(uri, dest),
    s3   = .backend_get_file_s3(backend, uri, dest),
    stop("Unknown backend type: ", backend$type, call. = FALSE))
  invisible(dest)
}

#' Upload a local file to the backend
#'
#' @param backend A dsimaging_backend object.
#' @param local_path Character; local file to upload.
#' @param uri Character; destination URI.
#' @return Invisible URI.
#' @export
backend_put_file <- function(backend, local_path, uri) {
  if (!file.exists(local_path))
    stop("Local file not found: ", local_path, call. = FALSE)
  switch(backend$type,
    file = .backend_put_file_local(local_path, uri),
    s3   = .backend_put_file_s3(backend, local_path, uri),
    stop("Unknown backend type: ", backend$type, call. = FALSE))
  invisible(uri)
}

#' Get metadata for a URI without downloading
#'
#' @param backend A dsimaging_backend object.
#' @param uri Character.
#' @return Named list with exists (logical), size (integer), last_modified (POSIXct),
#'   or NULL if the URI does not exist.
#' @export
backend_head <- function(backend, uri) {
  switch(backend$type,
    file = .backend_head_local(uri),
    s3   = .backend_head_s3(backend, uri),
    stop("Unknown backend type: ", backend$type, call. = FALSE))
}

#' List objects under a URI prefix
#'
#' @param backend A dsimaging_backend object.
#' @param prefix Character; URI prefix.
#' @return Character vector of full URIs.
#' @export
backend_list <- function(backend, prefix) {
  switch(backend$type,
    file = .backend_list_local(prefix),
    s3   = .backend_list_s3(backend, prefix),
    stop("Unknown backend type: ", backend$type, call. = FALSE))
}

#' Fetch and parse a YAML manifest from the backend
#'
#' @param backend A dsimaging_backend object.
#' @param uri Character; URI of the YAML file.
#' @return Parsed YAML as a named list.
#' @export
backend_fetch_manifest <- function(backend, uri) {
  switch(backend$type,
    file = yaml::yaml.load_file(uri),
    s3 = {
      tmp <- tempfile(fileext = ".yaml")
      on.exit(unlink(tmp), add = TRUE)
      .backend_get_file_s3(backend, uri, tmp)
      yaml::yaml.load_file(tmp)
    },
    stop("Unknown backend type: ", backend$type, call. = FALSE))
}

#' Upload a local directory to the backend (recursive)
#'
#' @param backend A dsimaging_backend object.
#' @param local_dir Character; local directory.
#' @param uri_prefix Character; destination URI prefix.
#' @return Invisible uri_prefix.
#' @export
backend_put_directory <- function(backend, local_dir, uri_prefix) {
  files <- list.files(local_dir, recursive = TRUE, full.names = TRUE)
  base <- normalizePath(local_dir)
  for (f in files) {
    rel <- sub(paste0("^", gsub("([\\[\\]()])", "\\\\\\1", base), "/?"), "",
               normalizePath(f))
    dest_uri <- if (backend$type == "file")
      file.path(uri_prefix, rel) else paste0(uri_prefix, "/", rel)
    backend_put_file(backend, f, dest_uri)
  }
  invisible(uri_prefix)
}

# ---------------------------------------------------------------------------
# File backend implementations
# ---------------------------------------------------------------------------

.backend_get_file_local <- function(uri, dest) {
  if (!file.exists(uri))
    stop("Local file not found: ", uri, call. = FALSE)
  if (normalizePath(uri, mustWork = FALSE) != normalizePath(dest, mustWork = FALSE))
    file.copy(uri, dest, overwrite = TRUE)
}

.backend_put_file_local <- function(local_path, uri) {
  dir.create(dirname(uri), recursive = TRUE, showWarnings = FALSE)
  file.copy(local_path, uri, overwrite = TRUE)
}

.backend_head_local <- function(uri) {
  if (!file.exists(uri)) return(NULL)
  info <- file.info(uri)
  list(exists = TRUE, size = as.integer(info$size),
       last_modified = info$mtime)
}

.backend_list_local <- function(prefix) {
  if (!dir.exists(prefix)) return(character(0))
  list.files(prefix, full.names = TRUE, recursive = TRUE)
}

# ---------------------------------------------------------------------------
# S3 backend implementations (delegate to backend_s3.R)
# ---------------------------------------------------------------------------

.backend_get_file_s3 <- function(backend, uri, dest) {
  parsed <- .parse_s3_uri(uri)
  config <- .resolve_backend_s3_config(backend)
  .s3_download(config, parsed$bucket, parsed$key, dest)
}

.backend_put_file_s3 <- function(backend, local_path, uri) {
  parsed <- .parse_s3_uri(uri)
  config <- .resolve_backend_s3_config(backend)
  .s3_upload(config, local_path, parsed$bucket, parsed$key)
}

.backend_head_s3 <- function(backend, uri) {
  parsed <- .parse_s3_uri(uri)
  config <- .resolve_backend_s3_config(backend)
  .s3_head(config, parsed$bucket, parsed$key)
}

.backend_list_s3 <- function(backend, prefix) {
  parsed <- .parse_s3_uri(prefix)
  config <- .resolve_backend_s3_config(backend)
  keys <- .s3_list(config, parsed$bucket, parsed$key)
  vapply(keys, function(k) .build_s3_uri(parsed$bucket, k), character(1))
}

#' Resolve S3 config from backend object
#' @keywords internal
.resolve_backend_s3_config <- function(backend) {
  creds <- .resolve_s3_credentials(backend$config$credentials_ref)
  list(
    endpoint = backend$config$endpoint %||% creds$endpoint %||% "",
    access_key = creds$access_key,
    secret_key = creds$secret_key,
    region = backend$config$region %||% creds$region %||% "us-east-1"
  )
}
