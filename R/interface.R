# Module: DataSHIELD Exposed Methods
# Assign and aggregate methods for imaging dataset management.

#' Initialize Imaging Handle
#'
#' DataSHIELD ASSIGN method. Creates an imaging dataset handle from a
#' resource that was assigned via \code{datashield.assign.resource()}.
#' The resource must be an \code{imaging+dataset://} URL resolved by
#' \code{ImagingDatasetResourceResolver}.
#'
#' @param resource_symbol Character; symbol name of the assigned resource.
#' @return An imaging handle object (assigned server-side).
#' @export
imagingInitDS <- function(resource_symbol) {
  obj <- get(resource_symbol, envir = parent.frame())

  if (inherits(obj, "ImagingDatasetResourceClient")) {
    desc <- obj$asImagingDescriptor()
    handle <- list(
      source      = "imaging_resource",
      dataset_id  = desc$dataset_id,
      descriptor  = desc,
      manifest    = desc$manifest,
      created_at  = Sys.time()
    )

    # Store in package environment for aggregate methods
    key <- paste0("imaging_", resource_symbol)
    assign(key, handle, envir = .dsimaging_env)

    return(handle)
  }

  # Also accept FlowerDatasetDescriptor / ImagingDatasetDescriptor
  if (inherits(obj, "ImagingDatasetDescriptor") ||
      inherits(obj, "FlowerDatasetDescriptor")) {
    handle <- list(
      source      = "descriptor",
      dataset_id  = obj$dataset_id,
      descriptor  = obj,
      manifest    = obj$manifest,
      created_at  = Sys.time()
    )
    key <- paste0("imaging_", resource_symbol)
    assign(key, handle, envir = .dsimaging_env)
    return(handle)
  }

  stop("Symbol '", resource_symbol, "' is not an ImagingDatasetResourceClient ",
       "or ImagingDatasetDescriptor. Assign an imaging+dataset:// resource first.",
       call. = FALSE)
}

#' List Available Imaging Datasets
#'
#' DataSHIELD AGGREGATE method. Returns the dataset_ids and titles of
#' all enabled datasets in the server's registry.
#'
#' @return Data.frame with columns: dataset_id, title, modality, enabled.
#' @export
imagingListDatasetsDS <- function() {
  datasets <- tryCatch(list_datasets(), error = function(e) list())

  if (length(datasets) == 0) {
    return(data.frame(
      dataset_id = character(0),
      title = character(0),
      modality = character(0),
      stringsAsFactors = FALSE
    ))
  }

  rows <- lapply(names(datasets), function(id) {
    entry <- datasets[[id]]
    # Try to read title from manifest
    title <- tryCatch({
      m <- parse_manifest(entry$manifest)
      m$title %||% "(untitled)"
    }, error = function(e) "(error reading manifest)")

    modality <- tryCatch({
      m <- parse_manifest(entry$manifest)
      m$modality %||% "unknown"
    }, error = function(e) "unknown")

    data.frame(
      dataset_id = id,
      title = title,
      modality = modality,
      stringsAsFactors = FALSE
    )
  })

  do.call(rbind, rows)
}

#' Get Imaging Dataset Metadata
#'
#' DataSHIELD AGGREGATE method. Returns disclosure-safe metadata about
#' the imaging dataset (column names, sample count, modality, etc.)
#' without exposing actual data values.
#'
#' @param handle_symbol Character; symbol of the imaging handle.
#' @return Named list with metadata summary.
#' @export
imagingMetadataDS <- function(handle_symbol) {
  handle <- .getImagingHandle(handle_symbol)
  manifest <- handle$manifest

  meta <- manifest$metadata
  n_samples <- NULL
  columns <- NULL

  if (!is.null(meta$file) && file.exists(meta$file)) {
    fmt <- meta$format %||% .guess_format(meta$file)
    if (identical(fmt, "parquet") && requireNamespace("arrow", quietly = TRUE)) {
      schema <- arrow::read_parquet(meta$file, as_data_frame = FALSE)
      n_samples <- nrow(schema)
      columns <- names(schema)
    } else if (identical(fmt, "csv")) {
      hdr <- utils::read.csv(meta$file, nrows = 1)
      columns <- names(hdr)
      # Count rows without loading all data
      n_samples <- length(readLines(meta$file)) - 1L
    }
  }

  list(
    dataset_id    = manifest$dataset_id,
    title         = manifest$title %||% "(untitled)",
    modality      = manifest$modality %||% "unknown",
    task_types    = manifest$task_types %||% character(0),
    templates     = manifest$compatible_templates %||% character(0),
    n_samples     = n_samples,
    columns       = columns,
    id_col        = meta$id_col %||% NULL,
    label_col     = meta$label_col %||% NULL,
    n_assets      = length(manifest$assets %||% list())
  )
}

#' Validate Imaging Dataset
#'
#' DataSHIELD AGGREGATE method. Runs security and integrity checks on
#' the imaging dataset.
#'
#' @param handle_symbol Character; symbol of the imaging handle.
#' @return Named list with validation results.
#' @export
imagingValidateDS <- function(handle_symbol) {
  handle <- .getImagingHandle(handle_symbol)
  validate_imaging_dataset(handle$manifest)
}

#' List Dataset Assets
#'
#' DataSHIELD AGGREGATE method. Returns the names and types of assets
#' available in the imaging dataset.
#'
#' @param handle_symbol Character; symbol of the imaging handle.
#' @return Data.frame with columns: name, type.
#' @export
imagingAssetsDS <- function(handle_symbol) {
  handle <- .getImagingHandle(handle_symbol)
  assets <- handle$manifest$assets %||% list()

  if (length(assets) == 0) {
    return(data.frame(
      name = character(0), type = character(0),
      stringsAsFactors = FALSE
    ))
  }

  data.frame(
    name = names(assets),
    type = vapply(assets, function(a) a$type %||% "unknown", character(1)),
    stringsAsFactors = FALSE
  )
}

#' Retrieve an imaging handle from storage
#'
#' @param symbol Character; handle symbol name.
#' @return The imaging handle.
#' @keywords internal
.getImagingHandle <- function(symbol) {
  # Try caller environments (DSLite)
  for (depth in 1:3) {
    env <- tryCatch(sys.frame(-(depth)), error = function(e) NULL)
    if (!is.null(env) && exists(symbol, envir = env, inherits = FALSE)) {
      obj <- get(symbol, envir = env, inherits = FALSE)
      if (is.list(obj) && "descriptor" %in% names(obj)) {
        return(obj)
      }
    }
  }

  # Global environment (Opal/Rock)
  if (exists(symbol, envir = .GlobalEnv, inherits = FALSE)) {
    obj <- get(symbol, envir = .GlobalEnv, inherits = FALSE)
    if (is.list(obj) && "descriptor" %in% names(obj)) {
      return(obj)
    }
  }

  # Package environment
  key <- paste0("imaging_", symbol)
  if (exists(key, envir = .dsimaging_env)) {
    return(get(key, envir = .dsimaging_env))
  }

  stop("No imaging handle for symbol '", symbol,
       "'. Call imagingInitDS first.", call. = FALSE)
}
