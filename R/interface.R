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

  # Path 1: Raw Resource from datashield.assign.resource()
  # In Opal, this is a resourcer::Resource object (not yet resolved)
  if (inherits(obj, "Resource") || (is.list(obj) && !is.null(obj$url))) {
    url <- obj$url %||% ""
    if (grepl("^imaging\\+dataset://", url)) {
      # Resolve the imaging+dataset:// URL ourselves
      parsed <- .parse_imaging_url(url)
      if (!is.null(parsed$dataset_id)) {
        resolved <- resolve_dataset(parsed$dataset_id)
        manifest <- parse_manifest(resolved$manifest_uri, resolved$backend)
      } else {
        stop("Cannot resolve imaging+dataset:// URL: ", url, call. = FALSE)
      }
      desc <- imaging_dataset_descriptor(manifest)
      return(.make_imaging_handle(desc, resource_symbol))
    }
  }

  # Path 2: Already-resolved ImagingDatasetResourceClient
  if (inherits(obj, "ImagingDatasetResourceClient")) {
    desc <- obj$asImagingDescriptor()
    return(.make_imaging_handle(desc, resource_symbol))
  }

  # Path 3: FlowerDatasetDescriptor / ImagingDatasetDescriptor
  if (inherits(obj, "ImagingDatasetDescriptor") ||
      inherits(obj, "FlowerDatasetDescriptor")) {
    return(.make_imaging_handle(obj, resource_symbol))
  }

  # Path 4: dataset_id string (convenience for server-side scripting)
  if (is.character(obj) && length(obj) == 1L && grepl("^[a-z0-9]", obj)) {
    tryCatch({
      resolved <- resolve_dataset(obj)
      manifest <- parse_manifest(resolved$manifest_uri, resolved$backend)
      desc <- imaging_dataset_descriptor(manifest)
      return(.make_imaging_handle(desc, resource_symbol))
    }, error = function(e) NULL)
  }

  stop("Symbol '", resource_symbol, "' is not a recognized imaging resource. ",
       "Supported: Resource (imaging+dataset://), ImagingDatasetResourceClient, ",
       "ImagingDatasetDescriptor, or dataset_id string.",
       call. = FALSE)
}

#' Create an imaging handle from a descriptor
#'
#' Also performs disclosure check: blocks creation if the dataset has
#' fewer samples than the nfilter threshold.
#'
#' @keywords internal
.make_imaging_handle <- function(desc, symbol) {
  # Count samples from metadata file to enforce nfilter
  n_samples <- .count_samples_from_manifest(desc$manifest)
  .assert_min_samples(n_samples, context = paste0("dataset '", desc$dataset_id, "'"))

  handle <- list(
    source      = "imaging_resource",
    dataset_id  = desc$dataset_id,
    descriptor  = desc,
    manifest    = desc$manifest,
    n_samples   = n_samples,
    created_at  = Sys.time()
  )
  key <- paste0("imaging_", symbol)
  assign(key, handle, envir = .dsimaging_env)
  handle
}

#' Count samples from a manifest's metadata file
#' @keywords internal
.count_samples_from_manifest <- function(manifest) {
  meta <- manifest$metadata
  if (is.null(meta) || is.null(meta$file) || !file.exists(meta$file)) {
    return(NA_integer_)
  }

  fmt <- meta$format %||% .guess_format(meta$file)
  if (identical(fmt, "parquet") && requireNamespace("arrow", quietly = TRUE)) {
    return(nrow(arrow::read_parquet(meta$file, as_data_frame = FALSE)))
  }
  if (identical(fmt, "csv")) {
    return(length(readLines(meta$file)) - 1L)
  }
  NA_integer_
}

# --- Disclosure controls ---

#' Get the nfilter threshold for minimum sample counts
#'
#' Uses DataSHIELD's standard \code{nfilter.subset} option, with
#' dsFlower's \code{dsflower.min_train_rows} as an additional floor
#' if dsFlower is loaded.
#'
#' @return Integer; the minimum number of samples to avoid disclosure.
#' @keywords internal
.nfilter_threshold <- function() {
  # Standard DataSHIELD nfilter
  nfilter <- as.numeric(getOption("nfilter.subset",
                                   getOption("default.nfilter.subset", 3)))

  # If dsFlower is loaded, also consider its trust profile minimum
  if (requireNamespace("dsFlower", quietly = TRUE)) {
    tryCatch({
      trust <- dsFlower:::.flowerTrustProfile()
      nfilter <- max(nfilter, trust$min_train_rows)
    }, error = function(e) NULL)
  }

  as.integer(nfilter)
}

#' Bucket a count to avoid exact disclosure
#'
#' Replaces exact counts with bucketed ranges when below a threshold,
#' following DataSHIELD disclosure conventions.
#'
#' @param n Integer; the exact count.
#' @param threshold Integer; minimum count for exact reporting.
#' @return Integer; the original count if above threshold, or NA.
#' @keywords internal
.safe_count <- function(n, threshold = NULL) {
  if (is.null(threshold)) threshold <- .nfilter_threshold()
  if (is.null(n) || is.na(n)) return(NA_integer_)
  if (n < threshold) return(NA_integer_)
  as.integer(n)
}

#' Assert that a dataset has enough samples
#'
#' @param n_samples Integer; number of samples.
#' @param context Character; context for error message.
#' @keywords internal
.assert_min_samples <- function(n_samples, context = "dataset") {
  threshold <- .nfilter_threshold()
  if (!is.null(n_samples) && !is.na(n_samples) && n_samples < threshold) {
    stop("Disclosive: ", context, " has fewer than ", threshold,
         " samples. Operation blocked.", call. = FALSE)
  }
  invisible(TRUE)
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

  # Disclosure control: suppress exact n_samples if below nfilter
  safe_n <- .safe_count(n_samples)

  list(
    dataset_id    = manifest$dataset_id,
    title         = manifest$title %||% "(untitled)",
    modality      = manifest$modality %||% "unknown",
    task_types    = manifest$task_types %||% character(0),
    templates     = manifest$compatible_templates %||% character(0),
    n_samples     = safe_n,
    columns       = columns,
    id_col        = meta$id_col %||% NULL,
    label_col     = meta$label_col %||% NULL,
    n_assets      = length(manifest$assets %||% list()),
    n_samples_sufficient = !is.na(safe_n)
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
