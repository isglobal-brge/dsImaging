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
  # resource_symbol is a STRING (e.g. "img_res"), not the object itself.
  # The ResourceClient is already in the session env from datashield.assign.resource().
  # Pattern matches dsOMOP: omopInitDS("res") -> get("res", parent.frame())
  obj <- get(resource_symbol, envir = parent.frame(), inherits = FALSE)

  # Path 1: Raw Resource from datashield.assign.resource()
  # In Opal, this is a resourcer::Resource object (not yet resolved)
  if (inherits(obj, "Resource") || (is.list(obj) && !is.null(obj$url))) {
    url <- obj$url %||% ""
    if (grepl("^imaging\\+dataset://", url)) {
      # Resolve the imaging+dataset:// URL ourselves
      parsed <- .parse_imaging_url(url)
      if (!is.null(parsed$dataset_id)) {
        resolved <- tryCatch(resolve_dataset(parsed$dataset_id), error = function(e) NULL)
        if (!is.null(resolved)) {
          manifest <- parse_manifest(resolved$manifest_uri, resolved$backend)
          desc <- imaging_dataset_descriptor(manifest)
          return(.make_imaging_handle(desc, resource_symbol, backend = resolved$backend))
        }
      }
      stop("Cannot resolve imaging+dataset:// URL: ", url, call. = FALSE)
    }
  }

  # Path 2: Already-resolved ImagingDatasetResourceClient
  if (inherits(obj, "ImagingDatasetResourceClient")) {
    desc <- obj$asImagingDescriptor()
    return(.make_imaging_handle(desc, resource_symbol, backend = obj$getBackend()))
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
      return(.make_imaging_handle(desc, resource_symbol, backend = resolved$backend))
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
.make_imaging_handle <- function(desc, symbol, backend = NULL) {
  # Count samples from metadata (may need S3 download)
  n_samples <- .count_samples_from_manifest(desc$manifest, backend)
  .assert_min_samples(n_samples, context = paste0("dataset '", desc$dataset_id, "'"))

  handle <- list(
    source      = "imaging_resource",
    dataset_id  = desc$dataset_id,
    descriptor  = desc,
    manifest    = desc$manifest,
    backend     = backend,
    n_samples   = n_samples,
    created_at  = Sys.time()
  )
  key <- paste0("imaging_", symbol)
  assign(key, handle, envir = .dsimaging_env)
  handle
}

#' Get the storage backend from an imaging handle
#'
#' Used by dsRadiomics to access S3/MinIO for dataset images.
#' The backend is stored when imagingInitDS creates the handle.
#'
#' @param handle_symbol Character; the symbol name of the imaging handle.
#' @return A dsimaging_backend object, or NULL if no backend.
#' @export
imagingGetBackendDS <- function(handle_symbol) {
  key <- paste0("imaging_", handle_symbol)
  handle <- get0(key, envir = .dsimaging_env)
  if (is.null(handle)) {
    # Try to get from the calling environment
    handle <- tryCatch(get(handle_symbol, envir = parent.frame()),
                        error = function(e) NULL)
  }
  if (is.null(handle)) return(NULL)
  handle$backend
}

#' Get the manifest from an imaging handle
#'
#' @param handle_symbol Character; the symbol name.
#' @return Parsed manifest list, or NULL.
#' @export
imagingGetManifestDS <- function(handle_symbol) {
  for (sym in c(handle_symbol, "img", "img_res", "imaging", "res")) {
    key <- paste0("imaging_", sym)
    handle <- get0(key, envir = .dsimaging_env)
    if (!is.null(handle) && !is.null(handle$manifest))
      return(handle$manifest)
  }
  NULL
}

#' Get an imaging handle by symbol
#' @keywords internal
.get_imaging_handle <- function(symbol) {
  for (sym in c(symbol, "img", "img_res", "imaging", "res")) {
    key <- paste0("imaging_", sym)
    handle <- get0(key, envir = .dsimaging_env)
    if (!is.null(handle)) return(handle)
  }
  NULL
}

#' List available label sets for an imaging dataset
#'
#' DataSHIELD AGGREGATE method. Returns the label sets defined in the
#' dataset's manifest. The researcher can then choose which label set
#' to use for training.
#'
#' @param handle_symbol Character; the imaging handle symbol.
#' @return A data.frame with columns: name, type, columns, description.
#' @export
imagingLabelsDS <- function(handle_symbol) {
  handle <- .get_imaging_handle(handle_symbol)
  if (is.null(handle) || is.null(handle$manifest))
    return(data.frame(name = character(0), type = character(0),
                      columns = character(0), description = character(0),
                      stringsAsFactors = FALSE))

  labels <- handle$manifest$labels
  if (is.null(labels) || length(labels) == 0)
    return(data.frame(name = character(0), type = character(0),
                      columns = character(0), description = character(0),
                      stringsAsFactors = FALSE))

  rows <- lapply(labels, function(lbl) {
    data.frame(
      name        = lbl$name %||% NA_character_,
      type        = lbl$type %||% NA_character_,
      columns     = paste(lbl$columns %||% character(0), collapse = ", "),
      description = lbl$description %||% NA_character_,
      stringsAsFactors = FALSE
    )
  })
  do.call(rbind, rows)
}

#' Get a specific label set's URI from the manifest
#' @keywords internal
.get_label_uri <- function(manifest, label_set_name) {
  labels <- manifest$labels
  if (is.null(labels)) return(NULL)
  for (lbl in labels) {
    if (identical(lbl$name, label_set_name)) return(lbl$uri)
  }
  NULL
}

#' List available mask assets for an imaging dataset
#'
#' DataSHIELD AGGREGATE method. Queries the asset catalog for segmentation
#' mask assets that are ACTIVE, complete, and linked to the connected dataset.
#'
#' @param handle_symbol Character; the imaging handle symbol.
#' @return A data.frame with columns: alias, provider, status, n_valid, created_at.
#' @export
imagingMasksDS <- function(handle_symbol) {
  handle <- .get_imaging_handle(handle_symbol)
  if (is.null(handle))
    return(data.frame(alias = character(0), provider = character(0),
                      status = character(0), n_valid = integer(0),
                      created_at = character(0), stringsAsFactors = FALSE))

  dataset_id <- handle$dataset_id

  # Query asset_generations for segmentation generations on this dataset
  tryCatch({
    db <- .get_asset_db()
    if (is.null(db)) return(.empty_masks_df())

    gens <- DBI::dbGetQuery(db, paste0(
      "SELECT generation_id, kind, state, spec_json, expected_n, completed_n, ",
      "created_at FROM asset_generations WHERE dataset_id = ? AND ",
      "kind LIKE '%seg%' AND state = 'PUBLISHED'"),
      params = list(dataset_id))

    if (nrow(gens) == 0) return(.empty_masks_df())

    rows <- lapply(seq_len(nrow(gens)), function(i) {
      spec <- tryCatch(
        jsonlite::fromJSON(gens$spec_json[i], simplifyVector = FALSE),
        error = function(e) list())
      provider <- spec$processor %||% spec$segmenter %||% "unknown"
      n_valid <- as.integer(gens$completed_n[i] %||% 0)
      data.frame(
        alias       = gens$generation_id[i],
        provider    = provider,
        status      = if (n_valid >= gens$expected_n[i]) "ready" else "partial",
        n_valid     = n_valid,
        created_at  = as.character(gens$created_at[i]),
        stringsAsFactors = FALSE
      )
    })
    result <- do.call(rbind, rows)
    # Only return ready (non-partial) by default
    result[result$status == "ready", , drop = FALSE]
  }, error = function(e) .empty_masks_df())
}

#' @keywords internal
.empty_masks_df <- function() {
  data.frame(alias = character(0), provider = character(0),
             status = character(0), n_valid = integer(0),
             created_at = character(0), stringsAsFactors = FALSE)
}

#' Get the asset database connection
#' @keywords internal
.get_asset_db <- function() {
  db_path <- getOption("dsimaging.asset_db",
    file.path(getOption("dsimaging.data_dir", "/var/lib/dsimaging"),
              "imaging_assets.sqlite"))
  if (!file.exists(db_path)) return(NULL)
  tryCatch(DBI::dbConnect(RSQLite::SQLite(), db_path),
           error = function(e) NULL)
}

#' Count samples from a manifest's metadata file
#' @keywords internal
.count_samples_from_manifest <- function(manifest, backend = NULL) {
  meta <- manifest$metadata
  if (is.null(meta)) return(NA_integer_)

  # Try local file first
  local_file <- meta$file
  if (!is.null(local_file) && file.exists(local_file)) {
    fmt <- meta$format %||% .guess_format(local_file)
    if (identical(fmt, "parquet") && requireNamespace("arrow", quietly = TRUE))
      return(nrow(arrow::read_parquet(local_file, as_data_frame = FALSE)))
    if (identical(fmt, "csv"))
      return(length(readLines(local_file)) - 1L)
  }

  # Try S3 URI via backend
  uri <- meta$uri
  if (!is.null(uri) && grepl("^s3://", uri) && !is.null(backend)) {
    tmp <- tempfile(fileext = if (grepl("\\.parquet$", uri)) ".parquet" else ".csv")
    on.exit(unlink(tmp), add = TRUE)
    tryCatch({
      backend_get_file(backend, uri, tmp)
      fmt <- meta$format %||% .guess_format(tmp)
      if (identical(fmt, "parquet") && requireNamespace("arrow", quietly = TRUE))
        return(nrow(arrow::read_parquet(tmp, as_data_frame = FALSE)))
      if (identical(fmt, "csv"))
        return(length(readLines(tmp)) - 1L)
    }, error = function(e) NULL)
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
      trust <- tryCatch(dsFlower::flowerTrustProfile(), error = function(e) NULL)
      if (is.null(trust)) trust <- list(min_train_rows = 0)
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
  if (is.null(n_samples) || is.na(n_samples)) {
    stop("Disclosive: ", context, " sample count is unknown. ",
         "Cannot verify disclosure control. Operation blocked.", call. = FALSE)
  }
  if (n_samples < threshold) {
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
