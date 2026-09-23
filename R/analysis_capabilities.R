# Module: Capabilities + Admin DS methods

#' Get clinical imaging analysis capabilities
#'
#' DataSHIELD AGGREGATE method. Reports only client-safe capability names and
#' readiness flags. Filesystem paths, dependency details, scheduler state and
#' server load errors remain administrator-only.
#'
#' @return Named list of capabilities.
#' @export
imagingCapabilitiesDS <- function() {
  .dsimaging_require_literal_arguments()
  models <- tryCatch(.safe_installed_models(), error = function(e) {
    .empty_public_model_bundles()
  })
  runners <- tryCatch(.imaging_runner_health(), error = function(e) {
    data.frame(runner = character(0), present = logical(0),
               stringsAsFactors = FALSE)
  })
  exposed_runners <- c(
    "pyradiomics_extract", "lungmask_infer", "ct_lung_threshold",
    "totalsegmentator_infer", "nnunetv2_predict", "monai_bundle_infer",
    "dicom_convert", "rt_convert", "rt_dose_plan", "wsi_tile",
    "dsimaging_image_preprocess",
    "mask_ops", "imaging_qc_metrics", "imaging_qc_visuals",
    "image_spatial", "image_embeddings")
  if ("runner" %in% names(runners)) {
    runners <- runners[runners$runner %in% exposed_runners, , drop = FALSE]
    runners$runner[runners$runner == "dsimaging_image_preprocess"] <-
      "image_preprocess"
  }
  list(
    dsimaging_version = as.character(utils::packageVersion("dsImaging")),
    profiles = .safe_public_identifiers(list_imaging_radiomics_profiles()),
    models = models,
    runners = runners[, intersect(c("runner", "present"), names(runners)),
                      drop = FALSE],
    admin_enabled = .radiomics_admin_enabled()
  )
}

#' Install a Segmentation Model (admin only)
#'
#' DataSHIELD AGGREGATE method. Protected by dshpc.admin_key.
#' Downloads every file from an administrator-owned, digest-pinned source
#' recipe and registers the fully verified bundle. No paths are accepted.
#'
#' @param admin_key_encoded Character; B64-encoded admin key.
#' @param provider Character; "totalsegmentator", "lungmask", "monai", "nnunetv2".
#' @param task Character; model/task name.
#' @return Named list with install status.
#' @export
imagingInstallModelDS <- function(admin_key_encoded, provider, task) {
  .dsimaging_require_literal_arguments()
  .verify_radiomics_admin(admin_key_encoded)
  provider <- .safe_public_identifier(provider)
  task <- .safe_public_identifier(task)
  if (is.na(provider) || is.na(task)) {
    return(list(status = "failed", provider = "unknown", task = "unknown",
                error = "installation_failed"))
  }

  result <- tryCatch({
    installed <- suppressWarnings(suppressMessages(install_model(provider, task)))
    sha <- installed$manifest_sha256
    if (!is.character(sha) || length(sha) != 1L || is.na(sha) ||
        !grepl("^[0-9a-f]{64}$", sha)) stop("Bundle digest is unavailable.")
    list(status = "installed", provider = provider, task = task,
         manifest_sha256 = sha)
  }, error = function(e) {
    list(status = "failed", provider = provider, task = task,
         error = "installation_failed")
  })

  result
}

#' List Installed Models (no admin needed)
#'
#' DataSHIELD AGGREGATE method.
#'
#' @return Data.frame of installed models.
#' @export
imagingListModelsDS <- function() {
  .dsimaging_require_literal_arguments()
  .safe_installed_models()
}

#' @keywords internal
.safe_installed_models <- function() {
  # Bundle verification diagnostics may include node paths. Only readiness and
  # the pinned public manifest digest cross the analyst boundary.
  models <- tryCatch(
    suppressWarnings(list_installed_models()),
    error = function(e) NULL)
  required <- c("provider", "task", "ready", "manifest_sha256")
  if (!is.data.frame(models) || !all(required %in% names(models)) ||
      nrow(models) == 0L) {
    return(.empty_public_model_bundles())
  }
  digests <- as.character(models$manifest_sha256)
  valid_digest <- !is.na(digests) & grepl("^[0-9a-f]{64}$", digests)
  digests[!valid_digest] <- NA_character_
  data.frame(
    provider = unname(vapply(
      models$provider, .safe_public_identifier, character(1),
      default = "unknown")),
    task = unname(vapply(
      models$task, .safe_public_identifier, character(1),
      default = "unknown")),
    ready = !is.na(models$ready) & models$ready & valid_digest,
    manifest_sha256 = digests,
    stringsAsFactors = FALSE
  )
}

#' @keywords internal
.empty_public_model_bundles <- function() {
  data.frame(provider = character(), task = character(), ready = logical(),
    manifest_sha256 = character(), stringsAsFactors = FALSE)
}

# --- Admin verification (reuses dshpc.admin_key) ---

#' @keywords internal
.radiomics_admin_enabled <- function() {
  key <- .dshpc_option_safe("admin_key")
  !is.null(key) && nzchar(key)
}

#' @keywords internal
.verify_radiomics_admin <- function(admin_key_encoded) {
  expected <- .dshpc_option_safe("admin_key")

  # Decode the same URL-safe B64 JSON transport used by dsHPCClient.
  decoded <- tryCatch({
    parsed <- .dsr_decode(admin_key_encoded)
    if (is.list(parsed)) parsed$.admin_key else parsed
  }, error = function(e) NULL)

  valid_scalar <- function(value) {
    is.character(value) && length(value) == 1L && !is.na(value) &&
      nzchar(value) && nchar(value, type = "bytes") <= 4096L
  }
  candidate <- if (valid_scalar(decoded)) decoded else ""
  reference <- if (valid_scalar(expected)) expected else ""
  candidate_hash <- charToRaw(digest::digest(
    enc2utf8(candidate), algo = "sha256", serialize = FALSE))
  reference_hash <- charToRaw(digest::digest(
    enc2utf8(reference), algo = "sha256", serialize = FALSE))
  difference <- sum(bitwXor(as.integer(candidate_hash),
                            as.integer(reference_hash)))
  if (!valid_scalar(decoded) || !valid_scalar(expected) || difference != 0L) {
    stop("Admin access denied.", call. = FALSE)
  }
  invisible(TRUE)
}

#' Read dshpc option safely (dsImaging doesn't import dsHPC)
#' @keywords internal
.dshpc_option_safe <- function(name) {
  value <- getOption(paste0("dshpc.", name), NULL)
  if (!is.null(value)) return(value)
  value <- getOption(paste0("default.dshpc.", name), NULL)
  if (!is.null(value)) return(value)

  env_name <- paste0("DSHPC_", toupper(gsub("[^A-Za-z0-9]+", "_", name)))
  env_value <- Sys.getenv(env_name, unset = NA_character_)
  if (!is.na(env_value) && nzchar(env_value)) return(env_value)

  NULL
}
