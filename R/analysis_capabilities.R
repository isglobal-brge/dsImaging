# Module: Capabilities + Admin DS methods

#' Get clinical imaging analysis capabilities
#'
#' DataSHIELD AGGREGATE method. Reports the installed analysis profiles,
#' segmentation models, Python environments, runner registrations, scheduler
#' status, and package load issues.
#'
#' @return Named list of capabilities.
#' @export
imagingCapabilitiesDS <- function() {
  list(
    dsimaging_version = as.character(utils::packageVersion("dsImaging")),
    profiles = list_imaging_radiomics_profiles(),
    models = tryCatch(list_installed_models(), error = function(e) data.frame()),
    envs = tryCatch(list_imaging_analysis_envs(), error = function(e) data.frame()),
    runners = tryCatch(.imaging_runner_health(), error = function(e) data.frame()),
    scheduler = tryCatch(dsHPC::hpcSchedulerStatusDS(), error = function(e) list(error = conditionMessage(e))),
    onload_errors = .dsimaging_env$onload_errors %||% character(0),
    admin_enabled = .radiomics_admin_enabled()
  )
}

#' Install a Segmentation Model (admin only)
#'
#' DataSHIELD AGGREGATE method. Protected by dshpc.admin_key.
#' Downloads model weights to the server.
#'
#' @param admin_key_encoded Character; B64-encoded admin key.
#' @param provider Character; "totalsegmentator", "lungmask", "monai", "nnunetv2".
#' @param task Character; model/task name.
#' @return Named list with install status.
#' @export
imagingInstallModelDS <- function(admin_key_encoded, provider, task) {
  .verify_radiomics_admin(admin_key_encoded)

  result <- tryCatch({
    install_model(provider, task)
    list(status = "installed", provider = provider, task = task)
  }, error = function(e) {
    list(status = "failed", provider = provider, task = task,
         error = conditionMessage(e))
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
  list_installed_models()
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

  if (is.null(expected) || !nzchar(expected))
    stop("Admin access is not enabled. Set dshpc.admin_key option.", call. = FALSE)

  # Decode the same URL-safe B64 JSON transport used by dsHPCClient.
  decoded <- tryCatch({
    parsed <- .dsr_decode(admin_key_encoded)
    if (is.list(parsed)) parsed$.admin_key else parsed
  }, error = function(e) admin_key_encoded)

  if (is.null(decoded) || !nzchar(decoded))
    stop("Access denied: admin_key required.", call. = FALSE)
  if (!identical(decoded, expected))
    stop("Access denied: invalid admin_key.", call. = FALSE)
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
