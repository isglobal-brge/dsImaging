# Module: Administrator-provisioned Model Bundles

#' Get the administrator-owned models directory
#' @keywords internal
.models_dir <- function() {
  .imaging_analysis_option("models_dir", Sys.getenv("DSIMAGING_MODELS",
    unset = file.path(.imaging_analysis_home(), "models")))
}

#' @keywords internal
.model_sources_dir <- function() {
  .imaging_analysis_option("model_sources", file.path(.models_dir(), "sources"))
}

#' Run the shared bundle verifier without provisioning Python or model weights
#' @keywords internal
.model_bundle_command <- function(command, args = character()) {
  python <- .imaging_analysis_option("model_python", unname(Sys.which("python3")))
  if (!is.character(python) || length(python) != 1L ||
      is.na(python) || !nzchar(python)) {
    stop("Model bundle verification requires an administrator-provisioned Python 3 interpreter.",
         call. = FALSE)
  }
  script <- system.file("python", "dsimaging_model_bundles.py", package = "dsImaging")
  if (!nzchar(script)) stop("Model bundle verifier is unavailable.", call. = FALSE)
  result <- processx::run(python, c(script, command,
    "--root", .models_dir(), "--registry", .model_registry_path(), args),
    error_on_status = FALSE, timeout = if (command == "install") Inf else 600)
  if (result$status != 0L) {
    # Detailed diagnostics stay within the server-side administrator API.
    stop("Model bundle ", command, " failed: ", trimws(result$stderr), call. = FALSE)
  }
  jsonlite::fromJSON(result$stdout, simplifyVector = FALSE)
}

#' @keywords internal
.model_bundle_identifier <- function(value, field) {
  if (!is.character(value) || length(value) != 1L || is.na(value) ||
      !grepl("^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$", value) ||
      grepl("..", value, fixed = TRUE)) {
    stop("Invalid model bundle ", field, ".", call. = FALSE)
  }
  value
}

#' Install and register a verified segmentation model bundle
#'
#' Administrator-only provisioning from a pinned source recipe at
#' `<models>/sources/<provider>/<task>.json`. Every file is downloaded and
#' verified before the manifest and its registry digest are published.
#' Existing bundles are reverified; old `.installed` markers are not accepted.
#' No model weights are downloaded during inference.
#'
#' @param provider Character; "totalsegmentator", "lungmask", "monai", "nnunetv2".
#' @param task Character; administrator-registered model/task name.
#' @param force Logical; replace an existing bundle from the administrator recipe.
#' @return Invisibly, verified bundle metadata including `manifest_sha256`.
#' @export
install_model <- function(provider, task, force = FALSE) {
  provider <- .model_bundle_identifier(provider, "provider")
  task <- .model_bundle_identifier(task, "task")
  if (!is.logical(force) || length(force) != 1L || is.na(force)) {
    stop("force must be TRUE or FALSE.", call. = FALSE)
  }
  result <- .model_bundle_command("install", c(
    "--provider", provider, "--task", task, "--sources", .model_sources_dir(),
    if (force) "--force"))
  message("Verified model bundle ", provider, ":", task, " (", result$manifest_sha256, ")")
  invisible(result)
}

#' List registered model bundles and verify their contents
#'
#' Reads the administrator registry and verifies its pinned manifest digest and
#' every model file. Unregistered directories and legacy markers are ignored.
#' A damaged registered bundle is listed with `ready = FALSE`.
#'
#' @return Data frame with name, provider, task, path, installed_at,
#'   manifest_sha256 and ready. Paths are for server administrators only.
#' @export
list_installed_models <- function() {
  empty <- data.frame(name = character(), provider = character(), task = character(),
    path = character(), installed_at = character(), manifest_sha256 = character(),
    ready = logical(), stringsAsFactors = FALSE)
  if (!dir.exists(.model_registry_path())) return(empty)
  rows <- .model_bundle_command("list")
  if (!length(rows)) return(empty)
  do.call(rbind, lapply(rows, function(row) data.frame(
    name = row$name %||% paste0(row$provider, "_", row$task),
    provider = row$provider, task = row$task, path = row$path %||% NA_character_,
    installed_at = row$installed_at %||% NA_character_,
    manifest_sha256 = row$manifest_sha256 %||% NA_character_,
    ready = isTRUE(row$ready), stringsAsFactors = FALSE)))
}
