# Module: Administrator-owned Segmentation Model Registry

#' Get model registry path
#' @keywords internal
.model_registry_path <- function() {
  .imaging_analysis_option("model_registry", file.path(.models_dir(), "registry"))
}

#' List registered segmentation model bundles
#'
#' @return Data frame with name, provider, task, path, installed_at,
#'   manifest_sha256 and ready, after verifying registered bundle contents.
#' @export
list_segmentation_models <- function() {
  list_installed_models()
}

#' Get a verified segmentation model by its administrator-assigned name
#' @keywords internal
.get_model_config <- function(model_name) {
  model_name <- .model_bundle_identifier(model_name, "name")
  models <- list_installed_models()
  found <- which(models$name == model_name & models$ready)
  if (length(found) != 1L) return(NULL)
  as.list(models[found, , drop = FALSE])
}

#' Register an existing verified segmentation model bundle
#'
#' Server-side administrator utility. The bundle must already have a complete
#' manifest at `<models>/<provider>/<task>/manifest.json`. Registration checks
#' every file and provider dependency before pinning the manifest SHA-256 in
#' the protected registry. Arbitrary external model paths are not accepted.
#'
#' @param name Administrator-assigned model name.
#' @param provider Provider identifier: "lungmask", "monai", "nnunetv2", or
#'   "totalsegmentator".
#' @param task Provider-specific task/model name.
#' @param path Canonical bundle directory below the configured models root.
#' @param python_deps Retained for compatibility; must be NULL. The manifest
#'   pins the provider version.
#' @param extra Retained for compatibility; must be empty. Metadata belongs in
#'   the verified manifest.
#' @return Invisibly, the path to the pinned registry entry.
#' @export
register_segmentation_model <- function(name, provider, task, path,
                                         python_deps = NULL, extra = list()) {
  name <- .model_bundle_identifier(name, "name")
  provider <- .model_bundle_identifier(provider, "provider")
  task <- .model_bundle_identifier(task, "task")
  if (!is.character(path) || length(path) != 1L || is.na(path) || !nzchar(path)) {
    stop("A canonical model bundle path is required.", call. = FALSE)
  }
  if (!is.null(python_deps) || !is.list(extra) || length(extra)) {
    stop("Model metadata must be recorded in the verified bundle manifest.", call. = FALSE)
  }
  .model_bundle_command("register", c("--provider", provider, "--task", task,
    "--path", path, "--name", name))
  invisible(file.path(.model_registry_path(), provider, paste0(task, ".json")))
}
