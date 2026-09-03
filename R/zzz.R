# Module: Package Hooks

`%||%` <- function(x, y) if (is.null(x)) y else x

# Package-level environment for handle storage and runtime caches
.dsimaging_env <- new.env(parent = emptyenv())

# Session-owned data must not be retained by a package-global registry. Keep it
# in one hidden, locked binding on the DataSHIELD session environment instead;
# when that environment is torn down, handles, workflows, feature tables,
# credentials, taint, and private-asset grants become unreachable together.
.DSIMAGING_SESSION_STATE_BINDING <- ".dsimaging_private_state_v1"
.dsimaging_session_state_marker <- new.env(parent = emptyenv())
.dsimaging_session_tombstone <- new.env(parent = emptyenv())

.imaging_session_state <- function(owner_env, create = FALSE) {
  if (!is.environment(owner_env)) {
    stop("Invalid imaging session environment.", call. = FALSE)
  }
  binding <- .DSIMAGING_SESSION_STATE_BINDING
  if (exists(binding, envir = owner_env, inherits = FALSE)) {
    state <- get(binding, envir = owner_env, inherits = FALSE)
    valid <- is.environment(state) &&
      identical(state$marker, .dsimaging_session_state_marker) &&
      is.environment(state$handles) &&
      is.environment(state$workflows) &&
      is.environment(state$feature_views) &&
      is.environment(state$private_asset_authorizations) &&
      is.environment(state$flags) &&
      bindingIsLocked(binding, owner_env) && environmentIsLocked(state)
    if (!isTRUE(valid)) {
      stop("Imaging private session state is unavailable.", call. = FALSE)
    }
    return(state)
  }
  if (!isTRUE(create)) return(NULL)

  state <- new.env(parent = emptyenv())
  state$marker <- .dsimaging_session_state_marker
  state$handles <- new.env(parent = emptyenv())
  state$workflows <- new.env(parent = emptyenv())
  state$feature_views <- new.env(parent = emptyenv())
  state$private_asset_authorizations <- new.env(parent = emptyenv())
  state$flags <- new.env(parent = emptyenv())
  state$flags$feature_table_exported <- FALSE
  lockEnvironment(state, bindings = TRUE)
  assign(binding, state, envir = owner_env)
  lockBinding(binding, owner_env)
  state
}

.invalidate_imaging_feature_views <- function(state, handle_capability) {
  if (!is.environment(state) ||
      !is.character(handle_capability) || length(handle_capability) != 1L) {
    return(invisible(FALSE))
  }
  capabilities <- ls(state$feature_views, all.names = TRUE)
  for (capability in capabilities) {
    entry <- state$feature_views[[capability]]
    if (is.list(entry) &&
        identical(entry$source_handle_capability, handle_capability)) {
      feature_views <- state$feature_views
      feature_views[[capability]] <- .dsimaging_session_tombstone
    }
  }
  invisible(TRUE)
}

#' @keywords internal
.onLoad <- function(libname, pkgname) {
  resourcer::registerResourceResolver(ImagingDatasetResourceResolver$new())
  tryCatch(.asset_db_prepare_path(), error = function(e) NULL)
  .dsimaging_env$onload_errors <- character(0)
  tryCatch(
    .register_imaging_analysis_runners(),
    error = function(e) {
      .dsimaging_env$onload_errors <- c(.dsimaging_env$onload_errors,
        paste("analysis runner registration failed:", conditionMessage(e)))
    })
  if (requireNamespace("dsHPC", quietly = TRUE)) {
    for (entry in list(
      list(kind = "imaging_asset", fn = .imaging_asset_publisher),
      list(kind = "imaging_radiomics_asset", fn = .radiomics_publisher),
      list(kind = "imaging_radiomics_image_result", fn = .radiomics_image_publisher)
    )) {
      tryCatch(
        dsHPC::register_dshpc_publisher(entry$kind, entry$fn),
        error = function(e) {
          .dsimaging_env$onload_errors <- c(.dsimaging_env$onload_errors,
            paste(entry$kind, "publisher registration failed:",
                  conditionMessage(e)))
        })
    }
  }
}

#' @keywords internal
.onAttach <- function(libname, pkgname) {
  # Re-register in case .onLoad ran in a different context
  resourcer::registerResourceResolver(ImagingDatasetResourceResolver$new())
}

#' @keywords internal
.onDetach <- function(libpath) {
  tryCatch(
    resourcer::unregisterResourceResolver("ImagingDatasetResourceResolver"),
    error = function(e) NULL)
}
