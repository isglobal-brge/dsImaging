# Module: Clinical Imaging Analysis Options

#' Read a server-side dsImaging option with an environment fallback.
#'
#' Worker daemons are separate R processes and do not inherit Rock/DSLite R
#' options. Deployment environment variables therefore provide the same
#' non-secret path/reference settings to both processes.
#' @keywords internal
.imaging_option <- function(name, default = NULL) {
  value <- getOption(paste0("dsimaging.", name),
    getOption(paste0("default.dsimaging.", name), NULL))
  if (!is.null(value)) return(value)
  env_name <- paste0(
    "DSIMAGING_", toupper(gsub("[^A-Za-z0-9]+", "_", name)))
  env_value <- Sys.getenv(env_name, unset = NA_character_)
  if (!is.na(env_value) && nzchar(env_value)) return(env_value)
  default
}

#' Read a dsImaging analysis option.
#'
#' Option chain:
#' `dsimaging.analysis.<name>` -> `default.dsimaging.analysis.<name>` ->
#' `dsimaging.<name>` -> `default.dsimaging.<name>` ->
#' `DSIMAGING_ANALYSIS_<NAME>` -> `DSIMAGING_<NAME>` -> default.
#'
#' @keywords internal
.imaging_analysis_option <- function(name, default = NULL) {
  value <- getOption(paste0("dsimaging.analysis.", name),
    getOption(paste0("default.dsimaging.analysis.", name),
      getOption(paste0("dsimaging.", name),
        getOption(paste0("default.dsimaging.", name), NULL))))
  if (!is.null(value)) return(value)
  suffix <- toupper(gsub("[^A-Za-z0-9]+", "_", name))
  for (env_name in c(paste0("DSIMAGING_ANALYSIS_", suffix),
                     paste0("DSIMAGING_", suffix))) {
    env_value <- Sys.getenv(env_name, unset = NA_character_)
    if (!is.na(env_value) && nzchar(env_value)) return(env_value)
  }
  default
}

#' Root directory for dsImaging analysis state.
#' @keywords internal
.imaging_analysis_home <- function() {
  .imaging_analysis_option("home",
    .imaging_option("data_dir", "/var/lib/dsimaging"))
}

#' Path to the administrator-managed worker credential store.
#' @keywords internal
.imaging_credentials_path <- function() {
  .imaging_option("credentials_path", file.path(
    .imaging_option("data_dir", "/var/lib/dsimaging"),
    "credentials.yaml"))
}
