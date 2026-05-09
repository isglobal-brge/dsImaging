# Module: Clinical Imaging Analysis Options

#' Read a dsImaging analysis option.
#'
#' Option chain:
#' `dsimaging.analysis.<name>` -> `default.dsimaging.analysis.<name>` ->
#' `dsimaging.<name>` -> `default.dsimaging.<name>` -> legacy
#' `dsradiomics.<name>` -> `default.dsradiomics.<name>` -> default.
#'
#' @keywords internal
.imaging_analysis_option <- function(name, default = NULL) {
  getOption(paste0("dsimaging.analysis.", name),
    getOption(paste0("default.dsimaging.analysis.", name),
      getOption(paste0("dsimaging.", name),
        getOption(paste0("default.dsimaging.", name),
          getOption(paste0("dsradiomics.", name),
            getOption(paste0("default.dsradiomics.", name), default))))))
}

#' Root directory for dsImaging analysis state.
#' @keywords internal
.imaging_analysis_home <- function() {
  .imaging_analysis_option("home",
    getOption("dsimaging.data_dir",
      getOption("default.dsimaging.data_dir", "/var/lib/dsimaging")))
}
