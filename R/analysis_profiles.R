# Module: Radiomics Profile Registry
# Manages PyRadiomics extraction profiles (YAML settings files).

# Fixed public metadata is separate from the immutable historical YAML.
# The 2014 supplement (ncomms5006-s1.pdf, Figure 1 and feature 16) specifies
# Compactness2. The replication documents its sphericity-cubed equivalence:
# https://pmc.ncbi.nlm.nih.gov/articles/PMC6805885/ (Data processing).
.aerts_profile_metadata <- function(profile_name) {
  switch(profile_name,
    aerts_signature_v1 = list(
      description = paste("Aerts-inspired historical four-feature client selection:",
        "Energy, Compactness1, original and wavelet-HLH GLRLM",
        "RunLengthNonUniformity. The unchanged YAML also enables other",
        "wavelet candidates when no selected_features filter is supplied."),
      selected_features = c("original_firstorder_Energy",
        "original_shape_Compactness1", "original_glrlm_RunLengthNonUniformity",
        "wavelet-HLH_glrlm_RunLengthNonUniformity")),
    aerts_signature_v2 = list(
      description = paste("Published Aerts four-feature selection: Energy,",
        "Compactness2 (sphericity cubed), original and wavelet-HLH GLRLM",
        "GrayLevelNonUniformity; historical PyRadiomics settings retained."),
      selected_features = c("original_firstorder_Energy",
        "original_shape_Compactness2", "original_glrlm_GrayLevelNonUniformity",
        "wavelet-HLH_glrlm_GrayLevelNonUniformity")),
    NULL)
}

#' List available radiomics profiles bundled with dsImaging
#' @export
list_imaging_radiomics_profiles <- function() {
  # Bundled profiles
  bundled_dir <- system.file("profiles", package = "dsImaging")
  profiles <- character(0)
  if (nzchar(bundled_dir) && dir.exists(bundled_dir)) {
    profiles <- sub("\\.yaml$", "", list.files(bundled_dir, "\\.yaml$"))
  }

  # Admin-installed profiles
  admin_dir <- file.path(.imaging_analysis_option("home", "/var/lib/dsimaging"), "profiles")
  if (dir.exists(admin_dir)) {
    profiles <- c(profiles, sub("\\.yaml$", "", list.files(admin_dir, "\\.yaml$")))
  }

  unique(profiles)
}

#' Get a radiomics profile path
#' @keywords internal
.get_profile_path <- function(profile_name) {
  if (!grepl("^[a-zA-Z0-9_]+$", profile_name)) return(NULL)

  # Admin override
  admin_path <- file.path(.imaging_analysis_option("home", "/var/lib/dsimaging"),
    "profiles", paste0(profile_name, ".yaml"))
  if (file.exists(admin_path)) return(admin_path)

  # Bundled
  bundled_path <- system.file("profiles", paste0(profile_name, ".yaml"),
    package = "dsImaging")
  if (nzchar(bundled_path) && file.exists(bundled_path)) return(bundled_path)

  NULL
}

#' Read a radiomics profile as a list
#'
#' @param profile_name Profile name without the `.yaml` suffix.
#' @return Parsed profile as a list.
#' @export
read_imaging_radiomics_profile <- function(profile_name) {
  path <- .get_profile_path(profile_name)
  if (is.null(path)) stop("Profile not found: ", profile_name, call. = FALSE)
  if (requireNamespace("yaml", quietly = TRUE)) {
    yaml::read_yaml(path)
  } else {
    jsonlite::fromJSON(readLines(path, warn = FALSE), simplifyVector = FALSE)
  }
}

#' List available radiomics profiles
#' @export
list_radiomics_profiles <- function() {
  list_imaging_radiomics_profiles()
}

#' Read a radiomics profile as a list
#'
#' @param profile_name Profile name without the `.yaml` suffix.
#' @return Parsed profile as a list.
#' @export
read_radiomics_profile <- function(profile_name) {
  read_imaging_radiomics_profile(profile_name)
}
