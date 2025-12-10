#' Create Lungmask Segmentation Pipeline
#'
#' Creates a pipeline that runs lungmask segmentation on a CT image.
#'
#' @param image_hash SHA-256 hash of the input CT image.
#' @param model Lungmask model to use. Options:
#'   - "R231" (default): Generic lung segmentation
#'   - "R231CovidWeb": COVID-19 optimized
#'   - "LTRCLobes": Lobe segmentation
#'   - "LTRCLobes_R231": Combined lobe segmentation
#' @param force_cpu Force CPU execution (default: FALSE). When FALSE, GPU is used
#'   if available, with automatic fallback to CPU if not.
#'
#' @return Pipeline definition list ready for submission.
#' @export
#'
#' @examples
#' \dontrun{
#' pipeline <- create_lungmask_pipeline(
#'   image_hash = "abc123...",
#'   model = "R231"
#' )
#' result <- dsHPC::execute_pipeline(config, pipeline)
#' }
create_lungmask_pipeline <- function(image_hash,
                                      model = "R231",
                                      force_cpu = FALSE) {
  # Validate model

valid_models <- c("R231", "R231CovidWeb", "LTRCLobes", "LTRCLobes_R231")
  if (!model %in% valid_models) {
    stop(sprintf("Invalid lungmask model. Must be one of: %s",
                 paste(valid_models, collapse = ", ")))
  }

  node <- dsHPC::create_pipeline_node(
    chain = list(
      list(
        method_name = "lungmask",
        parameters = list(
          model = model,
          force_cpu = force_cpu
        )
      )
    ),
    dependencies = character(0),
    input_file_hash = image_hash
  )

  list(
    nodes = list(
      segmentation = node
    )
  )
}


#' Create PyRadiomics Feature Extraction Pipeline
#'
#' Creates a pipeline that extracts radiomic features from an image with a mask.
#'
#' @param image_hash SHA-256 hash of the input image.
#' @param mask_hash SHA-256 hash of the segmentation mask.
#' @param feature_classes Feature classes to extract. Can be a character vector
#'   or comma-separated string. Options:
#'   - "firstorder": First-order statistics (mean, variance, etc.)
#'   - "shape": Shape-based features (volume, surface area, etc.)
#'   - "glcm": Gray Level Co-occurrence Matrix
#'   - "glrlm": Gray Level Run Length Matrix
#'   - "glszm": Gray Level Size Zone Matrix
#'   - "gldm": Gray Level Dependence Matrix
#'   - "ngtdm": Neighbouring Gray Tone Difference Matrix
#' @param bin_width Bin width for intensity discretization (default: 25).
#'   Smaller values = finer discretization but more computation.
#' @param normalize Whether to normalize the image before feature extraction
#'   (default: FALSE).
#' @param resample_spacing Optional voxel spacing for resampling (e.g., c(1, 1, 1)).
#'   NULL means no resampling.
#'
#' @return Pipeline definition list ready for submission.
#' @export
#'
#' @examples
#' \dontrun{
#' pipeline <- create_pyradiomics_pipeline(
#'   image_hash = "abc123...",
#'   mask_hash = "def456...",
#'   feature_classes = c("firstorder", "shape"),
#'   bin_width = 25
#' )
#' result <- dsHPC::execute_pipeline(config, pipeline)
#' }
create_pyradiomics_pipeline <- function(image_hash,
                                         mask_hash,
                                         feature_classes = c("firstorder", "shape", "glcm",
                                                             "glrlm", "glszm", "gldm", "ngtdm"),
                                         bin_width = 25,
                                         normalize = FALSE,
                                         resample_spacing = NULL) {
  # Convert feature_classes to comma-separated string if vector
  if (is.character(feature_classes) && length(feature_classes) > 1) {
    feature_classes <- paste(feature_classes, collapse = ",")
  }

  # Validate feature classes (sanitize input - only allow known values)
  valid_classes <- c("firstorder", "shape", "glcm", "glrlm", "glszm", "gldm", "ngtdm")
  provided_classes <- trimws(strsplit(feature_classes, ",")[[1]])
  invalid <- setdiff(provided_classes, valid_classes)
  if (length(invalid) > 0) {
    stop(sprintf("Invalid feature classes: %s. Valid options: %s",
                 paste(invalid, collapse = ", "),
                 paste(valid_classes, collapse = ", ")))
  }
  # Reconstruct clean feature_classes string from validated values
  feature_classes <- paste(provided_classes, collapse = ",")

  # Validate bin_width is numeric and reasonable
  if (!is.numeric(bin_width) || bin_width <= 0 || bin_width > 1000) {
    stop("bin_width must be a positive number between 1 and 1000")
  }

  # Build parameters
  params <- list(
    feature_classes = feature_classes,
    bin_width = bin_width,
    normalize = normalize
  )

  if (!is.null(resample_spacing)) {
    params$resample_spacing <- resample_spacing
  }

  node <- dsHPC::create_pipeline_node(
    chain = list(
      list(
        method_name = "pyradiomics",
        parameters = params,
        file_inputs = list(
          image = image_hash,
          mask = mask_hash
        )
      )
    ),
    dependencies = character(0)
  )

  list(
    nodes = list(
      features = node
    )
  )
}


#' Create Combined Lungmask + PyRadiomics Pipeline
#'
#' Creates a pipeline that first segments the lungs using lungmask,
#' then extracts radiomic features using pyradiomics.
#'
#' @param image_hash SHA-256 hash of the input CT image.
#' @param lungmask_model Lungmask model (default: "R231").
#' @param feature_classes Feature classes for pyradiomics (default: all).
#' @param bin_width Bin width for discretization (default: 25).
#' @param normalize Normalize image (default: FALSE).
#' @param force_cpu Force CPU for lungmask (default: FALSE). When FALSE, GPU is
#'   used if available, with automatic fallback to CPU if not.
#' @param resample_spacing Optional resampling spacing for pyradiomics.
#'
#' @return Pipeline definition list ready for submission.
#' @export
#'
#' @details
#' This creates a single-node pipeline with a chained meta-job:
#' 1. lungmask runs first, producing a segmentation mask
#' 2. pyradiomics runs second, using the mask from step 1
#'
#' The mask is passed via \code{$ref:prev/data/mask_base64} which extracts
#' the base64-encoded mask from lungmask's output.
#'
#' @examples
#' \dontrun{
#' pipeline <- create_lungmask_pyradiomics_pipeline(
#'   image_hash = "abc123...",
#'   lungmask_model = "R231",
#'   feature_classes = c("firstorder", "shape", "glcm"),
#'   bin_width = 25
#' )
#' result <- dsHPC::execute_pipeline(config, pipeline)
#' }
create_lungmask_pyradiomics_pipeline <- function(image_hash,
                                                  lungmask_model = "R231",
                                                  feature_classes = c("firstorder", "shape", "glcm",
                                                                      "glrlm", "glszm", "gldm", "ngtdm"),
                                                  bin_width = 25,
                                                  normalize = FALSE,
                                                  force_cpu = FALSE,
                                                  resample_spacing = NULL) {
  # Validate lungmask model
  valid_models <- c("R231", "R231CovidWeb", "LTRCLobes", "LTRCLobes_R231")
  if (!lungmask_model %in% valid_models) {
    stop(sprintf("Invalid lungmask model '%s'. Must be one of: %s",
                 lungmask_model, paste(valid_models, collapse = ", ")))
  }

  # Convert feature_classes to comma-separated string if vector
  if (is.character(feature_classes) && length(feature_classes) > 1) {
    feature_classes <- paste(feature_classes, collapse = ",")
  }

  # Validate feature classes (sanitize input - only allow alphanumeric and commas)
  valid_classes <- c("firstorder", "shape", "glcm", "glrlm", "glszm", "gldm", "ngtdm")
  provided_classes <- trimws(strsplit(feature_classes, ",")[[1]])
  invalid <- setdiff(provided_classes, valid_classes)
  if (length(invalid) > 0) {
    stop(sprintf("Invalid feature classes: %s. Valid options: %s",
                 paste(invalid, collapse = ", "),
                 paste(valid_classes, collapse = ", ")))
  }
  # Reconstruct clean feature_classes string from validated values
  feature_classes <- paste(provided_classes, collapse = ",")

  # Validate bin_width is numeric and reasonable
  if (!is.numeric(bin_width) || bin_width <= 0 || bin_width > 1000) {
    stop("bin_width must be a positive number between 1 and 1000")
  }

  # Build pyradiomics parameters
  pyrad_params <- list(
    feature_classes = feature_classes,
    bin_width = bin_width,
    normalize = normalize
  )

  if (!is.null(resample_spacing)) {
    pyrad_params$resample_spacing <- resample_spacing
  }

  # Single node with chained meta-job: lungmask -> pyradiomics
  processing_node <- dsHPC::create_pipeline_node(
    chain = list(
      # Step 1: Lungmask segmentation
      list(
        method_name = "lungmask",
        parameters = list(
          model = lungmask_model,
          force_cpu = force_cpu
        )
      ),
      # Step 2: PyRadiomics feature extraction
      # $ref:initial = original input image
      # $ref:prev/data/mask_base64 = mask from lungmask output
      list(
        method_name = "pyradiomics",
        parameters = pyrad_params,
        file_inputs = list(
          image = "$ref:initial",
          mask = "$ref:prev/data/mask_base64"
        )
      )
    ),
    dependencies = character(0),
    input_file_hash = image_hash
  )

  list(
    nodes = list(
      processing = processing_node
    )
  )
}


#' Available Lungmask Models
#'
#' Returns information about available lungmask models.
#'
#' @return Data frame with model names and descriptions.
#' @export
#'
#' @examples
#' lungmask_models()
lungmask_models <- function() {
  data.frame(
    model = c("R231", "R231CovidWeb", "LTRCLobes", "LTRCLobes_R231"),
    description = c(
      "Generic lung segmentation (recommended)",
      "COVID-19 optimized lung segmentation",
      "Lobe segmentation",
      "Combined lobe and lung segmentation"
    ),
    stringsAsFactors = FALSE
  )
}


#' Available PyRadiomics Feature Classes
#'
#' Returns information about available pyradiomics feature classes.
#'
#' @return Data frame with feature class names and descriptions.
#' @export
#'
#' @examples
#' pyradiomics_feature_classes()
pyradiomics_feature_classes <- function() {
  data.frame(
    class = c("firstorder", "shape", "glcm", "glrlm", "glszm", "gldm", "ngtdm"),
    description = c(
      "First-order statistics (mean, variance, skewness, etc.)",
      "Shape-based features (volume, surface area, sphericity, etc.)",
      "Gray Level Co-occurrence Matrix (texture)",
      "Gray Level Run Length Matrix (texture)",
      "Gray Level Size Zone Matrix (texture)",
      "Gray Level Dependence Matrix (texture)",
      "Neighbouring Gray Tone Difference Matrix (texture)"
    ),
    n_features = c(19, 17, 24, 16, 16, 14, 5),
    stringsAsFactors = FALSE
  )
}
