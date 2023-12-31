% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/features.R
\name{calculate_radiomic_features_R}
\alias{calculate_radiomic_features_R}
\title{Calculate Radiomic Features}
\usage{
calculate_radiomic_features_R(image, mask, config = "default")
}
\arguments{
\item{config}{A character string specifying the configuration to use. Available options: "default", "config2", ...}
}
\value{
A list of computed radiomic features.
}
\description{
This function calculates radiomic features based on a given original image,
its mask, and a set of parameters.
}
\examples{
\dontrun{
features <- calculate_radiomic_features_R("/path/to/original/image", "/path/to/mask", "/path/to/parameters")
print(features)
}
}
