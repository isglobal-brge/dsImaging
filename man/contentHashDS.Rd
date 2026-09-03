\name{contentHashDS}
\alias{contentHashDS}
\title{Return a non-disclosive content hash for an imaging resource}
\usage{
contentHashDS(resource_name)
}
\arguments{
\item{resource_name}{Character scalar; DataSHIELD imaging resource name,
dataset id, asset id, alias, or direct \code{s3://bucket/key} URI.}
}
\value{
Single-row list with \code{resource_name}, \code{content_hash},
\code{updated_at}, and \code{source}. Unsupported non-MinIO resources return
\code{content_hash = NA} and \code{source = "unsupported"}.
}
\description{
Returns only resource-level metadata suitable for dsHPC spec deduplication.
When a single-part S3 ETag is used, \code{source = "etag_md5"} records that
the value is a stable content-equivalent MD5 and not a SHA-256 digest.
}
