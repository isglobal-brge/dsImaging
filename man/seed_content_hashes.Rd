\name{seed_content_hashes}
\alias{seed_content_hashes}
\title{Seed the resource content-hash index from configured MinIO buckets}
\usage{
seed_content_hashes()
}
\value{
Invisibly, a data frame of indexed objects.
}
\description{
Walks every configured bucket and stores a stable non-disclosive hash for each
object. The operation is idempotent and may take minutes on large stores.
}
