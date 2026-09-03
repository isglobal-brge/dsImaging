\name{start_content_hash_listener}
\alias{start_content_hash_listener}
\alias{stop_content_hash_listener}
\title{Start or stop the MinIO content-hash listener}
\usage{
start_content_hash_listener()

stop_content_hash_listener()
}
\value{
Listener handle invisibly for \code{start_content_hash_listener()}; logical
success invisibly for \code{stop_content_hash_listener()}.
}
\description{
\code{start_content_hash_listener()} runs an initial reconciliation via
\code{seed_content_hashes()} and then starts either an HTTP webhook listener
(\code{notify_mode = "webhook"}) or a polling worker
(\code{notify_mode = "poll"}).
}
