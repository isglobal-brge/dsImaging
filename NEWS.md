# dsImaging 0.3.4

* File-backend datasets whose manifest declares only the documented
  `metadata.uri` field now initialize correctly: `.count_samples_from_manifest()`
  counts samples from a local absolute `metadata.uri` (previously only
  `metadata.file` and `s3://` URIs were honored, so init failed with
  "sample count is unknown").
* Fixed the worker-side drip feed crash "Drip-feed failed: $ operator is
  invalid for atomic vectors": `resolve_dataset()` now returns an explicit
  `manifest = NULL` element, so `resolved$manifest` no longer partial-matches
  `manifest_uri` and the manifest is parsed from its URI as intended. This
  unblocks server-side auto-submission of batches after the first
  (registry-resolved datasets, no analyst session attached).
* Collection workflow methods (scan, status, publish) now return exact item
  counts instead of power-of-2 bucketed values; a 6-image collection reports
  6/6 rather than 8/8. These counts are operational telemetry for a workflow
  that already returns per-sample identifiers to the same caller; dataset
  metadata endpoints keep `safe_metadata_count()` bucketing.
