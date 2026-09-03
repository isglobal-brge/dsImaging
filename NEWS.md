# dsImaging 0.3.6

* Private handles, workflows, feature views, export taint, and asset grants now
  live in locked session-owned state. Session teardown releases their private
  resources without explicit destroy, and destroying an imaging handle also
  invalidates its dependent feature views.
* Added an opaque, session-bound feature view for dsFlower. It binds a complete
  feature table to the admitted collection seal and exact sample-to-patient
  roster without placing the table or patient mapping in the DataSHIELD
  workspace. Public multi-column selections use a scalar encoded transport so
  they remain compatible with the strict expression boundary. Legacy raw
  feature-table loading remains available, but marks the session so the table
  cannot be laundered through a subset or copy into dsFlower.
* Collection downloads now carry and use exact S3 object VersionIds for images
  and masks. Worker maps, caches, radiomics, segmentation chains, and derived
  asset identities include the resolved source asset and exact mask map, so an
  alias change or object replacement cannot reuse stale output.
* Mask assets use their own validated sample index even when their storage root
  is identical to the image root; catalog aliases are resolved through the
  exact output manifest and admitted roster.
* Public label distributions include every operator-approved level before the
  disclosure threshold is applied; an unobserved approved level therefore
  suppresses the complete distribution instead of revealing a zero.
* TIFF images and masks are supported by the SimpleITK/PyRadiomics collection
  path. NRRD and MHA remain supported only when self-contained; detached MHD,
  NRRD/MHA sidecars, multi-file DICOM, and WSI inputs fail before processing.

# dsImaging 0.3.5

* Derived assets, aliases, fingerprints, sample manifests, and reusable
  generations are now bound to the immutable collection seal. Identical
  dataset names in independently published collections cannot share results or
  lineage, and unsealed legacy assets remain private.
* Label distributions require an operator-approved finite public vocabulary;
  undeclared values and labels equal to sample or patient identifiers fail
  closed. Published feature tables likewise require safe, exact public column
  names before they can be loaded into an analysis session.
* The imaging preprocessing runner now uses the namespaced physical identifier
  `dsimaging_image_preprocess`, avoiding collision with dsHPC built-ins. Worker
  contexts explicitly carry only the credential path needed after dsHPC clears
  the inherited process environment.
* Per-image dsHPC jobs now use durable opaque correlation tokens instead of
  sample identifiers in tags. S3 staging paths and Python artifact names no
  longer retain identifiable filename prefixes, and worker progress/error logs
  do not print private sample identifiers.
* Feature-asset metadata joins and S3 operations now suppress node-local paths,
  endpoints, and backend diagnostics at the registered DataSHIELD boundary.
* Imaging `ResourceClient` objects refuse generic data-frame coercion, closing
  the path that could otherwise discard collection provenance before patient-
  level admission.
* Optional clinical runner containers now require immutable sha256 image
  references; mutable tags are rejected before runner registration.
* Administrator-key failures now use one constant diagnostic and compare
  fixed-length SHA-256 values without a short-circuiting string comparison.
* Handle and workflow destroy methods now leave an opaque retry tombstone for
  the assign transport, so a failed client-side symbol removal never destroys
  the only identity needed to finish cleanup.
* Collection runners now consume an exact snapshot-provided sample map instead
  of deriving patient identifiers from filenames or recursive directory scans.
  Source objects are size/hash verified before processing, and non-tabular
  outputs require a complete, confined per-sample artifact manifest before
  publication.
* Multi-file DICOM series, RT, WSI, and MONAI analyst-facing workflows now fail
  closed until their input and output formats provide a verified exact patient
  association. Single-file DICOM conversion uses the admitted snapshot mapping;
  QC visuals process the complete roster and always anonymize filenames.
* Collection runners retain exact item counts and per-sample identifiers only
  for server-internal full-roster orchestration. Registered analyst methods
  return coarse workflow state and an authorized opaque result asset; they do
  not return exact progress counts, generation ids, hashes, or sample ids.
* Initialized resources are represented by session-scoped opaque handles backed
  by an immutable, hash-verified collection snapshot. Patient membership and
  the configured disclosure threshold are checked before any downstream
  workflow; `dsFlower` can consume the same handle lazily without receiving
  storage credentials, object paths, or a raw descriptor.
* Registered methods now reject nested expressions, sanitize backend failures,
  and fail closed for workflow formats whose complete patient association
  cannot yet be verified.

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
* Collection workflow methods (scan, status, publish) returned exact item
  counts instead of power-of-2 bucketed values. Version 0.3.5 supersedes this
  analyst-visible behavior with coarse state and opaque authorized results.
