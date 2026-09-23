# dsImaging disclosure-control contract

Reviewed: 2026-09-23. This document defines the analyst-facing DataSHIELD
boundary for imaging resources. It is a release invariant; it does not make the
object store, node filesystem, or server-administrator interfaces public.

## Public resource and session model

An analyst starts from a resource explicitly authorized by the node: either an
Opal/DSLite `imaging+dataset://<endpoint>/<bucket>/datasets/<collection>`
resource or an Armadillo marker Resource whose `format` is exactly
`dsimaging-dataset:<dataset_id>`.
`imagingInitDS()` validates that complete collection and leaves only a
session-bound opaque capability in the DataSHIELD workspace. The manifest,
storage credentials, object routes, metadata rows, patient identifiers, and
backend object are held in a private package registry owned by that session.

The supported client flows are:

1. `dsImagingClient::ds.imaging.init()` assigns the resource and creates an
   imaging handle, after which another trusted server package can consume that
   same-session handle.
2. A client may explicitly assign the resource and call `imagingInitDS()` as
   separate DataSHIELD operations.

`dsFlower` is one such consumer. `dsImaging` has no dependency on `dsFlower`,
and does not pass it a raw manifest, bucket path, credential, or image table.

## Enforced invariants

1. Admission is based on distinct canonical patient identifiers, not image
   rows. The manifest must declare `privacy_unit: patient`, its patient column,
   sample identifier, canonicalization rule, and one optional training label.
   Missing values, duplicate sample identifiers, non-canonical identifiers, or
   fewer patient units than the server-owned `nfilter.subset` threshold fail
   closed. Configuration cannot lower the hard floor of three.
2. Admission always snapshots the complete collection roster. There is no
   analyst-facing sample-list, offset, limit, or arbitrary subset parameter.
   A change to the manifest, metadata-to-patient mapping, object size/hash, or
   publication lock invalidates the handle before or during consumption.
3. Every source object is resolved within the selected collection prefix and
   pinned by its exact route, size, and SHA-256. Cross-collection paths,
   traversal, symbolic-link escapes, missing/extra files, and inferred
   filename-to-patient mappings are rejected.
4. Registered methods reject nested calls and blocks before forcing their
   arguments. Server-only publication helpers and raw dsHPC submission APIs are
   not reachable by composing them below an allowlisted outer call.
5. Dataset enumeration, raw manifests/backends, raw asset details/lineage,
   generation identifiers, exact per-image progress, hashes, paths, and worker
   diagnostics are absent from the registered analyst surface. Legacy exported
   method names remain only as fail-closed stubs for persisted old allowlists.
6. Public metadata projects only bounded identifiers. Patient/sample values and
   storage-like strings are never returned. Patient counts below threshold are
   hidden and admitted counts are normally upper-power-of-two buckets. A label
   distribution is withheld in full if any cell is below `nfilter.tab`, which
   prevents recovery of a suppressed cell by subtraction.
7. Each collection or direct derivation has one durable dsHPC tracking root.
   Its public identifier exposes only the fixed logical kind, coarse state,
   and completion flag. Per-image execution children and their exact count,
   progress, identifiers, bearers, retries, timestamps, paths, errors, logs,
   and outcomes remain hidden on the node. A session-bound workflow capability
   remains available for live orchestration but is not the durable identity.
   It can be reconstructed from the public id in a new session only when an
   imaging handle reauthorizes the same dataset and immutable collection seal.
8. Only a complete, active, globally reusable asset with an immutable
   collection seal can be published as `server_reusable`. A public tracking id
   may assign an opaque output reference containing no asset id, provider
   reference, path, or bytes. On consumption, `dsImaging` resolves that
   reference server-side and revalidates its provider, classification, asset
   state, collection seal, and requested derivation before use.
   Direct primary execution passes that derivation identity to dsHPC as its
   immutable active/completed-reuse fingerprint; dsHPC additionally binds
   package, runner, execution-unit identity, and an administrator-set SHA-256
   runtime revision covering the executable/container/model bundle. dsImaging
   refuses reusable workflow submission if that runtime seal is absent.
9. Derived feature tables can enter a DataSHIELD session only through the
   authorized imaging handle. Their complete patient roster is revalidated;
   only the manifest-declared label may be joined automatically, and no other
   clinical metadata is copied implicitly.
10. Collection runners consume the exact admitted sample map. Radiomics, QC
    metrics and embedding tables require exactly one row per admitted sample.
    RT dose tables have a dedicated long-table contract: unique `(sample_id,
    roi)` keys, the complete distinct sample roster and a fixed numeric schema.
    Each supported ROI group covers that roster; ROI multiplicity never counts
    toward the patient threshold. File outputs require a complete confined
    per-sample artifact manifest. Publication rejects missing, duplicate, extra,
    cross-attributed, symlinked, or hash/size-mismatched outputs. Bounded QC
    previews retain explicit no-file entries for samples omitted by the cap;
    this does not admit or publish a partial patient cohort.
11. Raster images, single-file NIfTI/DICOM, complete multi-slice DICOM series,
    RTSTRUCT conversion, RTDOSE/RTPLAN analysis, self-contained slides and
    conforming administrator-installed MONAI bundles are admitted through exact
    source and output maps. Series list every file with SHA-256 and size, have
    one canonical patient/study/series/frame and require consistent geometry,
    contiguous slices and an expected instance count. RT inputs join by sealed
    sample ID and verify patient and DICOM references. RT conversion and MONAI
    publish one mask per sample. WSI publishes one private tile manifest per
    slide, including zero-tile slides. DICOM SEG, sidecar slides, Analyze
    `.img/.hdr` pairs, missing expected series counts and unsupported geometry
    remain fail-closed. See `DESIGN_ADMISSION.md` for exact contracts.

## Store boundary and multiple collections

One `dsimaging-store` may host multiple collections. Each collection has its
own `datasets/<collection>` prefix and resource endpoint. `dsimaging-admin`
publishes an exact manifest scope atomically and verifies the complete roster,
hashes, sizes, and absence of extra objects. Selecting a collection resource
does not authorize neighbouring collection prefixes.

Store access credentials belong to the node/operator boundary. They must be
provided through protected deployment configuration and must never be returned
by a DataSHIELD method, embedded in a manifest, or logged by a client.
Armadillo locators resolve only through that server-managed registry. Its
marker URL, injected transport JWT, identity, and secret are discarded after
resolution and cannot select or authenticate an object-store backend.

## Trust boundary and residual signals

- RT per-ROI rows are individual-level derived data. The complete table may be
  assigned to an authorized server session, as with radiomics, but is never an
  aggregate response. Only the declared label can be joined automatically;
  downstream packages must handle repeated observations and their own
  disclosure controls. Opaque feature views that require one row per sample
  refuse dose-table fan-out.
- Slides, tiles, coordinates, per-slide tile counts, ROI values, local manifests,
  thumbnails, DICOM headers and all paths remain server-side. Asset catalogues
  expose only opaque identifiers and bounded kind/modality fields. No new
  per-slide count channel is introduced. Existing metadata uses distinct
  patients and its threshold/bucket policy; fan-out cannot inflate that count.
- QC thumbnails are pseudonymous derived assets, not anonymized public images.
  `max_tiles` defaults to 64 (range 1–1024); `max_size` defaults to 192 pixels
  (range 16–4096). The unchanged local CSV schema lists rendered previews and
  remains private. Pseudonymous names do not authorize image disclosure.
- The node operator and trusted server packages can access private storage and
  exact state by design. Installing or registering an unreviewed server package
  can invalidate this contract; the effective Opal/Rock method allowlist must
  be audited after every upgrade.
- Armadillo marker and Resource publication is administrator-only. The format
  locator selects a registry entry but is not itself an authorization token;
  allowing analysts to upload or replace Resource RDS files would invalidate
  the Resource authorization boundary.
- Polling reveals coarse state and elapsed wall time observable by the caller.
  Runners therefore use bounded, fixed workflows and generic failures, but
  infrastructure-level timing remains a deployment-side signal.
- `sandbox_open` and `trusted_internal` profiles may expose exact admitted
  counts. Production analyst endpoints should use a clinical/bucketed profile
  and site-approved `nfilter` values.
- The minimum-patient gate prevents tiny-cohort use; it is not, by itself, a
  differential-privacy guarantee for every downstream algorithm. `dsImaging`
  enforces its own registered methods and validates shared references it
  consumes. After a server-side object is assigned, every other trusted
  DataSHIELD package and its allowlisted methods remain responsible for their
  own output disclosure controls. Provenance is audit metadata, not a
  cross-package policy engine.
