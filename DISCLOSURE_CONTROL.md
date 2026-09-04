# dsImaging disclosure-control contract

Reviewed: 2026-09-03. This document defines the analyst-facing DataSHIELD
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
7. Analytical workflows are handle-bound, private, and represented in the
   workspace by opaque workflow capabilities. Public status exposes coarse
   state only. Raw dsHPC job identifiers, bearers, retries, timestamps, paths,
   errors, logs, and per-item outcomes remain on the node.
8. Derived feature tables can enter a DataSHIELD session only through the
   authorized imaging handle. Their complete patient roster is revalidated;
   only the manifest-declared label may be joined automatically, and no other
   clinical metadata is copied implicitly.
9. Collection runners consume the exact admitted sample map. Tabular outputs
   require exactly one row per admitted sample; file outputs require a complete
   confined per-sample artifact manifest. Publication rejects missing,
   duplicate, extra, cross-attributed, symlinked, or hash/size-mismatched
   outputs.
10. Raster images, single-file NIfTI, and single-file DICOM can follow this
    exact mapping. Multi-file DICOM series, RT, WSI, and MONAI analyst workflows
    remain fail-closed until their complete input/output patient association is
    represented and verified. Analyze `.img/.hdr` pairs are not a supported
    single-file NIfTI substitute.

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
  differential-privacy guarantee for every downstream algorithm. Each trusted
  consumer remains responsible for its own output disclosure controls.
