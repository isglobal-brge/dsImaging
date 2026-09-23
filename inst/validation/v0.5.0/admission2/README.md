# dsImaging 0.5.0 second admission validation

Task: `DSIMAGING_RAISE2_2026-09-23`. Recorded 2026-09-23.

Tested source: `743673eec3254cc152312bf96db08e9843fc41f2` on `feat/admit-rt-wsi-monai`, following
initial admission source/receipt commit `1693c7da0d559d2adb04d30a97e29ee04981e73e`.
The package remains 0.5.0. All commands ran against a clean detached checkout
at `.validation/admission2/checkout`; the captured before/after git statuses
are empty. This receipt is committed subsequently and contains no code changes.
Earlier `v0.5.0/` records are preserved.

## Results

Complete suite: **275 test cases, 1465 passed expectations, 0 failures,
0 errors, 0 warnings, 1 skipped test**. Source build succeeded.
`R CMD check --no-multiarch`: **Status: OK**, **0 errors, 0 warnings, 0 notes**.
Installed-package tests independently report **1465 passes, 0 failures,
0 warnings and 1 skip**. Repeated runs are not added together.

The sole skip remains the optional live MinIO integration test because
`DSIMAGING_RUN_MINIO_TESTS=1` was not set. All new SEG, labelled-dose and
DataSHIELD integration tests ran. `test-cases.csv` gives per-case results;
`results.csv` gives the package totals and source commit. Captured logs retain
all substantive output, normalizing line endings and trailing whitespace only.

## Exact commands

Worktree creation, from the server repository:

```sh
mkdir -p .validation/admission2/logs
git worktree add --detach .validation/admission2/checkout HEAD
```

The following foreground script ran in
`/Users/david/Documents/GitHub/dsimaging-fix/dsImaging/.validation/admission2`:

```sh
set -e
. /Users/david/Documents/GitHub/dsimaging-fix/dsImaging/.validation/env.sh
git -C checkout status --porcelain > logs/status-before.txt
git -C checkout rev-parse HEAD > logs/commit.txt
Rscript checkout/inst/validation/v0.5.0/run-suite.R dsImaging "$PWD/checkout" logs/test-cases.csv > logs/test-suite.txt 2>&1
R CMD build checkout > logs/build.txt 2>&1
R CMD check --no-multiarch dsImaging_0.5.0.tar.gz > logs/check.txt 2>&1
git -C checkout status --porcelain > logs/status-after.txt
```

The exact suite script is copied as `run-suite.R`. Runtime versions are in
`environment.txt`: R 4.5.2, Python 3.11.14, Darwin 24.4.0 arm64, NumPy 1.26.4,
pydicom 3.0.1, SimpleITK 2.5.3, nibabel 5.4.2 and rt-utils 1.2.7.
The unchanged local `.validation/env.sh` supplies `R_LIBS_USER` from
`.validation/library`, first-on-PATH Python from `.validation/python`, and
isolated `TMPDIR`, `DSHPC_HOME`, `DSIMAGING_HOME` and `DSIMAGING_ASSET_DB` under
`.validation`; `DSHPC_DISABLE_AUTOSTART=1` and `PYTHONDONTWRITEBYTECODE=1` remain
set. No global dependency installation was changed.

## Scope

Synthetic pydicom SEG fixtures reference the mapped CT series, contain several
segments, shuffled frames, overlapping regions and sparse slices. Tests cover
label/number selection, union/single-segment assets, explicit empty masks,
shared/per-frame metadata, wrong patients/studies/frames/series/SOP instances,
duplicate or missing references, unsupported encodings, geometry, exact file
sets, hashes and roster mutations. All frames, including unselected segments,
are checked. Geometry tolerances are absolute, with a large-origin regression.

Labelled dose fixtures cover several regions, absent and all-absent labels,
undeclared private values, mapped mask sets, exact numerical measurements,
source bytes and geometry, aliases of the same source, integer label encoding,
NIfTI scaling, conflicting qform/sform, and NaN/Inf source voxels that SimpleITK
would otherwise decode as zero. R tests validate public-schema provenance,
complete sample/ROI cross-products, patient thresholds, declared metadata joins,
literal public label `NA`, private ASSIGN and aggregate-extraction refusals.
Actual admitted DataSHIELD requests execute SEG and labelled dose runners,
publish their mapped outputs, and assign the labelled tables. DSLite exercises
the registered ASSIGN/AGGREGATE dispatch; final dsHPC submission is mocked.

## Limits and boundary

Binary SEG is admitted on the exact regular single-frame CT/MR source grid.
Fractional/LABELMAP SEG, resampled grids, multiframe reference images, ambiguous
or incomplete associations remain refused. A separate selected-segment request
publishes each per-segment asset, retaining one mask per sample per request.
Dose labels are declared public schema, never discovered from private masks;
missing regions retain four NA measurements and zero voxels. All mask values
and dose rows remain on the node except authorized server-side table ASSIGN,
with the same patient admission and downstream controls as radiomics.

No live MinIO/remote HPC deployment, real patient data, clinical accuracy study,
real MONAI model weights, historical demonstration or thesis edit was performed.
No tag or push was made. Initial admission limits unrelated to these routes remain.
