# dsImaging 0.5.0 admission validation receipt

Task: `DSIMAGING_RAISE_2026-09-23`. Recorded 2026-09-23.

The tested source commit is `fe8a6eeea381fa17bdda3bb51cf6eaec10ef2f49` on `feat/admit-rt-wsi-monai`,
branched from `v0.4.0`. Tests, source-package build and package check ran from
`.validation/checkout`, a detached checkout of that exact commit. Its
`git status --porcelain` was empty before and after validation. The receipt is
added in a subsequent commit containing only validation records; it does not
claim that a commit contains its own hash.

## Results

The complete local suite contains 265 test cases and passed
1308 expectations, with 0 failed expectations,
0 errors, 0 warnings and 1 skipped test(s).
`R CMD build` succeeded. `R CMD check --no-multiarch` reported **Status: OK**:
0 errors, 0 warnings and 0 notes. Its installed-package test run independently
reported 1308 passes and 1 skip(s).
Repeated test runs are not added together when reporting totals.
Captured text records normalize line endings and trailing whitespace only.

- `results.csv`: combined server/client validation summary with exact source commits.
- `test-cases.csv`: this package's per-case testthat results and timing.
- `test-suite.txt`, `build.txt`, `check.txt`, `check-tests.txt`: captured evidence.
- `environment.txt`: R/Python, platform and relevant dependency versions.
- `run-suite.R`: exact script used for the local testthat results.

The server's only skip is the existing optional live MinIO integration test:
`DSIMAGING_RUN_MINIO_TESTS=1` was not set. All new synthetic DICOM/RT/WSI,
MONAI-provider, QC and profile tests ran. Versioned S3 series were tested with
the real download/integrity helpers and a fake paginated S3 client, including
extra/missing objects and changed bytes. This is not a live MinIO test.
The client has no skips.

## Commands and environment

The commands below were executed in `/Users/david/Documents/GitHub/dsimaging-fix/dsImaging`
with shell failure propagation enabled. Server and client validation ran in
parallel, each command remaining in the foreground of its shell.

```sh
set -e
. .validation/env.sh
Rscript .validation/run-suite.R dsImaging "$PWD/.validation/checkout" .validation/logs/server-suite.csv > .validation/logs/server-suite.log 2>&1
R CMD build .validation/checkout > .validation/logs/server-build.log 2>&1
R CMD check --no-multiarch dsImaging_0.5.0.tar.gz > .validation/logs/server-check.log 2>&1
```

The local environment used R 4.5.2 (2025-10-31) on
`aarch64-apple-darwin20`, macOS 15.4.1 / Darwin 24.4.0 arm64, and Python
3.11.14 (Clang 17.0.0). `R_LIBS_USER` pointed to the server clone's
`.validation/library`. A local `.validation/python` virtual environment was
first on `PATH`, with NumPy 1.26.4 for PyRadiomics 3.0.1 compatibility.
`TMPDIR`, `DSHPC_HOME`, `DSIMAGING_HOME` and `DSIMAGING_ASSET_DB` were confined
to the server clone's `.validation` directory; `DSHPC_DISABLE_AUTOSTART=1`
and `PYTHONDONTWRITEBYTECODE=1` were set. No global dependency installation
was changed. R dependencies included dsHPC 0.3.0, dsHPCClient 0.4.0,
testthat 3.3.2, DSI 1.8.0, DSLite 1.4.1 and arrow 22.0.0.
Relevant Python versions are recorded in `environment.txt`.

## Scope and limitations

Synthetic fixtures are generated during the tests: pydicom CT series,
RTSTRUCT/RTDOSE/RTPLAN, Pillow slides and an isolated fake MONAI bundle.
Tests cover actual DataSHIELD request validation and sealed worker contexts,
runner execution, output validation, private assignment and aggregate-method
refusal. They also test hash/file-set/UID/geometry mismatches, wrong patients,
missing/extra outputs, symlinks and hidden artifacts. Real PyRadiomics
extraction verifies the exact four-feature v2 selection.

The final dsHPC submission is mocked in the synthetic workflow integration
fixture. DSLite assignment/aggregate dispatch is exercised directly. No remote
HPC deployment, real MONAI weights, clinical accuracy study or historical
demonstration was run. Client vignettes render their bundled static evidence;
workflow-execution chunks remain disabled.

Admission remains restricted to exact supported contracts: regular CT/MR
series with an authoritative instance count, RTSTRUCT union masks, physical
GY RTDOSE with a matched RTPLAN, self-contained slides and administrator
MONAI bundles implementing the documented interface. Dose ROI groups are
`whole_grid` and optional positive-mask union. DICOM SEG, sidecar slides,
Analyze pairs, incomplete series and unsupported geometry remain refused.
Tile fan-out has no new public count channel. The live MinIO test and real
clinical/model validation remain outside this receipt.

The historical `aerts_signature_v1.yaml` is unchanged from `v0.4.0`
(SHA-256 `1cc5d18d45b885e96641e8dd202d54a80aed7416c7f97d06b44d7d4f051ed623`).
No historical demonstration artifacts were changed, no tag was created and
no push was performed.
