# dsImaging 0.6.0 model bundle validation

Task: `DSIMAGING_MODELS_2026-09-23`. Recorded 2026-09-23.

Tested source: `39abd6d759aa68da75deecfb3f74750e19a5af8c` on `feat/model-bundles`,
descended from v0.5.0 (`538d6ef`). The complete suite, source-package build and
package check ran in the clean detached checkout `.validation/models/checkout`.
`status-before.txt` and `status-after.txt` are empty. These receipts are added
in a subsequent commit; a receipt does not contain its own commit hash.

## Results

The complete suite reported **282 test cases and 1540 passing expectations**, with
**0 failures, 0 errors, 0 warnings and 1 skip**. The sole skip is the existing
opt-in MinIO integration test (`DSIMAGING_RUN_MINIO_TESTS` was unset). No model
bundle test was skipped. `R CMD build` succeeded. `R CMD check --no-multiarch`
returned **Status: OK**, with **0 errors, 0 warnings and 0 notes**. Its independent
installed-package suite reported 1540 passes, 0 failures, 0 warnings and
1 skip. Repeated runs are not added together.

## Evidence

- `results.csv` records source identity and totals.
- `test-cases.csv` records each local test case and its outcomes.
- `test-suite.txt`, `build.txt`, `check.txt`, `check-tests.txt` contain logs.
- `commit.txt`, `status-before.txt`, `status-after.txt` prove checkout identity.
- `run-suite.R` is the exact suite runner, copied unchanged from v0.5.0.
- `environment.txt` records R, platform and dependency versions.
- `install.txt` records package installation: configure could not write the
  system `/var/lib/dsimaging` environment directories on this unprivileged
  host. No production segmentation environment or model weights were installed.
- `totalseg-source-audit.txt` records an independent review of pinned upstream
  2.4.0 sources: 23 model layouts and 27 mode branches across 12 task profiles,
  including crop/auxiliary dependencies, with exact source hashes.
- `torch-cache-probe.txt` contains the standalone command and results for an
  actual Torch 2.10.0 synthetic checkpoint: registered weights load, external
  `.bin` checkpoints fail before provider wrappers and through serialization
  aliases. This supplemental interpreter differs from the main suite Python.

Text log copies normalize line endings and trailing whitespace only. Earlier
release receipts remain unchanged. Development runs before the final fixes
are retained only in the ignored `.validation` directory and are not counted.

## Commands

From `dsImaging/.validation/models`, after creating the detached checkout:

```sh
set -e
. /Users/david/Documents/GitHub/dsimaging-fix/dsImaging/.validation/env.sh
git -C checkout status --porcelain > logs/status-before.txt
git -C checkout rev-parse HEAD > logs/commit.txt
Rscript checkout/inst/validation/v0.5.0/run-suite.R dsImaging "$PWD/checkout" logs/test-cases.csv > logs/test-suite.txt 2>&1
R CMD build checkout > logs/build.txt 2>&1
R CMD check --no-multiarch dsImaging_0.6.0.tar.gz > logs/check.txt 2>&1
git -C checkout status --porcelain > logs/status-after.txt
```

The shared environment points `R_LIBS_USER`, `TMPDIR`, `DSHPC_HOME`,
`DSIMAGING_HOME`, `DSIMAGING_ASSET_DB` and Python's `PATH` into this clone's
`.validation` area. It sets `DSHPC_DISABLE_AUTOSTART=1` and
`PYTHONDONTWRITEBYTECODE=1`.

## Scope

Synthetic bundles exercise manifest/registry identity, every file's size and
SHA-256, absent/corrupt material, traversal/symlink/extra-file refusal, fused
LungMask fill weights, TotalSegmentator crop dependencies, nnU-Net declared
folds and checkpoints, MONAI explicit checkpoint bindings, installer idempotence
and forced-update rollback, empty supporting files, and safe pinned ZIPs.

Actual runner subprocesses use fake providers and synthetic image/mask data.
They verify explicit local paths, provider-version refusals, offline flags,
direct network and external-command refusal, inherited policy in an actual
multiprocessing spawn, import-time warm-cache refusal, and private output
contracts. R tests cover administrator authentication, public capabilities,
legacy-marker refusal and path-free analyst selectors. Existing imaging,
DataSHIELD/disclosure and worker-admission suites also pass.

No real provider weights were downloaded. These tests do not establish GPU
execution, upstream checkpoint compatibility or clinical accuracy. MONAI
configuration and serialized models remain administrator-trusted material;
Python offline guards are not a sandbox against malicious native code.
Read-only deployment and controlled administrator updates remain required.
