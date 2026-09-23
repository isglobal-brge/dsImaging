# Administrator-registered model bundles

Release: 0.6.0. Task: `DSIMAGING_MODELS_2026-09-23`.

## Contract

Every learned segmentation provider consumes a complete model bundle approved
by the node administrator. Analysis never installs or downloads a model. A
provider import, a successful `--help` command, a cache directory or an
`.installed` marker is not evidence of an installed model.

The default layout is:

```text
/var/lib/dsimaging/models/
  sources/<provider>/<task>.json    # administrator's pinned download recipe
  registry/<provider>/<task>.json   # registered manifest SHA-256
  <provider>/<task>/
    manifest.json
    ... all required weights, configuration and supporting files ...
```

`DSIMAGING_MODELS` selects another model root. Source recipes and registry
locations may also be configured by the administrator. All participating
workers must see the same registered bytes and provider versions. The model
tree, recipes and registry are administrator-owned; analysis identities have
read access only. An analyst chooses a provider/task identifier,
never a model path, registry location, source URL or checksum.

## Manifest and registry

`manifest.json` has schema version 1 with these required fields:

| Field | Meaning |
| --- | --- |
| `schema_version` | Integer `1`. |
| `provider` | `lungmask`, `totalsegmentator`, `nnunetv2` or `monai`. |
| `task` | Administrator-approved model/task identifier. |
| `upstream_version` | Exact installed provider distribution version. |
| `licence` | Licence identifier or administrator-reviewed licence text/reference. |
| `source_url` | Provenance URL for the approved upstream release/model. |
| `download_date` | Installer-recorded UTC download timestamp. |
| `files` | Complete list of relative `path`, integer byte `size` and lowercase SHA-256 `sha256`. |
| `runtime` | Provider-specific local entry points and inference requirements. |

All weight files, model plans, inference configuration, metadata and other
supporting files belong in `files`. Empty supporting files such as Python
package markers are allowed; provider-required weights and configuration must
be nonempty. File paths must remain inside the bundle. Absolute paths,
traversal, duplicate entries and symlinks are refused. Extra
files cannot silently augment the bundle. The manifest itself is excluded
from its file list: its exact bytes are SHA-256-pinned separately in the
administrator registry. Rewriting either model bytes or manifest metadata
without an approved new registration invalidates the bundle.

The source recipe uses the same schema and runtime fields, adds a pinned
`url` to each directly downloaded file entry, and omits `download_date`, which
the installer supplies. File URLs use HTTPS or an administrator-controlled
`file://` mirror.
The installer does not discover a moving `latest` release or trust hashes
returned by an analysis-time download. The administrator reviews the release,
licence, model dependencies, sizes and hashes before installing. Digests ensure
the selected bytes remain unchanged; they are not an assertion of model
quality or independent proof of upstream authorship.

Upstream ZIP releases can instead use an `archives` list. Each entry pins
`url`, `size`, `sha256` and an optional bundle-relative `destination` prefix,
such as `"weights"` for TotalSegmentator. The complete extracted file inventory
still belongs in `files` with individual size/SHA-256 values; entries supplied
by an archive omit their direct `url`. The installer verifies the archive
before extraction and independently checks every extracted file. Traversal,
symlinks, duplicate destinations and undeclared extra files are refused.
Archives are transport inputs; their compressed bytes need not remain in the
inference bundle.

For example, a LungMask source recipe has this shape. Replace the placeholders
with the reviewed release metadata and actual file size/hash before use:

```json
{
  "schema_version": 1,
  "provider": "lungmask",
  "task": "R231",
  "upstream_version": "<exact installed lungmask version>",
  "licence": "<reviewed model licence>",
  "source_url": "https://<approved release page>",
  "files": [
    {
      "path": "weights/model.pth",
      "size": 12345,
      "sha256": "<64 lowercase hexadecimal characters>",
      "url": "https://<approved mirror>/model.pth"
    }
  ],
  "runtime": {"model_path": "weights/model.pth"}
}
```

## Complete provisioning

The existing administrator interface keeps its public function names:

```r
# On the node, after writing the reviewed source recipe:
dsImaging::install_model("lungmask", "R231")

# Or through the authenticated DataSHIELD administrator endpoint:
dsImagingClient::ds.imaging.install_model(
  conns, admin_key, provider = "lungmask", task = "R231")
```

The remote method remains `imagingInstallModelDS()` and requires
`dshpc.admin_key` or `DSHPC_ADMIN_KEY`. The caller supplies only provider/task;
the source recipe comes from protected server configuration. Local R helpers
and the Python CLI are administrator filesystem operations.

The installer downloads every recipe file into a staging directory, verifies
its exact size and SHA-256, checks the provider's required files, and writes
the manifest in staging. It publishes the bundle only after the complete set
passes. Registration records the manifest digest after verification. Failure
never reports `installed`.
Reinstalling an already registered valid bundle verifies it and returns its
digest; an administrator uses `force = TRUE` for a deliberate replacement.

The equivalent Python entry point is the package's
`inst/python/dsimaging_model_bundles.py`:

```sh
python dsimaging_model_bundles.py install \
  --provider lungmask --task R231 --root /var/lib/dsimaging/models
python dsimaging_model_bundles.py verify \
  --provider lungmask --task R231 --root /var/lib/dsimaging/models
python dsimaging_model_bundles.py list --root /var/lib/dsimaging/models
```

An administrator can register an already provisioned bundle with a complete
manifest, using `register_segmentation_model()` or the CLI `register` command.
Its path must be the canonical `<root>/<provider>/<task>` directory. Arbitrary
external paths and legacy path-only registry records do not authorize use.
Registration verifies the file set and provider requirements before pinning
the manifest.

## Provider requirements

| Provider | Explicit local entry points and required files |
| --- | --- |
| LungMask | `runtime.model_path`; fused `LTRCLobes_R231` additionally requires `runtime.fillmodel_path`. The runner gives both weight paths to `LMInferer`, so neither primary nor fill weights use a Torch download cache. |
| TotalSegmentator | `runtime.weights_path` and `runtime.models` enumerate every audited task, crop and auxiliary model. Each model entry declares `id`, `folder`, `trainer` and `configuration`. `TOTALSEG_WEIGHTS_PATH` points inside the verified bundle. Unknown version/task combinations fail closed. |
| nnU-Net v2 | `runtime.model_folder`, `runtime.folds`, and `runtime.checkpoint`; the model's dataset/plans metadata and every declared fold checkpoint must be present. The predictor initializes directly from the verified folder. |
| MONAI | `runtime.inference_config`, `runtime.metadata`, and `runtime.weight_files`; configuration and all referenced weights are bundle files. The administrator's adapter binds `image`, `output_dir` and a verified checkpoint and produces exactly one geometry-matched mask per sample. |

TotalSegmentator support is deliberately pinned to **2.4.0**. These complete
model sets cover the exposed normal/fast and applicable ROI/crop paths:

| Task | Required upstream model IDs |
| --- | --- |
| `total` | 291, 292, 293, 294, 295, 297, 298 |
| `total_mr` | 730, 731, 732, 733 |
| `body` | 299, 300 |
| `lung_vessels` | 258, 298 |
| `cerebral_bleed` | 150, 298 |
| `hip_implant` | 260, 298 |
| `coronary_arteries` | 503, 298 |
| `pleural_pericard_effusion` | 315, 298 |
| `head_glands_cavities` | 775, 298 |
| `headneck_bones_vessels` | 776, 298 |
| `head_muscles` | 777, 298 |
| `headneck_muscles` | 778, 779, 298 |

The `TS_MODELS` and `TS_TASK_MODELS` constants in the verifier specify the exact
approved folder/trainer/configuration combinations. Each model includes
`dataset.json`, `plans.json` and `fold_0/checkpoint_final.pth` beneath
`<weights_path>/<folder>/<trainer>__nnUNetPlans__<configuration>/`. The dependency
audit uses upstream 2.4.0's
[task selection](https://github.com/wasserth/TotalSegmentator/blob/v2.4.0/totalsegmentator/python_api.py),
[model registry](https://github.com/wasserth/TotalSegmentator/blob/v2.4.0/totalsegmentator/libs.py),
and [predictor](https://github.com/wasserth/TotalSegmentator/blob/v2.4.0/totalsegmentator/nnunet.py).
Other versions/tasks require a reviewed code change to the closure; an
administrator recipe cannot silently extend it.

A single-weight MONAI configuration declares `ckpt_path`; the runner replaces
that field with the verified absolute path of `runtime.weight_files[0]`.
For multiple weights, `runtime.weight_bindings` maps existing configuration
keys to bundle-relative files and must cover every declared weight. The runner
supplies those verified absolute paths and `bundle_root`, together with its
input/output bindings. Standard checkpoint file reads and Torch checkpoint
loaders refuse files outside the verified manifest, preventing accidental
fallback to a home cache or another local model. A site adapter must use these
bindings instead of hard-coded external paths.

For nnU-Net, the analyst's default `fold = "all"` selects the complete
registered fold set, including a registered `"all"` trained fold when present.
A numeric fold must appear in `runtime.folds`; it cannot discover an
unregistered model directory.

Provider package versions must match `upstream_version` before inference.
Provider-specific checks reject incomplete known requirements; generic file
hashing alone cannot prove that an arbitrary executable MONAI configuration
has no further dependency. MONAI configuration and serialized checkpoints are
administrator-trusted code/material and must be reviewed as part of approval.
An attempted runtime download is blocked even if a reviewed configuration
was incomplete.

CT lung threshold segmentation, existing-mask selection and the deterministic
image embedding baseline have no learned weights and need no bundle. Future
model-backed providers must join this same registration and verification path.

## Inference and offline boundary

1. Resolve the selected provider/task through the administrator registry.
2. Verify the manifest digest, identity, complete file set, sizes, SHA-256 and
   provider requirements. Missing registration or any mismatch stops the job.
3. Materialize the sealed image inputs through the existing authorized storage
   path, then enable the model runtime's offline policy before provider imports
   and inference. Storage access is not a model download permission.
4. Pass only verified local model/configuration paths to the provider. Check
   its installed version and the requested mode/fold against the bundle.
5. Keep the offline policy active for provider execution and child processes.
   There is no download retry or fallback to a global/home cache.

The offline environment includes `DSIMAGING_MODEL_OFFLINE=1`,
`HF_HUB_OFFLINE=1`, `HF_DATASETS_OFFLINE=1`, and `TRANSFORMERS_OFFLINE=1`.
`nnUNet_compile=false` prevents runtime compiler subprocesses. Telemetry is
disabled through `HF_HUB_DISABLE_TELEMETRY=1`, `DO_NOT_TRACK=1`, and the
TotalSegmentator usage-statistics hook.
Python network/DNS guards and an external-command audit guard supplement those
flags because provider support for offline flags is not sufficient by itself.
Spawned Python workers inherit the policy through `sitecustomize`.

This prevents normal provider download paths from acquiring model bytes during
inference. It is not an operating-system sandbox for malicious native code or
administrator-installed configuration. Deployments retain their own network
isolation and must prevent concurrent administrator mutation of bundles in
use. An update replaces a complete reviewed bundle during a controlled
deployment and updates the dsHPC runtime revision. Read-only mounts preserve
the verified bytes while jobs run.

## Capabilities, migration and validation

`imagingCapabilitiesDS()$models` and `imagingListModelsDS()` return
`provider`, `task`, `ready`, and `manifest_sha256`. The client exposes these
through `ds.imaging.capabilities()` and `ds.imaging.models()`. Readiness is
based on current integrity verification; a registry record alone is
insufficient. Paths, manifests, source URLs, licence text and local diagnostics
are not returned on the analyst surface. Successful administrator installation
also reports the registered manifest digest.
Private runner manifests and segmentation summaries record it as
`model_bundle_manifest_sha256`, so the result provenance identifies the exact
verified model bundle.

Old `.installed` markers and path-only model registrations are not migrated
implicitly. Administrators inventory the existing models, review a complete
source recipe or manifest, install/register it under the canonical root, and
confirm readiness before re-enabling learned segmentation. Partial Torch or
TotalSegmentator caches are not bundles. Update the provider environment and
`dshpc.runtime_revision` consistently across local, container and remote/HPC
workers when model material changes.

Tests use synthetic bundles and fake providers; they verify complete install
and registration, missing/tampered manifest or files, required auxiliary
weights, explicit provider paths, offline environment and blocked network
attempts, and capability projections. Real model weights are not downloaded
by the test suite. Complete R suite and `R CMD check` receipts for the release
are recorded under `inst/validation/v0.6.0/`; these checks do not constitute
clinical validation or a new execution of historical imaging demonstrations.
