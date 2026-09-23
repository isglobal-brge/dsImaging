#!/usr/bin/env python3
"""Administrator-pinned, complete model bundles; no provider imports for management.

Source recipes are administrator-owned JSON, never analysis configuration.  Each
file has an expected byte count, SHA-256 and explicit HTTPS or file URL.  Inference
accepts only provider/task identifiers and verifies the independently pinned
manifest and every file before importing a provider.
"""
import argparse
import datetime
import functools
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import socket
import stat
import sys
import tempfile
import urllib.parse
import urllib.request
import zipfile

PROVIDERS = ("lungmask", "totalsegmentator", "nnunetv2", "monai")
DISTRIBUTIONS = {name: name for name in PROVIDERS}
DISTRIBUTIONS["totalsegmentator"] = "TotalSegmentator"

# Audited against upstream v2.4.0 python_api.py, libs.py and nnunet.py:
# https://github.com/wasserth/TotalSegmentator/tree/v2.4.0/totalsegmentator
# This is the union of normal/fast and applicable ROI/crop dependencies. New
# versions/tasks require a source review; a recipe cannot redefine this closure.
TS_MODELS = {
    291: ("Dataset291_TotalSegmentator_part1_organs_1559subj", "nnUNetTrainerNoMirroring", "3d_fullres"),
    292: ("Dataset292_TotalSegmentator_part2_vertebrae_1532subj", "nnUNetTrainerNoMirroring", "3d_fullres"),
    293: ("Dataset293_TotalSegmentator_part3_cardiac_1559subj", "nnUNetTrainerNoMirroring", "3d_fullres"),
    294: ("Dataset294_TotalSegmentator_part4_muscles_1559subj", "nnUNetTrainerNoMirroring", "3d_fullres"),
    295: ("Dataset295_TotalSegmentator_part5_ribs_1559subj", "nnUNetTrainerNoMirroring", "3d_fullres"),
    297: ("Dataset297_TotalSegmentator_total_3mm_1559subj", "nnUNetTrainer_4000epochs_NoMirroring", "3d_fullres"),
    298: ("Dataset298_TotalSegmentator_total_6mm_1559subj", "nnUNetTrainer_4000epochs_NoMirroring", "3d_fullres"),
    299: ("Dataset299_body_1559subj", "nnUNetTrainer", "3d_fullres"),
    300: ("Dataset300_body_6mm_1559subj", "nnUNetTrainer", "3d_fullres"),
    730: ("Dataset730_TotalSegmentatorMRI_part1_organs_495subj", "nnUNetTrainer_DASegOrd0_NoMirroring", "3d_fullres"),
    731: ("Dataset731_TotalSegmentatorMRI_part2_muscles_495subj", "nnUNetTrainer_DASegOrd0_NoMirroring", "3d_fullres"),
    732: ("Dataset732_TotalSegmentatorMRI_total_3mm_495subj", "nnUNetTrainer_DASegOrd0_NoMirroring", "3d_fullres"),
    733: ("Dataset733_TotalSegmentatorMRI_total_6mm_495subj", "nnUNetTrainer_DASegOrd0_NoMirroring", "3d_fullres"),
    258: ("Dataset258_lung_vessels_248subj", "nnUNetTrainer", "3d_fullres"),
    150: ("Dataset150_icb_v0", "nnUNetTrainer", "3d_fullres"),
    260: ("Dataset260_hip_implant_71subj", "nnUNetTrainer", "3d_fullres"),
    503: ("Dataset503_cardiac_motion", "nnUNetTrainer", "3d_fullres"),
    315: ("Dataset315_thoraxCT", "nnUNetTrainer", "3d_fullres"),
    775: ("Dataset775_head_glands_cavities_492subj", "nnUNetTrainer_DASegOrd0_NoMirroring", "3d_fullres_high"),
    776: ("Dataset776_headneck_bones_vessels_492subj", "nnUNetTrainer_DASegOrd0_NoMirroring", "3d_fullres_high"),
    777: ("Dataset777_head_muscles_492subj", "nnUNetTrainer_DASegOrd0_NoMirroring", "3d_fullres_high"),
    778: ("Dataset778_headneck_muscles_part1_492subj", "nnUNetTrainer_DASegOrd0_NoMirroring", "3d_fullres_high"),
    779: ("Dataset779_headneck_muscles_part2_492subj", "nnUNetTrainer_DASegOrd0_NoMirroring", "3d_fullres_high"),
}
TS_TASK_MODELS = {
    "total": (291, 292, 293, 294, 295, 297, 298),
    "total_mr": (730, 731, 732, 733), "body": (299, 300),
    "lung_vessels": (258, 298), "cerebral_bleed": (150, 298),
    "hip_implant": (260, 298), "coronary_arteries": (503, 298),
    "pleural_pericard_effusion": (315, 298),
    "head_glands_cavities": (775, 298), "headneck_bones_vessels": (776, 298),
    "head_muscles": (777, 298), "headneck_muscles": (778, 779, 298),
}


class ModelBundleError(RuntimeError):
    """Bundle is absent, incomplete, unregistered or does not match its pin."""


def _identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value) or ".." in value:
        raise ModelBundleError("Invalid model provider/task identifier")
    return value


def _roots(root=None, registry=None):
    root = Path(root or os.environ.get("DSIMAGING_MODELS", "/var/lib/dsimaging/models")).absolute()
    registry = Path(registry or os.environ.get("DSIMAGING_MODEL_REGISTRY", str(root / "registry"))).absolute()
    return root, registry


def _location(provider, task, root=None, registry=None):
    if provider not in PROVIDERS:
        raise ModelBundleError("Unsupported model bundle provider")
    _identifier(task)
    root, registry = _roots(root, registry)
    return _path(root, provider + "/" + task), _path(registry, provider + "/" + task + ".json")


def _relative(value):
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise ModelBundleError("Invalid bundle-relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(p in (".", "..") for p in value.split("/")) or "" in value.split("/"):
        raise ModelBundleError("Bundle path must stay inside its registered directory")
    return value


def _path(root, relative):
    relative = _relative(relative)
    root = Path(root)
    path = root
    if root.is_symlink():
        raise ModelBundleError("Model bundle paths must not be symlinks")
    for part in PurePosixPath(relative).parts:
        path = path / part
        if path.is_symlink():
            raise ModelBundleError("Model bundle paths must not be symlinks")
    return path


def _read_json(path):
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError) as exc:
        raise ModelBundleError("Model bundle JSON is missing or invalid: " + Path(path).name) from exc


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _atomic_write(path, contents):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, tmp = tempfile.mkstemp(prefix=".bundle-", dir=path.parent)
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(contents)
            stream.flush()
            os.fchmod(stream.fileno(), 0o644)
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _atomic_json(path, value):
    _atomic_write(path, (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8"))


def _manifest(manifest, provider, task):
    if not isinstance(manifest, dict) or manifest.get("schema_version") != 1:
        raise ModelBundleError("Unsupported model manifest schema")
    if manifest.get("provider") != provider or manifest.get("task") != task:
        raise ModelBundleError("Model manifest provider/task mismatch")
    for field in ("upstream_version", "licence", "source_url", "download_date"):
        if not isinstance(manifest.get(field), str) or not manifest[field].strip():
            raise ModelBundleError("Model manifest lacks " + field)
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise ModelBundleError("Model manifest must list every model file")
    listed = {}
    for entry in files:
        if not isinstance(entry, dict):
            raise ModelBundleError("Invalid model manifest file entry")
        name = _relative(entry.get("path"))
        if name == "manifest.json" or name in listed:
            raise ModelBundleError("Duplicate or reserved model manifest file path")
        if type(entry.get("size")) is not int or entry["size"] <= 0:
            raise ModelBundleError("Invalid model file size")
        if not isinstance(entry.get("sha256"), str) or not re.fullmatch(r"[a-f0-9]{64}", entry["sha256"]):
            raise ModelBundleError("Invalid model file SHA-256")
        listed[name] = entry
    if not isinstance(manifest.get("runtime"), dict):
        raise ModelBundleError("Model manifest lacks provider runtime configuration")
    _closure(manifest, listed)
    return listed


def _closure(manifest, files):
    """Check provider-required files independently of the recipe's file inventory."""
    provider, task, runtime = manifest["provider"], manifest["task"], manifest["runtime"]
    required = []
    if provider == "lungmask":
        if task not in ("R231", "LTRCLobes", "LTRCLobes_R231", "R231CovidWeb"):
            raise ModelBundleError("Unsupported LungMask bundle task")
        required.append(_relative(runtime.get("model_path")))
        if task == "LTRCLobes_R231":
            required.append(_relative(runtime.get("fillmodel_path")))
        elif runtime.get("fillmodel_path") is not None:
            raise ModelBundleError("Unexpected LungMask auxiliary model")
    elif provider == "nnunetv2":
        folder = _relative(runtime.get("model_folder"))
        checkpoint = _relative(runtime.get("checkpoint"))
        if "/" in checkpoint:
            raise ModelBundleError("nnU-Net checkpoint must be a filename")
        folds = runtime.get("folds")
        if not isinstance(folds, list) or not folds or any(type(f) is not int and f != "all" for f in folds):
            raise ModelBundleError("nnU-Net bundle must declare explicit folds")
        if len(set(folds)) != len(folds) or any(type(f) is int and f < 0 for f in folds):
            raise ModelBundleError("Invalid nnU-Net folds")
        required.extend(folder + "/" + f for f in ("dataset.json", "plans.json"))
        required.extend(folder + "/fold_" + str(fold) + "/" + checkpoint for fold in folds)
    elif provider == "monai":
        required.extend(_relative(runtime.get(key)) for key in ("inference_config", "metadata"))
        weights = runtime.get("weight_files")
        if not isinstance(weights, list) or not weights:
            raise ModelBundleError("MONAI bundle must declare every weight file")
        required.extend(_relative(name) for name in weights)
        monai_weight_bindings(runtime)
    elif provider == "totalsegmentator":
        if manifest["upstream_version"] != "2.4.0" or task not in TS_TASK_MODELS:
            raise ModelBundleError("Unaudited TotalSegmentator version/task dependency closure")
        weights = _relative(runtime.get("weights_path"))
        models = runtime.get("models")
        if not isinstance(models, list) or not all(isinstance(m, dict) and type(m.get("id")) is int for m in models):
            raise ModelBundleError("TotalSegmentator bundle must list task and auxiliary models")
        ids = [m["id"] for m in models]
        if len(ids) != len(set(ids)) or set(ids) != set(TS_TASK_MODELS[task]):
            raise ModelBundleError("TotalSegmentator task/auxiliary/crop model closure is incomplete")
        for model in models:
            folder, trainer, configuration = TS_MODELS[model["id"]]
            if (model.get("folder"), model.get("trainer"), model.get("configuration")) != (folder, trainer, configuration):
                raise ModelBundleError("TotalSegmentator model layout differs from audited upstream")
            model_root = weights + "/" + folder + "/" + trainer + "__nnUNetPlans__" + configuration
            required.extend(model_root + "/" + name for name in ("dataset.json", "plans.json", "fold_0/checkpoint_final.pth"))
    if any(name not in files for name in required):
        raise ModelBundleError("Model bundle is missing a required weight, configuration or auxiliary file")


def monai_weight_bindings(runtime):
    weights = runtime["weight_files"]
    bindings = runtime.get("weight_bindings")
    if bindings is None and len(weights) == 1:
        bindings = {"ckpt_path": weights[0]}
    if (not isinstance(bindings, dict) or not bindings or
            any(not isinstance(key, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", key)
                or key in ("image", "output_dir", "run", "bundle_root", "config_file", "meta_file", "run_id")
                for key in bindings) or set(bindings.values()) != set(weights)):
        raise ModelBundleError("MONAI requires explicit configuration bindings for every weight file")
    return bindings


def _verify_directory(path, provider, task):
    manifest_path = _path(path, "manifest.json")
    manifest = _read_json(manifest_path)
    files = _manifest(manifest, provider, task)
    for name, expected in files.items():
        file_path = _path(path, name)
        if not file_path.is_file() or file_path.stat().st_size != expected["size"]:
            raise ModelBundleError("Model bundle file is missing or has a size mismatch: " + name)
        if _sha256(file_path) != expected["sha256"]:
            raise ModelBundleError("Model bundle file SHA-256 mismatch: " + name)
    actual = set()
    for base, directories, filenames in os.walk(path, followlinks=False):
        for name in directories + filenames:
            if (Path(base) / name).is_symlink():
                raise ModelBundleError("Model bundle contains a symlink")
        actual.update((Path(base) / name).relative_to(path).as_posix() for name in filenames)
    if actual != set(files) | {"manifest.json"}:
        raise ModelBundleError("Model bundle contains files not covered by its manifest")
    if provider == "monai":
        config = _read_json(_path(path, manifest["runtime"]["inference_config"]))
        if not isinstance(config, dict) or not ({"image", "output_dir", "run"} | set(monai_weight_bindings(manifest["runtime"]))).issubset(config):
            raise ModelBundleError("MONAI bundle lacks the dsImaging input/output binding contract")
    return manifest, _sha256(manifest_path)


def verify_bundle(provider, task, root=None, registry=None, check_version=False):
    path, registry_path = _location(provider, task, root, registry)
    entry = _read_json(registry_path)
    if not isinstance(entry, dict) or entry.get("schema_version") != 1 or entry.get("provider") != provider or entry.get("task") != task:
        raise ModelBundleError("Model bundle registry identity mismatch")
    manifest_path = _path(path, "manifest.json")
    if not manifest_path.is_file():
        raise ModelBundleError("Model manifest is missing")
    digest = _sha256(manifest_path)
    if entry.get("manifest_sha256") != digest:
        raise ModelBundleError("Model manifest SHA-256 does not match the administrator registry")
    manifest, verified_digest = _verify_directory(path, provider, task)
    if verified_digest != digest:
        raise ModelBundleError("Model manifest changed during verification")
    if check_version:
        try:
            version = importlib.metadata.version(DISTRIBUTIONS[provider])
        except importlib.metadata.PackageNotFoundError as exc:
            raise ModelBundleError("Model provider runtime is not installed") from exc
        if version != manifest["upstream_version"]:
            raise ModelBundleError("Model provider version does not match the registered bundle")
    return {"name": entry.get("name", task), "provider": provider, "task": task,
            "path": str(path), "manifest_sha256": digest, "manifest": manifest,
            "installed_at": entry.get("registered_at"), "ready": True,
            "registry_path": str(registry_path)}


def register_bundle(provider, task, path=None, name=None, root=None, registry=None):
    canonical, registry_path = _location(provider, task, root, registry)
    if path is not None and Path(path).absolute() != canonical:
        raise ModelBundleError("Register the bundle at its canonical provider/task directory")
    manifest, digest = _verify_directory(canonical, provider, task)
    entry = {"schema_version": 1, "name": _identifier(name or task), "provider": provider,
             "task": task, "manifest_sha256": digest, "registered_at": _now()}
    _atomic_json(registry_path, entry)
    return verify_bundle(provider, task, root, registry)


def list_bundles(root=None, registry=None):
    root, registry = _roots(root, registry)
    result = []
    for provider in PROVIDERS:
        directory = registry / provider
        if not directory.is_dir() or directory.is_symlink():
            continue
        for path in sorted(directory.glob("*.json")):
            task = path.stem
            try:
                item = verify_bundle(provider, task, root, registry)
                item.pop("manifest")
            except (ModelBundleError, OSError) as exc:
                item = {"name": task, "provider": provider, "task": task,
                        "manifest_sha256": None, "ready": False, "error": str(exc)}
            result.append(item)
    return result


def _source(entry):
    if not isinstance(entry, dict):
        raise ModelBundleError("Invalid model download source")
    parsed = urllib.parse.urlparse(entry.get("url", ""))
    if parsed.scheme not in ("https", "file") or (parsed.scheme == "https" and not parsed.netloc):
        raise ModelBundleError("Administrator source files require explicit HTTPS or file URLs")
    if parsed.scheme == "file" and parsed.netloc not in ("", "localhost"):
        raise ModelBundleError("Local source URLs cannot name a remote host")
    if type(entry.get("size")) is not int or entry["size"] <= 0 or not re.fullmatch(r"[a-f0-9]{64}", str(entry.get("sha256", ""))):
        raise ModelBundleError("Model download sources require pinned sizes and SHA-256")


def _download(entry, destination):
    _source(entry)
    with urllib.request.urlopen(entry["url"], timeout=120) as source, open(destination, "wb") as output:
        if urllib.parse.urlparse(source.geturl()).scheme not in ("https", "file"):
            raise ModelBundleError("Model source redirected to an insecure URL")
        remaining = entry["size"]
        while remaining:
            chunk = source.read(min(1024 * 1024, remaining))
            if not chunk:
                break
            output.write(chunk)
            remaining -= len(chunk)
        if remaining or source.read(1):
            raise ModelBundleError("Downloaded model file size mismatch")
    if _sha256(destination) != entry["sha256"]:
        raise ModelBundleError("Downloaded model file SHA-256 mismatch")


def _extract_archive(archive, stage, files):
    prefix = archive.get("destination", "")
    if prefix:
        prefix = _relative(prefix) + "/"
    handle, tmp = tempfile.mkstemp(prefix=".model-archive-", suffix=".zip", dir=stage.parent)
    os.close(handle)
    try:
        _download(archive, tmp)
        with zipfile.ZipFile(tmp) as zipped:
            for member in zipped.infolist():
                mode = member.external_attr >> 16
                if stat.S_ISLNK(mode) or (stat.S_IFMT(mode) and not (stat.S_ISREG(mode) or stat.S_ISDIR(mode))):
                    raise ModelBundleError("Model archive contains a symlink or special file")
                name = prefix + _relative(member.filename.rstrip("/") if member.is_dir() else member.filename)
                destination = _path(stage, name)
                if member.is_dir():
                    destination.mkdir(parents=True, exist_ok=True)
                    continue
                if name not in files or destination.exists():
                    raise ModelBundleError("Model archive has undeclared or duplicate files")
                if member.file_size != files[name]["size"]:
                    raise ModelBundleError("Model archive file size mismatch")
                destination.parent.mkdir(parents=True, exist_ok=True)
                with zipped.open(member) as source, open(destination, "wb") as output:
                    shutil.copyfileobj(source, output)
    except zipfile.BadZipFile as exc:
        raise ModelBundleError("Invalid model ZIP archive") from exc
    finally:
        os.unlink(tmp)


def install_bundle(provider, task, root=None, registry=None, sources=None, force=False):
    root, registry = _roots(root, registry)
    target, registry_path = _location(provider, task, root, registry)
    if not force and target.exists():
        return verify_bundle(provider, task, root, registry)
    sources = Path(sources or os.environ.get("DSIMAGING_MODEL_SOURCES", str(root / "sources")))
    recipe = _read_json(_path(sources, provider + "/" + task + ".json"))
    if not isinstance(recipe, dict):
        raise ModelBundleError("Model source recipe must be a JSON object")
    recipe["download_date"] = _now()
    files = _manifest(recipe, provider, task)
    archives = recipe.get("archives", [])
    if not isinstance(archives, list):
        raise ModelBundleError("Model source archives must be a list")
    for archive in archives:
        _source(archive)
        if archive.get("destination"):
            _relative(archive["destination"])
    for entry in files.values():
        if "url" in entry:
            _source(entry)
        elif not archives:
            raise ModelBundleError("Every model file requires an explicit download source")
    target.parent.mkdir(parents=True, exist_ok=True)
    lock = target.parent / ("." + task + ".install-lock")
    try:
        lock.mkdir()
    except FileExistsError as exc:
        raise ModelBundleError("Another administrator is installing this model bundle") from exc
    stage = None
    backup = None
    published = False
    registered = False
    previous_registry = None
    try:
        previous_registry = registry_path.read_bytes() if registry_path.exists() else None
        stage = Path(tempfile.mkdtemp(prefix="." + task + "-", dir=target.parent))
        for archive in archives:
            _extract_archive(archive, stage, files)
        for name, entry in files.items():
            destination = _path(stage, name)
            if "url" in entry:
                if destination.exists():
                    raise ModelBundleError("Model file has duplicate download sources")
                destination.parent.mkdir(parents=True, exist_ok=True)
                _download(entry, destination)
            if not destination.is_file() or destination.stat().st_size != entry["size"]:
                raise ModelBundleError("Downloaded model file is missing or has a size mismatch: " + name)
            if _sha256(destination) != entry["sha256"]:
                raise ModelBundleError("Downloaded model file SHA-256 mismatch: " + name)
        # Per-file source URLs are retained as provenance; the manifest pins bytes.
        _atomic_json(stage / "manifest.json", recipe)
        _verify_directory(stage, provider, task)
        # Staging is private while incomplete. Published material must be
        # readable by worker accounts, while writable only by its admin owner.
        for base, directories, filenames in os.walk(stage):
            os.chmod(base, 0o755)
            for name in filenames:
                os.chmod(Path(base) / name, 0o644)
        if target.exists():
            previous = target.parent / (stage.name + "-previous")
            os.replace(target, previous)
            backup = previous
        os.replace(stage, target)
        stage = None
        published = True
        installed = register_bundle(provider, task, root=root, registry=registry)
        registered = True
        return installed
    except Exception:
        if published:
            shutil.rmtree(target)
        if backup is not None:
            os.replace(backup, target)
            backup = None
        if published:
            if previous_registry is None:
                if registry_path.exists():
                    registry_path.unlink()
            else:
                _atomic_write(registry_path, previous_registry)
        raise
    finally:
        if stage is not None:
            shutil.rmtree(stage, ignore_errors=True)
        if registered and backup is not None:
            shutil.rmtree(backup, ignore_errors=True)
        lock.rmdir()


_OFFLINE_INSTALLED = False


def install_offline_guard():
    """Block Python network/download escape routes, including spawned workers.

    This protects trusted providers against accidental download fallbacks, not
    hostile native code. Sites must retain administrator-only bundle ownership.
    """
    global _OFFLINE_INSTALLED
    if _OFFLINE_INSTALLED:
        return
    for name in ("HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE", "HF_DATASETS_OFFLINE", "DSIMAGING_MODEL_OFFLINE"):
        os.environ[name] = "1"
    os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
    os.environ["DO_NOT_TRACK"] = "1"
    os.environ["nnUNet_compile"] = "false"
    # Python spawn does not inherit audit hooks. Its fresh interpreter imports
    # our sitecustomize before importing provider code (fork inherits the hook).
    scripts = str(Path(__file__).resolve().parent)
    paths = os.environ.get("PYTHONPATH", "").split(os.pathsep)
    os.environ["PYTHONPATH"] = os.pathsep.join([scripts] + [p for p in paths if p and p != scripts])

    verified_files = set(json.loads(os.environ.get("DSIMAGING_VERIFIED_MODEL_FILES", "[]")))
    checkpoint_suffixes = (".pth", ".pt", ".ckpt", ".onnx", ".safetensors", ".h5", ".hdf5", ".npz")

    def audit(event, args):
        if event == "open" and verified_files and isinstance(args[0], (str, bytes)):
            name = os.fsdecode(args[0])
            mode, flags = args[1], args[2]
            reading = (isinstance(mode, str) and "r" in mode) or (mode is None and not flags & os.O_WRONLY)
            if reading and name.lower().endswith(checkpoint_suffixes) and str(Path(name).resolve()) not in verified_files:
                raise ModelBundleError("Model checkpoint is outside the verified bundle manifest")
        if event in ("socket.connect", "socket.getaddrinfo", "socket.gethostbyname", "socket.gethostbyaddr", "socket.sendto"):
            # Local AF_UNIX IPC is needed by multiprocessing resource sharing.
            if event in ("socket.connect", "socket.sendto") and getattr(args[0], "family", None) == socket.AF_UNIX:
                return
            raise ModelBundleError("Model inference is offline: network access/downloads are disabled")
        if event in ("subprocess.Popen", "os.system", "os.posix_spawn", "os.exec", "os.spawn"):
            raise ModelBundleError("Model inference is offline: external commands/downloads are disabled")

    sys.addaudithook(audit)
    _OFFLINE_INSTALLED = True


def prepare_inference(provider, task):
    bundle = verify_bundle(provider, task, check_version=True)
    # Pass the immutable inventory to spawn workers as well as their offline flag.
    os.environ["DSIMAGING_VERIFIED_MODEL_FILES"] = json.dumps([
        str(_path(bundle["path"], item["path"]).resolve())
        for item in bundle["manifest"]["files"]])
    install_offline_guard()
    runtime = bundle["manifest"]["runtime"]
    if provider == "totalsegmentator":
        weights = str(_path(bundle["path"], runtime["weights_path"]))
        os.environ["TOTALSEG_WEIGHTS_PATH"] = weights
        for name in ("nnUNet_results", "nnUNet_raw", "nnUNet_preprocessed"):
            os.environ[name] = weights
    elif provider == "nnunetv2":
        for name in ("nnUNet_results", "nnUNet_raw", "nnUNet_preprocessed"):
            os.environ[name] = bundle["path"]
    return bundle


def bundle_path(bundle, relative):
    """Return only a verified local file or directory from this bundle."""
    return str(_path(bundle["path"], relative))


def guard_checkpoint_loads(bundle):
    allowed = {str(_path(bundle["path"], item["path"]).resolve())
               for item in bundle["manifest"]["files"]}

    def local_loader(loader):
        @functools.wraps(loader)
        def load(file, *args, **kwargs):
            path = file if isinstance(file, (str, bytes, os.PathLike)) else getattr(file, "name", None)
            if not isinstance(path, (str, bytes, os.PathLike)) or str(Path(os.fsdecode(path)).resolve()) not in allowed:
                raise ModelBundleError("Model checkpoint is outside the verified bundle manifest")
            return loader(file, *args, **kwargs)
        return load

    for module_name, names in {"torch": ("load",), "torch.jit": ("load",),
                               "safetensors.torch": ("load_file",),
                               "safetensors.numpy": ("load_file",)}.items():
        module = sys.modules.get(module_name)
        if module is not None:
            for name in names:
                if hasattr(module, name):
                    setattr(module, name, local_loader(getattr(module, name)))


def disable_provider_downloads(bundle):
    """Replace public lazy-download helpers after guarded provider imports."""
    guard_checkpoint_loads(bundle)

    def forbidden(*args, **kwargs):
        raise ModelBundleError("Model inference is offline: model downloads are disabled")

    for module_name, names in {
        "torch.hub": ("download_url_to_file", "load_state_dict_from_url", "load"),
        "monai.bundle": ("download", "load"),
        "monai.bundle.scripts": ("download", "load"),
        "monai.apps.utils": ("download_url", "download_and_extract"),
    }.items():
        module = sys.modules.get(module_name)
        if module is not None:
            for name in names:
                if hasattr(module, name):
                    setattr(module, name, forbidden)
    if bundle["provider"] == "totalsegmentator":
        allowed = set(TS_TASK_MODELS[bundle["task"]])

        def verified_weights(task_id, *args, **kwargs):
            if task_id not in allowed:
                raise ModelBundleError("Unregistered TotalSegmentator auxiliary model requested")
            # Upstream calls its downloader even when weights exist. Replace
            # that call with verification; it must never consult a remote URL.
            # The complete read-only bundle was verified before provider import.
            # Do not hash multi-gigabyte weights again for every part/crop call.
            return None

        for module_name in ("totalsegmentator.libs", "totalsegmentator.python_api"):
            module = sys.modules.get(module_name)
            if module is not None:
                module.download_pretrained_weights = verified_weights
        for module_name in ("totalsegmentator.config", "totalsegmentator.python_api"):
            module = sys.modules.get(module_name)
            if module is not None:
                module.send_usage_stats = lambda *args, **kwargs: None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("install", "register", "list", "verify"))
    parser.add_argument("--root")
    parser.add_argument("--registry")
    parser.add_argument("--sources")
    parser.add_argument("--provider", choices=PROVIDERS)
    parser.add_argument("--task")
    parser.add_argument("--path")
    parser.add_argument("--name")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    try:
        common = {"root": args.root, "registry": args.registry}
        if args.command == "list":
            result = list_bundles(**common)
        else:
            if not args.provider or not args.task:
                parser.error("--provider and --task are required")
            if args.command == "install":
                result = install_bundle(args.provider, args.task, sources=args.sources, force=args.force, **common)
            elif args.command == "register":
                result = register_bundle(args.provider, args.task, path=args.path, name=args.name, **common)
            else:
                result = verify_bundle(args.provider, args.task, **common)
        print(json.dumps(result, sort_keys=True))
    except (ModelBundleError, OSError, ValueError) as exc:
        print("ERROR: Model bundle unavailable: " + str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
