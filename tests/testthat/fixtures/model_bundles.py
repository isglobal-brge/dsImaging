"""Synthetic model bundles and fake providers; no upstream weights or network needed."""

import argparse
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import socket
import stat
import subprocess
import sys
import types
import zipfile

import dsimaging_model_bundles as bundles


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


def file_entry(path, relative):
    return {"path": relative, "size": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def make_bundle(root, provider="lungmask", task="R231", register=True):
    """Construct a manifest independently of the production installer."""
    directory = Path(root) / provider / task
    directory.mkdir(parents=True, exist_ok=True)
    version = "2.4.0" if provider == "totalsegmentator" else "0.0.1"
    if provider == "lungmask":
        files = {"weights/model.pth": b"synthetic primary weight"}
        runtime = {"model_path": "weights/model.pth"}
        if task == "LTRCLobes_R231":
            files["weights/fill.pth"] = b"synthetic fill weight"
            runtime["fillmodel_path"] = "weights/fill.pth"
    elif provider == "nnunetv2":
        files = {"model/dataset.json": b"{}", "model/plans.json": b"{}",
                 "model/fold_0/checkpoint_final.pth": b"synthetic nnUNet weight"}
        runtime = {"model_folder": "model", "folds": [0],
                   "checkpoint": "checkpoint_final.pth"}
    elif provider == "monai":
        files = {"configs/inference.json": json.dumps({
                    "image": "", "output_dir": "", "ckpt_path": "", "run": "inference"}).encode(),
                 "configs/metadata.json": b"{}", "models/model.pt": b"synthetic MONAI weight"}
        runtime = {"inference_config": "configs/inference.json",
                   "metadata": "configs/metadata.json", "weight_files": ["models/model.pt"]}
    elif provider == "totalsegmentator":
        files = {}
        models = []
        for model_id, folder, trainer in (
            (258, "Dataset258_lung_vessels_248subj", "nnUNetTrainer"),
            (298, "Dataset298_TotalSegmentator_total_6mm_1559subj", "nnUNetTrainer_4000epochs_NoMirroring"),
        ):
            model = {"id": model_id, "folder": folder,
                     "trainer": trainer, "configuration": "3d_fullres"}
            models.append(model)
            base = f"weights/{model['folder']}/{model['trainer']}__nnUNetPlans__{model['configuration']}"
            for relative, content in {"dataset.json": b"{}", "plans.json": b"{}",
                                      "fold_0/checkpoint_final.pth": f"weight {model_id}".encode()}.items():
                files[f"{base}/{relative}"] = content
        runtime = {"weights_path": "weights", "models": models}
    else:
        raise AssertionError(provider)
    for relative, content in files.items():
        path = directory / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    manifest = {"schema_version": 1, "provider": provider, "task": task,
                "upstream_version": version, "licence": "Synthetic test fixture only",
                "source_url": "https://example.invalid/synthetic-model",
                "download_date": "2026-09-23T00:00:00Z", "runtime": runtime,
                "files": [file_entry(directory / name, name) for name in sorted(files)]}
    write_json(directory / "manifest.json", manifest)
    if register:
        bundles.register_bundle(provider, task, root=str(root))
    return directory, manifest


def expect_refusal(action, label):
    try:
        action()
    except (RuntimeError, ValueError, OSError) as error:
        assert str(error).strip(), f"No explanatory error for {label}"
        return
    raise AssertionError(f"Accepted {label}")


def case_verification(root):
    count = 0
    for name, mutate in (
        ("missing weight", lambda d, m: (d / m["files"][0]["path"]).unlink()),
        ("same-size corruption", lambda d, m: (d / m["files"][0]["path"]).write_bytes(
            b"x" * m["files"][0]["size"])),
        ("size corruption", lambda d, m: (d / m["files"][0]["path"]).write_bytes(b"changed")),
        ("unlisted weight", lambda d, m: (d / "unlisted.pth").write_bytes(b"extra")),
        ("manifest modification", lambda d, m: write_json(d / "manifest.json", {**m, "licence": "changed"})),
        ("unregistered bundle", lambda d, m: (d.parent.parent / "registry" / "lungmask" / "R231.json").unlink()),
        ("missing manifest", lambda d, m: (d / "manifest.json").unlink()),
    ):
        model_root = root / str(count)
        directory, manifest = make_bundle(model_root)
        verified = bundles.verify_bundle("lungmask", "R231", root=str(model_root))
        assert verified is not None
        mutate(directory, manifest)
        expect_refusal(lambda: bundles.verify_bundle("lungmask", "R231", root=str(model_root)), name)
        count += 1
    expect_refusal(lambda: bundles.verify_bundle("lungmask", "R231", root=str(root / "absent")), "absent root")
    count += 1
    for name in ("../escape", "/tmp/model", "a/b", "a\\b", ".", ".."):
        expect_refusal(lambda: bundles.verify_bundle("lungmask", name, root=str(root)), "unsafe task " + name)
        count += 1
    for unsafe in ("../outside.pth", "/outside.pth", "weights/../../outside.pth", "weights\\model.pth"):
        model_root = root / str(count)
        directory, manifest = make_bundle(model_root, register=False)
        manifest["files"][0]["path"] = unsafe
        write_json(directory / "manifest.json", manifest)
        expect_refusal(lambda: bundles.register_bundle("lungmask", "R231", root=str(model_root)), "unsafe file " + unsafe)
        count += 1
    for kind in ("file", "directory", "manifest", "bundle"):
        model_root = root / str(count)
        directory, manifest = make_bundle(model_root)
        target = {"file": directory / "weights" / "model.pth", "directory": directory / "weights",
                  "manifest": directory / "manifest.json", "bundle": directory}[kind]
        outside = root / ("outside-" + kind)
        target.rename(outside)
        target.symlink_to(outside, target_is_directory=outside.is_dir())
        expect_refusal(lambda: bundles.verify_bundle("lungmask", "R231", root=str(model_root)), kind + " symlink")
        count += 1
    for provider, task in (("lungmask", "LTRCLobes_R231"), ("nnunetv2", "synthetic"),
                           ("monai", "synthetic"), ("totalsegmentator", "lung_vessels")):
        model_root = root / str(count)
        directory, manifest = make_bundle(model_root, provider, task, register=False)
        missing = next(item for item in manifest["files"]
                       if ("fill.pth" if provider == "lungmask" else
                           "checkpoint_final.pth" if provider in ("nnunetv2", "totalsegmentator") else
                           "model.pt") in item["path"])
        (directory / missing["path"]).unlink()
        manifest["files"].remove(missing)
        write_json(directory / "manifest.json", manifest)
        expect_refusal(lambda: bundles.register_bundle(provider, task, root=str(model_root)), provider + " incomplete model")
        count += 1
    model_root = root / "crop"
    directory, manifest = make_bundle(model_root, "totalsegmentator", "lung_vessels", register=False)
    crop = manifest["runtime"]["models"].pop()
    shutil.rmtree(directory / "weights" / crop["folder"])
    manifest["files"] = [item for item in manifest["files"] if crop["folder"] not in item["path"]]
    write_json(directory / "manifest.json", manifest)
    expect_refusal(lambda: bundles.register_bundle("totalsegmentator", "lung_vessels", root=str(model_root)), "missing TS crop model")
    count += 1
    model_root = root / "empty-required-weight"
    directory, manifest = make_bundle(model_root, register=False)
    weight = directory / "weights/model.pth"
    weight.write_bytes(b"")
    manifest["files"] = [file_entry(weight, "weights/model.pth")]
    write_json(directory / "manifest.json", manifest)
    expect_refusal(lambda: bundles.register_bundle("lungmask", "R231", root=str(model_root)), "empty required weight")
    count += 1
    model_root = root / "listing"
    directory, manifest = make_bundle(model_root)
    listed = bundles.list_bundles(root=str(model_root))
    digest = hashlib.sha256((directory / "manifest.json").read_bytes()).hexdigest()
    assert len(listed) == 1 and listed[0]["provider"] == "lungmask"
    assert listed[0]["task"] == "R231" and listed[0]["manifest_sha256"] == digest
    assert listed[0]["ready"]
    (directory / "weights" / "model.pth").write_bytes(b"damaged")
    assert not any(item["ready"] for item in bundles.list_bundles(root=str(model_root))), \
        "Corrupt bundle advertised as installed"
    loader_root = root / "loaders"
    directory, _ = make_bundle(loader_root)
    bundle = bundles.verify_bundle("lungmask", "R231", root=str(loader_root))
    outside = root / "undeclared-checkpoint.bin"
    outside.write_bytes(b"not in manifest")
    for module_name in ("torch", "torch.jit", "torch.serialization"):
        module = types.ModuleType(module_name)
        module.load = lambda *args, **kwargs: "loaded"
        sys.modules[module_name] = module
    bundles.guard_checkpoint_loads(bundle)
    for module_name in ("torch", "torch.jit", "torch.serialization"):
        load = sys.modules[module_name].load
        assert load(directory / "weights/model.pth") == "loaded"
        with open(directory / "weights/model.pth", "rb") as handle:
            assert load(handle) == "loaded"
        expect_refusal(lambda: load(outside), "unlisted checkpoint with arbitrary extension")
        expect_refusal(lambda: load(io.BytesIO(b"undeclared checkpoint")), "anonymous checkpoint stream")
    return {"negative_cases": count, "listed_bundles": 1, "digest_pinned": True,
            "crop_required": True, "checkpoint_refusals": 6}


def case_installer(root):
    source, manifest = make_bundle(root / "source", register=False)
    # A complete bundle may contain an empty Python package marker; required
    # checkpoints still cannot be empty. Direct and archive transport agree.
    support = source / "scripts" / "__init__.py"
    support.parent.mkdir()
    support.touch()
    manifest["files"].append(file_entry(support, "scripts/__init__.py"))
    recipe = copy.deepcopy(manifest)
    recipe.pop("download_date")
    for item in recipe["files"]:
        item["url"] = (source / item["path"]).as_uri()
    model_root = root / "installed"
    recipe_path = model_root / "sources" / "lungmask" / "R231.json"
    write_json(recipe_path, recipe)
    bundles.install_bundle("lungmask", "R231", root=str(model_root))
    installed = model_root / "lungmask" / "R231"
    written = json.loads((installed / "manifest.json").read_text())
    assert written["download_date"] and written["provider"] == "lungmask"
    assert written["files"][0]["sha256"] == manifest["files"][0]["sha256"]
    assert (installed / "scripts/__init__.py").stat().st_size == 0
    assert bundles.verify_bundle("lungmask", "R231", root=str(model_root))
    # A second install must use its verified registration even if the source is gone.
    (source / recipe["files"][0]["path"]).unlink()
    bundles.install_bundle("lungmask", "R231", root=str(model_root))
    for index, field in enumerate(("sha256", "size", "url")):
        bad_root = root / ("bad-" + field)
        bad = copy.deepcopy(recipe)
        fresh_source, fresh = make_bundle(root / ("fresh-" + field), register=False)
        bad["files"][0]["url"] = (fresh_source / fresh["files"][0]["path"]).as_uri()
        bad["files"][0][field] = {"sha256": "0" * 64, "size": 1,
                                    "url": (root / "does-not-exist.pth").as_uri()}[field]
        write_json(bad_root / "sources" / "lungmask" / "R231.json", bad)
        expect_refusal(lambda: bundles.install_bundle("lungmask", "R231", root=str(bad_root)), "bad source " + field)
        assert not bundles.list_bundles(root=str(bad_root))
        assert not (bad_root / "registry" / "lungmask" / "R231.json").exists()
    ts_source, ts_manifest = make_bundle(root / "ts-source", "totalsegmentator", "lung_vessels", register=False)
    ts_recipe = copy.deepcopy(ts_manifest)
    ts_recipe.pop("download_date")
    for item in ts_recipe["files"]:
        item["url"] = (ts_source / item["path"]).as_uri()
    ts_root = root / "ts-installed"
    write_json(ts_root / "sources" / "totalsegmentator" / "lung_vessels.json", ts_recipe)
    bundles.install_bundle("totalsegmentator", "lung_vessels", root=str(ts_root))
    ts_installed = ts_root / "totalsegmentator" / "lung_vessels"
    for item in ts_manifest["files"]:
        assert hashlib.sha256((ts_installed / item["path"]).read_bytes()).hexdigest() == item["sha256"]
    assert len(ts_manifest["files"]) == 6
    broken_root = root / "ts-missing-crop"
    crop = next(item for item in ts_recipe["files"] if "298" in item["path"] and item["path"].endswith(".pth"))
    crop["url"] = (root / "missing-crop.pth").as_uri()
    write_json(broken_root / "sources" / "totalsegmentator" / "lung_vessels.json", ts_recipe)
    expect_refusal(lambda: bundles.install_bundle("totalsegmentator", "lung_vessels", root=str(broken_root)),
                   "download omitted TS auxiliary weight")
    assert not (broken_root / "registry" / "totalsegmentator" / "lung_vessels.json").exists()
    for index, invalid in enumerate(([], None)):
        invalid_root = root / f"invalid-recipe-{index}"
        write_json(invalid_root / "sources/lungmask/R231.json", invalid)
        expect_refusal(lambda: bundles.install_bundle("lungmask", "R231", root=str(invalid_root)),
                       "non-object source recipe")
    # Fail the atomic promotion only after the existing target was backed up.
    # Both old bytes and registry pin must survive a failed forced upgrade.
    prior = bundles.verify_bundle("lungmask", "R231", root=str(model_root))
    pin_path = model_root / "registry/lungmask/R231.json"
    old_pin = pin_path.read_bytes()
    source_weight = source / recipe["files"][0]["path"]
    source_weight.write_bytes(b"new verified replacement weight")
    recipe["files"] = [{**file_entry(source_weight, "weights/model.pth"), "url": source_weight.as_uri()}]
    write_json(recipe_path, recipe)
    replace = bundles.os.replace
    promotion_failed = False
    def fail_promotion(source_path, destination):
        nonlocal promotion_failed
        if Path(destination) == installed and not promotion_failed:
            promotion_failed = True
            raise OSError("Injected atomic promotion failure")
        return replace(source_path, destination)
    bundles.os.replace = fail_promotion
    try:
        expect_refusal(lambda: bundles.install_bundle("lungmask", "R231", root=str(model_root), force=True),
                       "atomic promotion failure")
    finally:
        bundles.os.replace = replace
    assert promotion_failed
    restored = bundles.verify_bundle("lungmask", "R231", root=str(model_root))
    assert restored["ready"] and restored["manifest_sha256"] == prior["manifest_sha256"]
    assert pin_path.read_bytes() == old_pin
    assert (installed / "weights/model.pth").read_bytes() == b"synthetic primary weight"
    return {"downloads": len(manifest["files"]) + len(ts_manifest["files"]), "idempotent": True, "negative_cases": 7,
            "complete_manifest": True, "rollback_preserved": True}


def case_archives(root):
    source, manifest = make_bundle(root / "source", register=False)
    content = (source / "weights/model.pth").read_bytes()
    for mode in ("valid", "digest", "size", "traversal", "symlink", "undeclared"):
        archive = root / (mode + ".zip")
        with zipfile.ZipFile(archive, "w") as zipped:
            if mode == "symlink":
                member = zipfile.ZipInfo("model.pth")
                member.create_system = 3
                member.external_attr = (stat.S_IFLNK | 0o777) << 16
                zipped.writestr(member, "../../outside.pth")
            elif mode == "traversal":
                zipped.writestr("../outside.pth", content)
            else:
                zipped.writestr("model.pth", content)
                zipped.writestr("__init__.py", b"")
                if mode == "undeclared":
                    zipped.writestr("hidden.pth", b"unlisted weight")
        recipe = copy.deepcopy(manifest)
        recipe.pop("download_date")
        recipe["files"].append({"path": "weights/__init__.py", "size": 0,
                                "sha256": hashlib.sha256(b"").hexdigest()})
        archive_source = {"url": archive.as_uri(), "size": archive.stat().st_size,
                          "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                          "destination": "weights"}
        if mode == "digest":
            archive_source["sha256"] = "0" * 64
        if mode == "size":
            archive_source["size"] += 1
        recipe["archives"] = [archive_source]
        model_root = root / (mode + "-installed")
        write_json(model_root / "sources/lungmask/R231.json", recipe)
        if mode == "valid":
            bundles.install_bundle("lungmask", "R231", root=str(model_root))
            bundle = bundles.verify_bundle("lungmask", "R231", root=str(model_root))
            assert bundle["ready"]
            assert (Path(bundle["path"]) / "weights/model.pth").read_bytes() == content
            assert (Path(bundle["path"]) / "weights/__init__.py").read_bytes() == b""
            assert bundle["manifest"]["archives"][0]["sha256"] == archive_source["sha256"]
        else:
            expect_refusal(lambda: bundles.install_bundle("lungmask", "R231", root=str(model_root)), mode + " ZIP")
            assert not (model_root / "registry/lungmask/R231.json").exists()
            assert not any(model_root.rglob("outside.pth"))
    return {"verified_archives": 1, "negative_cases": 5}


COMMON_PROVIDER = '''import json, os, pathlib, socket, subprocess, sys, urllib.request
def record(event, **values):
    with open(os.environ["FAKE_PROVIDER_TRACE"], "a") as handle:
        handle.write(json.dumps({"event": event, **values}) + "\\n")
def child_probe():
    try:
        urllib.request.urlopen(os.environ["FAKE_DOWNLOAD_URL"], timeout=1).read()
    except Exception as error:
        record("spawn_result", error=str(error))
        raise
    raise AssertionError("Spawned worker download was permitted")
def check():
    for name in ("HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE", "HF_DATASETS_OFFLINE", "DSIMAGING_MODEL_OFFLINE"):
        assert os.environ.get(name) == "1", name
    record("import", offline=True)
    mode = os.environ.get("FAKE_PROVIDER_MODE", "normal")
    if mode == "warm-cache":
        try:
            pathlib.Path(os.environ["FAKE_CACHE_MODEL"]).read_bytes()
        except Exception as error:
            record("cache_refused", error=str(error))
            raise
        raise AssertionError("Unregistered cached model was read during provider import")
    if mode == "download":
        try:
            urllib.request.urlopen(os.environ["FAKE_DOWNLOAD_URL"], timeout=1).read()
        except Exception as error:
            record("download_refused", error=str(error))
            raise
        raise AssertionError("Download was permitted")
    if mode == "child-download":
        code = "import os,socket;socket.create_connection(('127.0.0.1',int(os.environ['FAKE_DOWNLOAD_PORT'])),timeout=1)"
        try:
            result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
        except Exception as error:
            record("child_result", status=1, error=str(error))
            raise
        record("child_result", status=result.returncode, error=result.stderr)
        result.check_returncode()
        raise AssertionError("Child download was permitted")
    if mode == "spawn-download":
        import multiprocessing
        os.environ["FAKE_PROVIDER_MODE"] = "spawn-child"
        process = multiprocessing.get_context("spawn").Process(target=child_probe)
        process.start()
        process.join(10)
        if process.is_alive():
            process.terminate()
            raise AssertionError("Spawned download worker hung")
        if process.exitcode == 0:
            raise AssertionError("Spawned worker download was permitted")
        raise RuntimeError("Spawned worker refused its network download")
check()
'''


def fake_distribution(modules, provider, version):
    metadata = Path(modules) / f"{provider}-{version}.dist-info" / "METADATA"
    metadata.parent.mkdir(parents=True, exist_ok=True)
    metadata.write_text(f"Metadata-Version: 2.1\nName: {provider}\nVersion: {version}\n")


def fake_providers(root):
    modules = root / "fake_modules"
    modules.mkdir()
    (modules / "fake_common.py").write_text(COMMON_PROVIDER)
    definitions = {
        "lungmask/__init__.py": '''from fake_common import record
import pathlib
import SimpleITK as sitk
class LMInferer:
    def __init__(self, **kwargs):
        assert pathlib.Path(kwargs["modelpath"]).is_file()
        record("lungmask", **kwargs)
    def apply(self, image):
        return (sitk.GetArrayFromImage(image) > 0).astype("uint8")
''',
        "totalsegmentator/python_api.py": '''from fake_common import record
import os, pathlib
import SimpleITK as sitk
def totalsegmentator(image, output, **kwargs):
    weights = pathlib.Path(os.environ["TOTALSEG_WEIGHTS_PATH"])
    assert weights.is_dir()
    assert any("298" in str(path) for path in weights.rglob("checkpoint_final.pth"))
    record("totalsegmentator", weights=str(weights), **kwargs)
    sitk.WriteImage(sitk.Cast(sitk.ReadImage(image) > 0, sitk.sitkUInt8), str(pathlib.Path(output) / "lung_vessels.nii.gz"))
''',
        "nnunetv2/inference/predict_from_raw_data.py": '''from fake_common import record
import pathlib, shutil
class nnUNetPredictor:
    def __init__(self, *args, **kwargs): pass
    def initialize_from_trained_model_folder(self, folder, **kwargs):
        assert (pathlib.Path(folder) / "fold_0/checkpoint_final.pth").is_file()
        record("nnunetv2", folder=str(folder), **kwargs)
    def predict_from_files(self, source, output, **kwargs):
        for path in pathlib.Path(source).glob("*_0000.nii.gz"):
            shutil.copyfile(path, pathlib.Path(output) / path.name.replace("_0000.nii.gz", ".nii.gz"))
''',
        "monai/bundle.py": '''from fake_common import record
import pathlib
import SimpleITK as sitk
def run(**kwargs):
    assert (pathlib.Path(kwargs["bundle_root"]) / "models/model.pt").is_file()
    assert pathlib.Path(kwargs["ckpt_path"]) == pathlib.Path(kwargs["bundle_root"]) / "models/model.pt"
    record("monai", **kwargs)
    image = sitk.ReadImage(kwargs["image"])
    sitk.WriteImage(sitk.Cast(image > 0, sitk.sitkUInt8), str(pathlib.Path(kwargs["output_dir"]) / "mask.nii.gz"))
'''}
    for relative, code in definitions.items():
        path = modules / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(code)
        current = path.parent
        while current != modules:
            (current / "__init__.py").touch(exist_ok=True)
            current = current.parent
    for provider in ("lungmask", "totalsegmentator", "nnunetv2", "monai"):
        fake_distribution(modules, provider, "2.4.0" if provider == "totalsegmentator" else "0.0.1")
    return modules


def case_runners(root):
    import numpy as np
    import SimpleITK as sitk
    modules = fake_providers(root)
    model_root = root / "models"
    image = root / "image.nii.gz"
    sitk.WriteImage(sitk.GetImageFromArray(np.ones((2, 3, 4), dtype="uint8")), str(image))
    python_dir = Path(bundles.__file__).parent
    env = {**os.environ, "PYTHONPATH": str(modules) + os.pathsep + str(python_dir),
           "PYTHONDONTWRITEBYTECODE": "1", "DSIMAGING_MODELS": str(model_root),
           "DSHPC_CFG_MODELPATH": "/analyst/forbidden.pth", "DSHPC_CFG_MODEL_PATH": "/analyst/forbidden.pth",
           "HF_HUB_OFFLINE": "0", "HF_DATASETS_OFFLINE": "0", "TRANSFORMERS_OFFLINE": "0"}
    providers = {"lungmask": ("R231", "lungmask", "--model"),
                 "totalsegmentator": ("lung_vessels", "totalseg", "--task"),
                 "nnunetv2": ("synthetic", "nnunet", "--model"),
                 "monai": ("synthetic", "monai", "--bundle")}
    negative = 0
    download_refusals = 0
    cache_refusals = 0
    outside = root / "pytorch_model.bin"
    outside.write_bytes(b"unregistered warm-cache checkpoint")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(0.05)
        port = listener.getsockname()[1]
        env.update(FAKE_DOWNLOAD_URL=f"http://127.0.0.1:{port}/weights", FAKE_DOWNLOAD_PORT=str(port))
        for provider, (task, script, flag) in providers.items():
            directory, manifest = make_bundle(model_root, provider, task)
            def execute(name, **changes):
                trace = root / f"{provider}-{name}.jsonl"
                result = subprocess.run([sys.executable, str(python_dir / f"dsimaging_seg_{script}.py"),
                    "--input", str(root), "--output", str(root / f"output-{provider}-{name}"),
                    flag, task, "--image", str(image), "--sample-id", "synthetic"],
                    env={**env, "FAKE_PROVIDER_TRACE": str(trace), **changes}, capture_output=True, text=True)
                events = [json.loads(line) for line in trace.read_text().splitlines()] if trace.exists() else []
                return result, events
            result, events = execute("valid")
            assert result.returncode == 0, f"{provider}: {result.stderr}\n{result.stdout}"
            assert any(event.get("offline") for event in events), events
            used = next(event for event in events if event["event"] == provider)
            expected = {"lungmask": ("modelpath", directory / "weights/model.pth"),
                        "totalsegmentator": ("weights", directory / "weights"),
                        "nnunetv2": ("folder", directory / "model"),
                        "monai": ("bundle_root", directory)}[provider]
            assert Path(used[expected[0]]) == expected[1]
            if provider == "nnunetv2":
                assert used["use_folds"] == [0]
                assert used["checkpoint_name"] == "checkpoint_final.pth"
                result, events = execute("fold-0", DSHPC_CFG_FOLD="0")
                assert result.returncode == 0, result.stderr
                assert next(event for event in events if event["event"] == provider)["use_folds"] == [0]
                result, events = execute("unregistered-fold", DSHPC_CFG_FOLD="9")
                assert result.returncode != 0 and not events, "Unregistered fold reached provider"
                negative += 1
            if provider == "lungmask":
                primary_task, task = task, "LTRCLobes_R231"
                composite, _ = make_bundle(model_root, provider, task)
                result, events = execute("composite")
                assert result.returncode == 0, result.stderr
                used = next(event for event in events if event["event"] == provider)
                assert used["modelname"] == "LTRCLobes" and used["fillmodel"] == "R231"
                assert Path(used["modelpath"]) == composite / "weights/model.pth"
                assert Path(used["fillmodel_path"]) == composite / "weights/fill.pth"
                task = primary_task
            result, events = execute("warm-cache", FAKE_PROVIDER_MODE="warm-cache",
                                     FAKE_CACHE_MODEL=str(outside))
            assert result.returncode != 0, "Unregistered cached model reached inference"
            assert any(event["event"] == "cache_refused" and "verified bundle" in event["error"]
                       for event in events), events
            cache_refusals += 1
            for mode in ("download", "child-download", "spawn-download"):
                result, events = execute(mode, FAKE_PROVIDER_MODE=mode)
                assert result.returncode != 0, f"{provider} permitted {mode}"
                refusals = [event for event in events if event["event"] in
                            ("download_refused", "child_result", "spawn_result")]
                assert refusals and any("offline" in event.get("error", "").lower() or
                                        "network" in event.get("error", "").lower() for event in refusals), events
                try:
                    connection, _ = listener.accept()
                except socket.timeout:
                    pass
                else:
                    connection.close()
                    raise AssertionError(f"{provider} contacted the model download server")
                download_refusals += 1
            metadata = modules / f"{provider}-{manifest['upstream_version']}.dist-info" / "METADATA"
            original = metadata.read_bytes()
            metadata.write_text(f"Metadata-Version: 2.1\nName: {provider}\nVersion: 0.0.9\n")
            result, events = execute("version")
            assert result.returncode != 0 and not events, "Version mismatch reached provider"
            negative += 1
            metadata.write_bytes(original)
            weight = next(directory / item["path"] for item in manifest["files"]
                          if item["path"].endswith((".pt", ".pth")))
            weight.write_bytes(b"x" * weight.stat().st_size)
            result, events = execute("tampered")
            assert result.returncode != 0 and not events, "Corrupt weight reached provider"
            negative += 1
            shutil.rmtree(directory)
            result, events = execute("absent")
            assert result.returncode != 0 and not events, "Absent bundle reached provider"
            negative += 1
    return {"providers": 4, "negative_cases": negative,
            "download_refusals": download_refusals, "explicit_paths": True,
            "offline": True, "composite_lungmask": True, "cache_refusals": cache_refusals}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", choices=("verification", "installer", "archives", "runners"), required=True)
    parser.add_argument("--root", required=True)
    args = parser.parse_args()
    root = Path(args.root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    print(json.dumps(globals()["case_" + args.case](root)))
