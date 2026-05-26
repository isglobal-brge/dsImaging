"""Shared helpers for dsImaging Python runners."""

import json
import hashlib
import os
import re
import sqlite3
import sys
from urllib.parse import urlparse


IMAGE_EXTS = (".nii.gz", ".nii", ".nrrd", ".mha", ".mhd", ".dcm", ".png", ".jpg", ".jpeg")
MASK_EXTS = (".nii.gz", ".nii", ".nrrd", ".mha", ".mhd", ".png", ".jpg", ".jpeg")


def cfg(name, default=None):
    value = os.environ.get(f"DSHPC_CFG_{name.upper()}")
    if value is None or value == "":
        return default
    return value


def cfg_bool(name, default=False):
    value = str(cfg(name, "")).strip().lower()
    if value == "":
        return bool(default)
    return value in ("1", "true", "yes", "y")


def cfg_int(name, default=0):
    try:
        return int(float(cfg(name, default)))
    except Exception:
        return int(default)


def cfg_float(name, default=0.0):
    try:
        return float(cfg(name, default))
    except Exception:
        return float(default)


def cfg_list(name, default=None):
    value = cfg(name, None)
    if value is None:
        return [] if default is None else default
    return [v.strip() for v in str(value).split(",") if v.strip()]


def strip_extensions(filename):
    lower = filename.lower()
    for ext in (".nii.gz", ".nii", ".nrrd", ".mha", ".mhd", ".dcm", ".png", ".jpg", ".jpeg"):
        if lower.endswith(ext):
            return filename[: -len(ext)]
    return os.path.splitext(filename)[0]


def safe_id(value):
    value = strip_extensions(os.path.basename(str(value)))
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    return value.strip("._") or "sample"


def image_files(root, extensions=IMAGE_EXTS):
    if not root or not os.path.exists(root):
        return []
    if os.path.isfile(root):
        return [root]
    out = []
    for base, _, files in os.walk(root):
        for name in files:
            if name.startswith("."):
                continue
            if name.lower().endswith(tuple(e.lower() for e in extensions)):
                out.append(os.path.join(base, name))
    return sorted(out)


def read_json(path, default=None):
    try:
        with open(path) as handle:
            return json.load(handle)
    except Exception:
        return default


def read_yaml(path, default=None):
    try:
        import yaml
        with open(path) as handle:
            return yaml.safe_load(handle)
    except Exception:
        return default


def read_yaml_text(text, default=None):
    try:
        import yaml
        return yaml.safe_load(text)
    except Exception:
        return default


def asset_uri(entry):
    if not isinstance(entry, dict):
        return None
    return entry.get("uri") or entry.get("root") or entry.get("file") or entry.get("path")


def is_s3_uri(uri):
    return isinstance(uri, str) and uri.startswith("s3://")


def parse_s3_uri(uri):
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Not an S3 URI: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def cache_root():
    return os.environ.get(
        "DSIMAGING_CACHE_DIR",
        os.path.join(os.environ.get("DSHPC_STAGING_ROOT", "/srv/dshpc/staging"),
                     "dsimaging_s3"),
    )


def registry_entry(dataset_id):
    registry_path = cfg("registry_path", os.environ.get("DSIMAGING_REGISTRY_PATH", "/var/lib/dsimaging/registry.yaml"))
    registry = read_yaml(registry_path, {})
    if not isinstance(registry, dict):
        return {}
    entry = registry.get(dataset_id)
    if not isinstance(entry, dict):
        return {}
    return entry


def load_persisted_credentials(ref):
    if not ref:
        return {}
    path = os.environ.get(
        "DSIMAGING_CREDENTIALS_PATH",
        cfg("credentials_path", "/var/lib/dsimaging/credentials.yaml"),
    )
    store = read_yaml(path, {})
    if not isinstance(store, dict):
        return {}
    cred = store.get(ref)
    return cred if isinstance(cred, dict) else {}


def s3_client_for_entry(entry):
    try:
        import boto3
        from botocore.config import Config
    except Exception as exc:
        raise RuntimeError(
            "boto3 is required to materialise S3-backed imaging assets. "
            "Reinstall dsImaging or add boto3 to the analysis environment."
        ) from exc

    ref = entry.get("credentials_ref")
    cred = load_persisted_credentials(ref)
    endpoint = entry.get("endpoint") or cred.get("endpoint") or None
    region = entry.get("region") or cred.get("region") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
    kwargs = {"region_name": region or "us-east-1"}
    if endpoint:
        kwargs["endpoint_url"] = endpoint

    access_key = (
        cred.get("access_key") or cred.get("identity") or
        os.environ.get("AWS_ACCESS_KEY_ID") or None
    )
    secret_key = (
        cred.get("secret_key") or cred.get("secret") or
        os.environ.get("AWS_SECRET_ACCESS_KEY") or None
    )
    if access_key:
        kwargs["aws_access_key_id"] = access_key
    if secret_key:
        kwargs["aws_secret_access_key"] = secret_key

    config = Config(signature_version="s3v4",
                    s3={"addressing_style": "path"} if endpoint else None)
    return boto3.client("s3", config=config, **kwargs)


def s3_get_text(entry, uri):
    bucket, key = parse_s3_uri(uri)
    s3 = s3_client_for_entry(entry)
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        return body.read().decode("utf-8")
    finally:
        body.close()


def s3_list_keys(entry, uri):
    bucket, prefix = parse_s3_uri(uri)
    s3 = s3_client_for_entry(entry)
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key")
            if key and not key.endswith("/"):
                keys.append((key, int(obj.get("Size", 0))))
    return bucket, prefix, sorted(keys)


def s3_download(entry, bucket, key, dest):
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    s3 = s3_client_for_entry(entry)
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        return dest
    tmp = f"{dest}.tmp"
    s3.download_file(bucket, key, tmp)
    os.replace(tmp, dest)
    return dest


def s3_materialize_uri(entry, uri, dataset_id, role="images"):
    bucket, key = parse_s3_uri(uri)
    digest = hashlib.sha256(uri.encode("utf-8")).hexdigest()[:16]
    root = os.path.join(cache_root(), safe_id(dataset_id), safe_id(role), digest)

    if key and not key.endswith("/"):
        dest = os.path.join(root, os.path.basename(key))
        return s3_download(entry, bucket, key, dest)

    bucket, prefix, keys = s3_list_keys(entry, uri)
    extensions = MASK_EXTS if role in ("mask", "masks") else IMAGE_EXTS
    selected = [
        (obj_key, size) for obj_key, size in keys
        if os.path.basename(obj_key) and
        os.path.basename(obj_key).lower().endswith(tuple(e.lower() for e in extensions))
    ]
    if not selected:
        return root

    for obj_key, _ in selected:
        rel = obj_key[len(prefix):].lstrip("/") if obj_key.startswith(prefix) else os.path.basename(obj_key)
        rel = rel or os.path.basename(obj_key)
        dest = os.path.join(root, rel)
        s3_download(entry, bucket, obj_key, dest)
    return root


def manifest_from_registry(dataset_id):
    entry = registry_entry(dataset_id)
    if not entry:
        return {}
    manifest_path = entry.get("manifest") or entry.get("manifest_uri")
    if not manifest_path:
        return {}
    if is_s3_uri(manifest_path):
        manifest = read_yaml_text(s3_get_text(entry, manifest_path), {})
        return manifest if isinstance(manifest, dict) else {}
    if not os.path.exists(manifest_path):
        return {}
    manifest = read_yaml(manifest_path, {})
    if not isinstance(manifest, dict):
        return {}
    return manifest


def manifest_assets_from_registry(dataset_id):
    manifest = manifest_from_registry(dataset_id)
    if not isinstance(manifest, dict):
        return {}
    return manifest.get("assets", {}) or {}


def resolve_catalog_asset_path(asset_name, dataset_id=None):
    if not asset_name:
        return None
    dataset_id = dataset_id or cfg("dataset_id", "")
    db_path = cfg("asset_db", os.environ.get("DSIMAGING_ASSET_DB", "/var/lib/dsimaging/imaging_assets.sqlite"))
    if not db_path or not os.path.exists(db_path):
        return None
    try:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        row = con.execute(
            "SELECT path_or_root FROM assets WHERE asset_id = ? AND status = 'active'",
            (asset_name,),
        ).fetchone()
        if row is None and dataset_id:
            row = con.execute(
                "SELECT a.path_or_root FROM asset_aliases aa "
                "JOIN assets a ON a.asset_id = aa.asset_id "
                "WHERE aa.dataset_id = ? AND aa.alias = ? AND a.status = 'active'",
                (dataset_id, asset_name),
            ).fetchone()
        con.close()
        if row is not None:
            path = row["path_or_root"]
            if path and os.path.exists(path):
                return path
    except Exception:
        return None
    return None


def resolve_asset_path(asset_name, role="images", explicit=None):
    if explicit and os.path.exists(explicit):
        return explicit

    dataset_id = cfg("dataset_id", "")
    catalog_path = resolve_catalog_asset_path(asset_name, dataset_id)
    if catalog_path:
        return catalog_path

    if dataset_id:
        entry = registry_entry(dataset_id)
        assets = manifest_assets_from_registry(dataset_id)
        candidates = [asset_name, role, "images" if role == "image" else role]
        for name in candidates:
            if not name:
                continue
            uri = asset_uri(assets.get(name))
            if uri and os.path.exists(uri):
                return uri
            if uri and is_s3_uri(uri) and entry:
                staged = s3_materialize_uri(entry, uri, dataset_id, role)
                if staged and os.path.exists(staged):
                    return staged

    input_dir = os.environ.get("DSHPC_INPUT_DIR") or cfg("input_dir")
    if input_dir and os.path.exists(input_dir):
        return input_dir

    return explicit


def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as handle:
        json.dump(obj, handle, indent=2)


def package_versions(packages):
    try:
        from importlib import metadata
    except Exception:
        metadata = None

    out = {"python": sys.version.split()[0]}
    for name in packages:
        version = None
        candidates = [name]
        if name == "radiomics":
            candidates.extend(["pyradiomics", "PyRadiomics"])
        for candidate in candidates:
            if metadata is None:
                continue
            try:
                version = metadata.version(candidate)
                break
            except Exception:
                pass
        if version is None:
            try:
                module = __import__(name)
                version = getattr(module, "__version__", None)
            except Exception:
                version = None
        if version:
            out[name] = str(version)
    return out
