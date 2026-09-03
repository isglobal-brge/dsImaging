"""Shared helpers for dsImaging Python runners."""

import json
import hashlib
import os
import re
import sys
from urllib.parse import urlparse


IMAGE_EXTS = (".nii.gz", ".nii", ".nrrd", ".mha", ".dcm", ".png", ".jpg", ".jpeg", ".tif", ".tiff")
MASK_EXTS = (".nii.gz", ".nii", ".nrrd", ".mha", ".png", ".jpg", ".jpeg", ".tif", ".tiff")


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
    for ext in IMAGE_EXTS:
        if lower.endswith(ext):
            return filename[: -len(ext)]
    return os.path.splitext(filename)[0]


def safe_id(value):
    value = strip_extensions(os.path.basename(str(value)))
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    return value.strip("._") or "sample"


def sample_token(value):
    """Return a collision-resistant filesystem token for a private sample id."""
    value = str(value)
    return hashlib.sha256(
        ("dsImaging:private-sample:" + value).encode("utf-8")
    ).hexdigest()


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


def worker_context():
    """Load the exact server-created context for this job.

    Analyst configuration may name logical assets, but it never selects a
    manifest, registry, database, or filesystem path.  The context id and path
    are injected by dsImaging after handle authorization.
    """
    context_id = cfg("dataset_id", "")
    if not re.fullmatch(r"dsctx_[0-9a-f]{64}", str(context_id)):
        return {}

    root = os.path.realpath(os.environ.get(
        "DSIMAGING_WORKER_CONTEXT_DIR", "/srv/dshpc/staging/dsimaging-contexts"
    ))
    expected = os.path.join(root, f"{context_id}.context.yaml")
    configured = cfg("worker_context", expected)
    if not configured or os.path.realpath(configured) != os.path.realpath(expected):
        return {}
    path = os.path.realpath(expected)
    try:
        if os.path.commonpath([root, path]) != root or not os.path.isfile(path):
            return {}
    except (OSError, ValueError):
        return {}

    context = read_yaml(path, {})
    if not isinstance(context, dict) or context.get("context_id") != context_id:
        return {}
    if context.get("schema_version") != 1:
        return {}
    manifest = context.get("manifest")
    backend = context.get("backend")
    if not isinstance(manifest, dict) or not isinstance(backend, dict):
        return {}
    return context


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


def s3_download(entry, bucket, key, dest, version_id=None):
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    s3 = s3_client_for_entry(entry)
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        return dest
    tmp = f"{dest}.tmp"
    if version_id is None:
        s3.download_file(bucket, key, tmp)
    else:
        s3.download_file(
            bucket, key, tmp, ExtraArgs={"VersionId": version_id})
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


def resolve_asset_path(asset_name, role="images", explicit=None):
    if not isinstance(asset_name, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", asset_name
    ) or ".." in asset_name:
        return None

    context = worker_context()
    if context:
        manifest = context["manifest"]
        assets = manifest.get("assets", {}) or {}
        backend = context.get("backend", {})
        entry = backend.get("config", {}) or {}
        context_id = context["context_id"]
        uri = asset_uri(assets.get(asset_name))
        if uri and os.path.exists(uri):
            return uri
        if uri and is_s3_uri(uri) and backend.get("type") == "s3":
            staged = s3_materialize_uri(entry, uri, context_id, role)
            if staged and os.path.exists(staged):
                return staged

    input_dir = os.environ.get("DSHPC_INPUT_DIR") or cfg("input_dir")
    if input_dir and os.path.exists(input_dir):
        return input_dir

    # Previous-step artifacts are already isolated inside this dsHPC job. An
    # analyst-provided absolute path is never accepted as a fallback.
    return None


def _privacy_sample_ids(context):
    manifest = context.get("manifest", {})
    roster = manifest.get(".dsimaging_privacy_roster", {})
    sample_ids = roster.get("sample_ids")
    if not isinstance(sample_ids, list) or not sample_ids:
        raise RuntimeError("The admitted collection roster is unavailable")
    sample_ids = [str(value) for value in sample_ids]
    if len(set(sample_ids)) != len(sample_ids):
        raise RuntimeError("The admitted collection roster is invalid")
    return sample_ids


def _safe_relative_path(value):
    if not isinstance(value, str) or not value or "\\" in value:
        raise RuntimeError("An imaging sample route is invalid")
    if value.startswith("/") or re.match(r"^[A-Za-z]:", value):
        raise RuntimeError("An imaging sample route is invalid")
    parts = value.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise RuntimeError("An imaging sample route is invalid")
    return value


def _sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_input_file(path, extensions=IMAGE_EXTS):
    """Reject unsupported or detached image containers before decoding."""
    if not os.path.isfile(path):
        raise RuntimeError("An admitted imaging sample is unavailable")
    lower = path.lower()
    if lower.endswith(".mhd"):
        raise RuntimeError("Detached MetaImage samples are not supported")
    if extensions and not lower.endswith(tuple(e.lower() for e in extensions)):
        raise RuntimeError("An admitted imaging sample has an unsupported format")
    if lower.endswith(".nrrd"):
        with open(path, "rb") as handle:
            prefix = handle.read(1024 * 1024 + 1)
        separators = [position for position in (
            prefix.find(b"\n\n"), prefix.find(b"\r\n\r\n"))
            if position >= 0]
        if not separators:
            raise RuntimeError("An admitted NRRD header is invalid")
        header = prefix[:min(separators)]
        for line in header.splitlines():
            line = line.strip()
            if not line or line.startswith(b"#") or b":" not in line:
                continue
            key = line.split(b":", 1)[0].strip().lower().replace(b" ", b"")
            if key == b"datafile":
                raise RuntimeError("Detached NRRD samples are not supported")
    if lower.endswith(".mha"):
        with open(path, "rb") as handle:
            prefix = handle.read(1024 * 1024 + 1)
        match = re.search(
            br"(?im)^\s*ElementDataFile\s*=\s*([^\r\n]+)", prefix)
        if match is None or match.group(1).strip().upper() != b"LOCAL":
            raise RuntimeError("Detached MetaImage samples are not supported")
    return path


def _verify_mapped_file(path, record, extensions):
    validate_input_file(path, extensions)
    expected_size = record.get("size")
    expected_hash = str(record.get("content_hash", "")).lower()
    try:
        expected_size = int(expected_size)
    except Exception as exc:
        raise RuntimeError("An admitted imaging sample has invalid integrity metadata") from exc
    if expected_size < 0 or not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
        raise RuntimeError("An admitted imaging sample has invalid integrity metadata")
    if os.path.getsize(path) != expected_size:
        raise RuntimeError("An admitted imaging sample failed integrity verification")
    if _sha256_file(path) != expected_hash:
        raise RuntimeError("An admitted imaging sample failed integrity verification")
    return path


def collection_sample_files(asset_name="images", role="images", extensions=IMAGE_EXTS):
    """Resolve the exact snapshot roster without scanning an asset directory."""
    context = worker_context()
    if not context:
        raise RuntimeError("The admitted collection mapping is unavailable")
    sample_ids = _privacy_sample_ids(context)
    mapping = context.get("collection_map")
    if not isinstance(mapping, dict) or mapping.get("version") != 1:
        raise RuntimeError("The admitted collection mapping is unavailable")
    asset_names = mapping.get("asset_names")
    if not isinstance(asset_names, list) or asset_name not in asset_names:
        raise RuntimeError("The requested asset has no exact sample mapping")
    records_by_asset = mapping.get("records_by_asset")
    if records_by_asset is None:
        records = mapping.get("records")
    elif not isinstance(records_by_asset, dict):
        raise RuntimeError("The admitted collection mapping is invalid")
    else:
        records = records_by_asset.get(asset_name)
    if not isinstance(records, list) or len(records) != len(sample_ids):
        raise RuntimeError("The admitted collection mapping is invalid")
    by_id = {}
    for record in records:
        if not isinstance(record, dict):
            raise RuntimeError("The admitted collection mapping is invalid")
        sid = str(record.get("sample_id", ""))
        if not sid or sid in by_id:
            raise RuntimeError("The admitted collection mapping is invalid")
        by_id[sid] = record
    if set(by_id) != set(sample_ids):
        raise RuntimeError("The admitted collection mapping is not the complete roster")

    manifest = context["manifest"]
    asset = (manifest.get("assets", {}) or {}).get(asset_name)
    root_uri = asset_uri(asset)
    backend = context.get("backend", {})
    backend_type = backend.get("type")
    entry = backend.get("config", {}) or {}
    if not isinstance(root_uri, str) or not root_uri:
        raise RuntimeError("The requested imaging asset is unavailable")

    resolved = []
    for sid in sample_ids:
        record = by_id[sid]
        if (record.get("source_kind") not in ("single_file", "mask_file") or
                record.get("n_files") != 1):
            raise RuntimeError("Multi-file imaging samples are not supported by this runner")
        uri = record.get("uri")
        relative = _safe_relative_path(record.get("relative_path"))
        version_id = record.get("version_id")
        if version_id is not None and (
                not isinstance(version_id, str) or not version_id or
                len(version_id.encode("utf-8")) > 1024 or
                "\r" in version_id or "\n" in version_id):
            raise RuntimeError("An admitted imaging sample has invalid integrity metadata")
        if not isinstance(uri, str) or not uri:
            raise RuntimeError("An admitted imaging sample route is invalid")

        if backend_type == "s3":
            root_bucket, root_key = parse_s3_uri(root_uri)
            bucket, key = parse_s3_uri(uri)
            prefix = root_key.rstrip("/") + "/"
            if bucket != root_bucket or not key.startswith(prefix):
                raise RuntimeError("An admitted imaging sample leaves its collection")
            if key[len(prefix):] != relative:
                raise RuntimeError("An admitted imaging sample route is inconsistent")
            cache_identity = "\0".join(
                (uri, version_id or "", str(record.get("content_hash", ""))))
            digest = hashlib.sha256(
                cache_identity.encode("utf-8")).hexdigest()[:16]
            path = os.path.join(cache_root(), context["context_id"],
                                safe_id(role), digest, os.path.basename(relative))
            s3_download(entry, bucket, key, path, version_id=version_id)
        elif backend_type == "file":
            root = os.path.realpath(root_uri)
            path = os.path.realpath(uri)
            try:
                if os.path.commonpath([root, path]) != root:
                    raise RuntimeError("An admitted imaging sample leaves its collection")
            except ValueError as exc:
                raise RuntimeError("An admitted imaging sample leaves its collection") from exc
            actual_relative = os.path.relpath(path, root).replace(os.sep, "/")
            if actual_relative != relative:
                raise RuntimeError("An admitted imaging sample route is inconsistent")
        else:
            raise RuntimeError("The admitted imaging backend is unavailable")

        resolved.append((_verify_mapped_file(path, record, extensions), sid))
    return resolved


def artifact_sample_files(root, artifact_types=None, extensions=None):
    """Read an exact per-sample mapping emitted by a prior trusted runner."""
    context = worker_context()
    if not context:
        raise RuntimeError("The admitted collection roster is unavailable")
    sample_ids = _privacy_sample_ids(context)
    if not root or not os.path.isdir(root):
        raise RuntimeError("The requested imaging asset is unavailable")
    manifest_path = os.path.join(root, "dsimaging_output_manifest.json")
    manifest = read_json(manifest_path, {})
    if not isinstance(manifest, dict) or manifest.get("schema_version") != 1:
        raise RuntimeError("The requested imaging asset has no exact sample mapping")
    artifact_type = manifest.get("artifact_type")
    if artifact_types and artifact_type not in artifact_types:
        raise RuntimeError("The requested imaging asset has an incompatible sample mapping")
    samples = manifest.get("samples")
    if not isinstance(samples, list) or len(samples) != len(sample_ids):
        raise RuntimeError("The requested imaging asset is not the complete roster")

    by_id = {}
    used_files = set()
    root_real = os.path.realpath(root)
    for sample in samples:
        if not isinstance(sample, dict):
            raise RuntimeError("The requested imaging asset mapping is invalid")
        sid = str(sample.get("sample_id", ""))
        files = sample.get("files")
        primary = sample.get("primary")
        integrity = sample.get("file_integrity")
        if not sid or sid in by_id or not isinstance(files, list) or not files:
            raise RuntimeError("The requested imaging asset mapping is invalid")
        files = [_safe_relative_path(value) for value in files]
        if (len(set(files)) != len(files) or primary not in files or
                not isinstance(integrity, list) or len(integrity) != len(files)):
            raise RuntimeError("The requested imaging asset mapping is invalid")
        integrity_by_path = {}
        for record in integrity:
            if not isinstance(record, dict):
                raise RuntimeError("The requested imaging asset mapping is invalid")
            relative = _safe_relative_path(record.get("path"))
            if relative in integrity_by_path:
                raise RuntimeError("The requested imaging asset mapping is invalid")
            integrity_by_path[relative] = record
        if set(integrity_by_path) != set(files):
            raise RuntimeError("The requested imaging asset mapping is invalid")
        if any(value in used_files for value in files):
            raise RuntimeError("An imaging artifact is attributed to multiple samples")
        used_files.update(files)
        primary_path = None
        for relative in files:
            path = os.path.realpath(os.path.join(root_real, relative))
            try:
                if os.path.commonpath([root_real, path]) != root_real or not os.path.isfile(path):
                    raise RuntimeError("A mapped imaging artifact is unavailable")
            except ValueError as exc:
                raise RuntimeError("A mapped imaging artifact is unavailable") from exc
            if extensions and not path.lower().endswith(tuple(e.lower() for e in extensions)):
                raise RuntimeError("A mapped imaging artifact has an unsupported format")
            _verify_mapped_file(path, {
                "size": integrity_by_path[relative].get("size"),
                "content_hash": integrity_by_path[relative].get("sha256"),
            }, extensions)
            if relative == primary:
                primary_path = path
        if primary_path is None:
            raise RuntimeError("The requested imaging asset mapping is invalid")
        by_id[sid] = primary_path
    if set(by_id) != set(sample_ids):
        raise RuntimeError("The requested imaging asset is not the complete roster")
    return [(by_id[sid], sid) for sid in sample_ids]


def mapped_sample_files(asset_name, role="images", artifact_types=None,
                        extensions=IMAGE_EXTS):
    context = worker_context()
    mapping = context.get("collection_map", {}) if context else {}
    if asset_name in (mapping.get("asset_names") or []):
        return collection_sample_files(asset_name, role, extensions)
    root = resolve_asset_path(asset_name, role)
    return artifact_sample_files(root, artifact_types, extensions)


def write_collection_output_manifest(output_dir, artifact_type, samples):
    """Write the canonical complete-roster map consumed by later runners."""
    context = worker_context()
    if not context:
        raise RuntimeError("The admitted collection roster is unavailable")
    sample_ids = _privacy_sample_ids(context)
    if not isinstance(samples, dict) or set(samples) != set(sample_ids):
        raise RuntimeError("Runner output is not the complete admitted collection")
    root = os.path.realpath(output_dir)
    encoded = []
    used_files = set()
    for sid in sample_ids:
        sample = samples[sid]
        if not isinstance(sample, dict):
            raise RuntimeError("Runner output sample mapping is invalid")
        primary = sample.get("primary")
        files = sample.get("files") or ([primary] if primary else [])
        relative_files = []
        integrity = []
        relative_primary = None
        for path in files:
            path_real = os.path.realpath(path)
            try:
                if os.path.commonpath([root, path_real]) != root or not os.path.isfile(path_real):
                    raise RuntimeError("Runner output leaves its artifact directory")
            except ValueError as exc:
                raise RuntimeError("Runner output leaves its artifact directory") from exc
            relative = _safe_relative_path(os.path.relpath(path_real, root).replace(os.sep, "/"))
            if relative in used_files:
                raise RuntimeError("Runner output is attributed to multiple samples")
            used_files.add(relative)
            relative_files.append(relative)
            integrity.append({
                "path": relative,
                "size": os.path.getsize(path_real),
                "sha256": _sha256_file(path_real),
            })
            if primary and os.path.realpath(primary) == path_real:
                relative_primary = relative
        if not relative_files or relative_primary is None:
            raise RuntimeError("Runner output sample mapping is invalid")
        encoded.append({
            "sample_id": sid,
            "primary": relative_primary,
            "files": relative_files,
            "file_integrity": integrity,
        })
    path = os.path.join(output_dir, "dsimaging_output_manifest.json")
    write_json(path, {
        "schema_version": 1,
        "artifact_type": artifact_type,
        "samples": encoded,
    })
    return path


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
