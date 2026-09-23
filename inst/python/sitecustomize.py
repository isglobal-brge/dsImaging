"""Propagate dsImaging's offline inference guard into Python spawn workers."""
import os

if os.environ.get("DSIMAGING_MODEL_OFFLINE") == "1":
    from dsimaging_model_bundles import install_offline_guard
    install_offline_guard()
