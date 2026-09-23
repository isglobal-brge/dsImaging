#!/usr/bin/env python3
"""Basic WSI/pathology tiling runner."""

import argparse
import os
import sys

from dsimaging_utils import (
    cfg, cfg_bool, cfg_float, cfg_int, mapped_sample_files, package_versions,
    sample_token, write_collection_output_manifest, write_json,
)


WSI_EXTS = (".svs", ".tif", ".tiff", ".ndpi", ".png", ".jpg", ".jpeg")


def open_slide(path):
    try:
        import openslide
        return ("openslide", openslide.OpenSlide(path))
    except Exception:
        from PIL import Image
        return ("pil", Image.open(path))


def dimensions(handle):
    kind, obj = handle
    return obj.dimensions if kind == "openslide" else obj.size


def read_region(handle, x, y, size):
    kind, obj = handle
    if kind == "openslide":
        return obj.read_region((x, y), 0, (size, size)).convert("RGB")
    return obj.crop((x, y, x + size, y + size)).convert("RGB")


def tissue_fraction(tile):
    import numpy as np

    arr = np.asarray(tile).astype("float32") / 255.0
    # Bright background has all channels high and low saturation.
    darkness = 1.0 - arr.mean(axis=2)
    return float((darkness > 0.08).mean())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    os.makedirs(args.output, exist_ok=True)

    try:
        slides = mapped_sample_files(cfg("wsi_asset", "wsi"), "wsi",
            extensions=WSI_EXTS)
        tile_size = cfg_int("tile_size", 512)
        stride = cfg_int("stride", tile_size)
        max_tiles = cfg_int("max_tiles", 2048)
        tissue_threshold = cfg_float("tissue_threshold", 0.10)
        write_tiles = cfg_bool("write_tiles", True)
        if (not 16 <= tile_size <= 4096 or not 1 <= stride <= 4096 or
                not 1 <= max_tiles <= 100000 or not 0 <= tissue_threshold <= 1):
            raise RuntimeError("Invalid tiling options")
        outputs = {}
        total_tiles = 0
        for slide_path, sid in slides:
            token = sample_token(sid)
            slide_dir = os.path.join(args.output, token)
            os.mkdir(slide_dir)
            handle = open_slide(slide_path)
            rows = []
            files = []
            try:
                width, height = dimensions(handle)
                for y in range(0, max(1, height - tile_size + 1), stride):
                    for x in range(0, max(1, width - tile_size + 1), stride):
                        if len(rows) >= max_tiles:
                            break
                        tile = read_region(handle, x, y, tile_size)
                        frac = tissue_fraction(tile)
                        if frac < tissue_threshold:
                            continue
                        tile_name = f"tile_{len(rows):06d}.png"
                        tile_path = os.path.join(slide_dir, tile_name)
                        if write_tiles:
                            tile.save(tile_path, format="PNG", optimize=True)
                            files.append(tile_path)
                        rows.append({"x": x, "y": y, "tile_size": tile_size,
                            "tissue_fraction": frac,
                            "tile_file": token + "/" + tile_name if write_tiles else ""})
                    if len(rows) >= max_tiles:
                        break
            finally:
                handle[1].close()
            manifest = os.path.join(slide_dir, "tile_manifest.json")
            write_json(manifest, {"sample_id": sid, "n_tiles": len(rows), "tiles": rows})
            outputs[sid] = {"primary": manifest, "files": [manifest] + files}
            total_tiles += len(rows)
        write_collection_output_manifest(args.output, "wsi_tile_root", outputs)
        write_json(os.path.join(args.output, "wsi_tiling_summary.json"), {
            "n_slides": len(slides), "n_tiles": total_tiles,
            "max_tiles_per_slide": max_tiles, "tile_size": tile_size, "stride": stride,
            "write_tiles": write_tiles, "versions": package_versions(["openslide", "PIL", "numpy"]),
        })
    except Exception:
        print("ERROR: Exact slide association or tiling failed", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
