#!/usr/bin/env python3
"""Basic WSI/pathology tiling runner."""

import argparse
import csv
import os
import sys

from dsimaging_utils import cfg, cfg_bool, cfg_float, cfg_int, image_files, package_versions, resolve_asset_path, safe_id, write_json


WSI_EXTS = (".svs", ".tif", ".tiff", ".ndpi", ".mrxs", ".png", ".jpg", ".jpeg")


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

    wsi_root = resolve_asset_path(cfg("wsi_asset", "wsi"), "wsi", cfg("wsi_root"))
    tile_size = cfg_int("tile_size", 512)
    stride = cfg_int("stride", tile_size)
    max_tiles = cfg_int("max_tiles", 2048)
    tissue_threshold = cfg_float("tissue_threshold", 0.10)
    write_tiles = cfg_bool("write_tiles", True)
    tiles_dir = os.path.join(args.output, "tiles")
    if write_tiles:
        os.makedirs(tiles_dir, exist_ok=True)

    slides = image_files(wsi_root, extensions=WSI_EXTS)
    if not slides:
        print("ERROR: No WSI/pathology images found", file=sys.stderr)
        sys.exit(1)

    rows = []
    for slide_path in slides:
        slide_id = safe_id(slide_path)
        handle = open_slide(slide_path)
        width, height = dimensions(handle)
        n_for_slide = 0
        for y in range(0, max(1, height - tile_size + 1), stride):
            for x in range(0, max(1, width - tile_size + 1), stride):
                if len(rows) >= max_tiles:
                    break
                tile = read_region(handle, x, y, tile_size)
                frac = tissue_fraction(tile)
                if frac < tissue_threshold:
                    continue
                tile_name = f"{slide_id}_x{x}_y{y}.png"
                tile_path = os.path.join(tiles_dir, tile_name)
                if write_tiles:
                    tile.save(tile_path, format="PNG", optimize=True)
                rows.append({
                    "slide_id": slide_id,
                    "x": x,
                    "y": y,
                    "tile_size": tile_size,
                    "tissue_fraction": frac,
                    "tile_file": os.path.join("tiles", tile_name) if write_tiles else "",
                })
                n_for_slide += 1
            if len(rows) >= max_tiles:
                break
        if n_for_slide == 0:
            tile = read_region(handle, 0, 0, min(tile_size, width, height))
            tile_name = f"{slide_id}_thumbnail.png"
            tile_path = os.path.join(tiles_dir, tile_name)
            if write_tiles:
                tile.save(tile_path, format="PNG", optimize=True)
            rows.append({
                "slide_id": slide_id,
                "x": 0,
                "y": 0,
                "tile_size": min(tile_size, width, height),
                "tissue_fraction": tissue_fraction(tile),
                "tile_file": os.path.join("tiles", tile_name) if write_tiles else "",
            })

    manifest = os.path.join(args.output, "tile_manifest.csv")
    with open(manifest, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "slide_id", "x", "y", "tile_size", "tissue_fraction", "tile_file"])
        writer.writeheader()
        writer.writerows(rows)
    write_json(os.path.join(args.output, "wsi_tiling_summary.json"), {
        "n_slides": len(slides),
        "n_tiles": len(rows),
        "tile_size": tile_size,
        "stride": stride,
        "write_tiles": write_tiles,
        "versions": package_versions(["openslide", "PIL", "numpy"]),
    })


if __name__ == "__main__":
    main()
