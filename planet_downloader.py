import os
import argparse
import requests
import mercantile
import geopandas as gpd
import numpy as np
import rasterio
from rasterio.merge import merge
from rasterio.transform import from_bounds
from tqdm import tqdm
from PIL import Image
from io import BytesIO
from tempfile import TemporaryDirectory
from concurrent.futures import ThreadPoolExecutor, as_completed

# ------------------------------
MAX_WORKERS = 20
COORD = "{z}/{x}/{y}"

def download_and_save_tile(tile, session, tmpdir, tms_url):
    try:
        url = tms_url.format(z=tile.z, x=tile.x, y=tile.y)
        response = session.get(url, timeout=20)
        response.raise_for_status()
        img = Image.open(BytesIO(response.content)).convert("RGB")

        tile_bounds = mercantile.bounds(tile)
        transform = from_bounds(tile_bounds.west, tile_bounds.south,
                                tile_bounds.east, tile_bounds.north,
                                img.width, img.height)

        dst_path = os.path.join(tmpdir, f"{tile.z}_{tile.x}_{tile.y}.tif")

        with rasterio.open(
            dst_path, "w",
            driver="GTiff",
            height=img.height,
            width=img.width,
            count=3,
            dtype=rasterio.uint8,
            crs="EPSG:4326",
            transform=transform
        ) as dst:
            r, g, b = img.split()
            dst.write(np.array(r), 1)
            dst.write(np.array(g), 2)
            dst.write(np.array(b), 3)

        return dst_path

    except Exception as e:
        print(f"Failed downloading tile {tile.z}/{tile.x}/{tile.y}: {e}")
        return None
    

def merge_in_batches(paths, batch_size=500):
    temp_outputs = []
    with TemporaryDirectory() as tmpmerge:
        for i in tqdm(range(0, len(paths), batch_size), desc="Merging batches"):
            batch = paths[i:i + batch_size]
            datasets = []
            for path in batch:
                try:
                    datasets.append(rasterio.open(path))
                except Exception as e:
                    print(f"Error opening {path}: {e}")
            if not datasets:
                continue
            mosaic, transform = merge(datasets)
            for ds in datasets:
                ds.close()

            tmp_path = os.path.join(tmpmerge, f"batch_{i}.tif")
            with rasterio.open(
                tmp_path, "w",
                driver="GTiff",
                height=mosaic.shape[1],
                width=mosaic.shape[2],
                count=3,
                dtype=mosaic.dtype,
                crs="EPSG:4326",
                transform=transform,
            ) as dst:
                dst.write(mosaic)
            temp_outputs.append(tmp_path)

        # Merge all batch mosaics to final
        print("Merging final batches...")
        final_datasets = [rasterio.open(p) for p in temp_outputs]
        mosaic, transform = merge(final_datasets)
        for ds in final_datasets:
            ds.close()

    return mosaic, transform


def main():
    parser = argparse.ArgumentParser(description="Download PlanetScope tiles and merge to GeoTIFF")
    parser.add_argument("--aoi", type=str, required=True, help="Path to AOI GeoJSON")
    parser.add_argument("--month", type=str, required=True, help="Month of mosaic (YYYY_MM)")
    parser.add_argument("--save-dir", type=str, default="./data", help="Directory to save output GeoTIFF")
    parser.add_argument("--zoom", type=int, default=15, help="Zoom level for tiles")
    parser.add_argument("--output-name", type=str, default=None, help="Output GeoTIFF filename")
    parser.add_argument("--api-key", type=str, required=True, help="Planet API key")
    args = parser.parse_args()

    GEOJSON_PATH = args.aoi
    ZOOM = args.zoom
    FILENAME = os.path.splitext(os.path.basename(GEOJSON_PATH))[0]
    OUTPUT_GEOTIFF = os.path.join(args.save_dir, f"{FILENAME}_{args.month}.tif")
    os.makedirs(args.save_dir, exist_ok=True)

    TMS_URL = f"https://tiles.planet.com/basemaps/v1/planet-tiles/global_monthly_{args.month}_mosaic/gmap/{COORD}.png?api_key={args.api_key}"

    gdf = gpd.read_file(GEOJSON_PATH)
    bounds = gdf.total_bounds
    tiles = list(mercantile.tiles(bounds[0], bounds[1], bounds[2], bounds[3], ZOOM))

    print(f"Total tiles: {len(tiles)}")

    with TemporaryDirectory() as tmpdir, requests.Session() as session:
        paths = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(download_and_save_tile, tile, session, tmpdir, TMS_URL): tile for tile in tiles}

            for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading tiles"):
                result = future.result()
                if result:
                    paths.append(result)

        if not paths:
            print("No images downloaded.")
            return

        print("Merging tiles...")

        try:
            mosaic, out_trans = merge_in_batches(paths)

            with rasterio.open(
                OUTPUT_GEOTIFF, "w",
                driver="GTiff",
                height=mosaic.shape[1],
                width=mosaic.shape[2],
                count=3,
                dtype=mosaic.dtype,
                crs="EPSG:4326",
                transform=out_trans,
            ) as dst:
                dst.write(mosaic)

            print(f"✅ Saved output to {OUTPUT_GEOTIFF}")

        except Exception as e:
            print(f"Error during merging or saving: {e}")


if __name__ == "__main__":
    main()
