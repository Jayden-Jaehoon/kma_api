# Fusion Weather System

A data pipeline that downloads gridded weather data from the KMA (Korea Meteorological Administration) Fusion Weather Observation API and spatially aggregates it to **administrative dong (HJD)** and/or **legal dong (BJD)** level.

## Overview

- **Data Source**: [KMA API Hub](https://apihub.kma.go.kr/) - Fusion Weather tab
- **API Endpoint**:
  | Type | Domain | Purpose |
  |------|--------|---------|
  | `org` (default) | `apihub-org.kma.go.kr` | Institutional / bulk downloads |
  | `public` | `apihub.kma.go.kr` | Personal API key |
- **Spatial Aggregation**:
  | Type | Description | Shapefile | Mapping Cache | Output Columns |
  |------|-------------|-----------|---------------|----------------|
  | `hjd` | Administrative dong | `bnd_dong_00_2022_4Q` | `grid_to_hjd.parquet` | `HJD_CD`, `HJD_NM` |
  | `bjd` | Legal dong (eup/myeon/dong) | `LSMD_ADM_SECT_UMD_*` (17 provinces) | `grid_to_emd_umd.parquet` | `EMD_CD`, `EMD_NM` |
  | `both` | Unified (HJD + BJD) | Both of the above | `grid_to_unified.parquet` | `HJD_CD`, `EMD_CD` |
- **Supported Variables**:
  | Key | Description | Unit | Interval |
  |-----|-------------|------|----------|
  | `ta` | Temperature | ℃ | 1-hour |
  | `rn_60m` | 60-min Cumulative Precipitation | mm | 1-hour |
  | `sd_3hr` | 3-hour New Snowfall | cm | 3-hour |

## Directory Structure

```
fusion_weather/
├── ../.env                 # Unified API key (fusion_weather_authKey) [root]
├── run_download.py         # [Stage A] Raw download / cache creation
├── run_process.py          # [Stage B] Post-processing (pivot/spatial aggregation/output)
├── fusion/                 # Core package
│   ├── __init__.py
│   ├── config.py           # Configuration (paths, API, variable definitions)
│   ├── download.py         # API download & parsing
│   ├── geocode.py          # Grid-to-region mapping (HJD + BJD)
│   ├── aggregate.py        # Temporal / spatial aggregation
│   └── pipeline.py         # Full pipeline orchestration
├── data/
│   ├── geodata/            # Grid coordinate NetCDF, mapping cache files
│   │   ├── sfc_grid_latlon.nc      # Grid lat/lon coordinates (2049x2049, ~4.2M grid points)
│   │   ├── grid_to_hjd.parquet     # [Auto-generated] HJD mapping cache
│   │   ├── grid_to_emd_umd.parquet # [Auto-generated] BJD mapping cache
│   │   └── grid_to_unified.parquet # [Auto-generated] Unified HJD+BJD mapping cache
│   ├── geodata_hjd/        # Administrative dong Shapefile
│   ├── geodata_umd/        # Legal dong (UMD) Shapefiles (17 provinces)
│   ├── fusion_raw/         # [A] Raw cache (parquet)
│   ├── fusion_interim/     # [B] Intermediate results (parquet)
│   └── fusion_output/      # [B] Final output (csv)
└── README_EN.md            # This document
```

## Setup

### 1. Create Conda Environment

```bash
cd fusion_weather
conda env create -f environment.yml
conda activate kma_api
```

Or with pip:

```bash
pip install -r requirements.txt
```

### 2. Configure API Key

API keys are managed centrally in the **project root** `.env` file:

```env
# project_root/.env
asos_authKey=YOUR_ASOS_KEY
fusion_weather_authKey=YOUR_FUSION_KEY

# Dynamic data storage path (raw/interim/output)
# If not set, defaults to project_root/data
FUSION_DATA_ROOT=E:\kma
```

- `fusion_weather_authKey`: KMA API key (obtain from [API Hub](https://apihub.kma.go.kr/))
- `FUSION_DATA_ROOT`: Storage path for downloaded/intermediate/output files. Set once to avoid specifying `--output-path` every time.

**Path priority:** `--output-path` CLI arg > `FUSION_DATA_ROOT` in `.env` > default (`project_root/data`)

### 3. Download Grid Coordinate File

Download the "High-resolution grid lat/lon" NetCDF file from the **Fusion Weather** tab on the [KMA API Hub](https://apihub.kma.go.kr/) and place it in `data/geodata/`.
This file is used to obtain grid point coordinates when building mapping tables.

```
data/geodata/
└── sfc_grid_latlon.nc    # Grid lat/lon coordinates (2049x2049, ~4.2M grid points)
```

### 4. Download Shapefiles

Only the shapefiles for your chosen `--region-type` are required.

#### Administrative Dong (HJD) -- required for `--region-type hjd` or `both`

Download the administrative dong boundary Shapefile (2022 Q4) and place it in `data/geodata_hjd/`.

- **Download**: [Google Drive](https://drive.google.com/file/d/1OHMMUa5lezsSURUztnVS4t1YJYeNKndZ/view?usp=drive_link)

```
data/geodata_hjd/
└── bnd_dong_00_2022_4Q/
    ├── bnd_dong_00_2022_4Q.shp
    ├── bnd_dong_00_2022_4Q.shx
    ├── bnd_dong_00_2022_4Q.dbf
    ├── bnd_dong_00_2022_4Q.cpg
    └── bnd_dong_00_2022_4Q.prj
```

#### Legal Dong (BJD/UMD) -- required for `--region-type bjd` or `both`

Download "Legal Boundary (eup/myeon/dong)" Shapefiles from the National Spatial Information Portal and place them in `data/geodata_umd/`.

```
data/geodata_umd/
├── LSMD_ADM_SECT_UMD_서울/LSMD_ADM_SECT_UMD_11_*.shp
├── LSMD_ADM_SECT_UMD_경기/LSMD_ADM_SECT_UMD_41_*.shp
└── ... (17 provinces)
```

## Usage

### Stage A: Download Raw Data

Downloads grid data from the API and saves it as parquet cache under `data/fusion_raw/`.
This stage does not perform spatial aggregation, so no Shapefile or `--region-type` is needed.

```bash
# Download by year/month range
python fusion_weather/run_download.py \
    --start-year 2024 --end-year 2024 \
    --start-month 1 --end-month 12 \
    --variables ta,rn_60m \
    --max-workers 4

# Single day test
python fusion_weather/run_download.py --test-day 20241128 --variables ta,rn_60m,sd_3hr

# Save to custom path
python fusion_weather/run_download.py --output-path E:\kma --start-year 2024 --end-year 2024

# Use public (personal) API key endpoint
python fusion_weather/run_download.py --api-type public --test-day 20241128 --variables ta
```

**Stage A output structure:**
```
data/fusion_raw/
└── 2024/
    └── 01/
        ├── ta_20240101_parsed.parquet       # Temperature (grid_idx, date, hour, value)
        ├── rn_60m_20240101_parsed.parquet   # Precipitation
        └── sd_3hr_20240101_parsed.parquet   # Snowfall
```

### Stage B: Post-Processing (Spatial Aggregation)

Reads Stage A raw cache, performs time pivot, spatial aggregation, and outputs CSV.
**This stage does not download data** and can run without an API key.

```bash
# Administrative dong
python fusion_weather/run_process.py \
    --region-type hjd \
    --start-year 2024 --end-year 2024 \
    --start-month 1 --end-month 12 \
    --variables ta,rn_60m,sd_3hr

# Legal dong
python fusion_weather/run_process.py \
    --region-type bjd \
    --start-year 2024 --end-year 2024 \
    --variables ta,rn_60m

# Unified (both HJD and BJD columns in a single file)
python fusion_weather/run_process.py \
    --region-type both \
    --test-day 20241128 \
    --variables ta,rn_60m,sd_3hr

# Force rebuild mapping tables
python fusion_weather/run_process.py --force-rebuild-mapping --test-day 20241128

# Process from custom path (must match Stage A --output-path)
python fusion_weather/run_process.py --output-path E:\kma --start-year 2024 --end-year 2024
```

## Processing Pipeline (Detail)

### Stage A: Download -> Raw Cache

```
API call (per variable, per hour)
  -> Parse ASCII response (extract grid values, handle missing data)
  -> Validate (check expected grid count, retry on failure)
  -> Save to data/fusion_raw/{YYYY}/{MM}/{var}_{date}_parsed.parquet
```

- Each parquet contains one day of one variable's grid data.
- Columns: `grid_idx` (grid number 0~4.2M), `date`, `hour`, `value`
- Up to 3 retries with exponential backoff on failure; logs saved to `fusion_raw/_validation_logs/`

### Stage B: Raw Cache -> Spatial Aggregation -> CSV Output

```
1. Build/load mapping table
   ├── --region-type hjd  -> grid_to_hjd.parquet     (grid -> admin dong)
   ├── --region-type bjd  -> grid_to_emd_umd.parquet  (grid -> legal dong)
   └── --region-type both -> grid_to_unified.parquet   (grid -> both HJD + BJD)

2. Per-variable processing (ta, rn_60m, sd_3hr each)
   ├── Load raw parquet (grid_idx, date, hour, value)
   ├── Time pivot: rows=grid, columns=time slots
   │     Temperature: t0001, t0102, ..., t2324 (24 cols)
   │     Precipitation: p0001, p0102, ..., p2324 (24 cols)
   │     Snowfall: s0003, s0306, ..., s2124 (8 cols)
   └── Spatial aggregation: join grid->region mapping, then mean per region

3. Merge variables -> CSV output
```

### Mapping Table Generation

The mapping table is a lookup that assigns each grid point (~4.2M) to its containing administrative region.
HJD and BJD mappings are **generated as separate files**; the unified mapping joins them on `grid_idx`.

```
sfc_grid_latlon.nc (grid lat/lon)
  -> Convert grid points to Point geometry (EPSG:4326)
  -> Spatial join with Shapefile polygons (Point-in-Polygon, geopandas.sjoin)
  -> Save mapping result

  HJD: geodata_hjd/bnd_dong_00_2022_4Q.shp -> grid_to_hjd.parquet
       Columns: grid_idx, lat, lon, HJD_CD, HJD_NM

  BJD: geodata_umd/LSMD_ADM_SECT_UMD_*.shp (17 provinces merged) -> grid_to_emd_umd.parquet
       Columns: grid_idx, lat, lon, EMD_CD, EMD_NM

  Unified: merge HJD + BJD on grid_idx -> grid_to_unified.parquet
       Columns: grid_idx, lat, lon, HJD_CD, HJD_NM, EMD_CD, EMD_NM
```

- Auto-generated on first run; subsequent runs reuse cached parquet.
- Use `--force-rebuild-mapping` to regenerate.
- Ocean/border grid points with no match are `NaN` and excluded from aggregation.

### Output File Structure

All output files include a suffix based on `--region-type` (`_hjd`, `_bjd`, `_both`).

```
data/fusion_output/
├── 2024/
│   ├── fusion_202401_hjd.csv       # Administrative dong
│   ├── fusion_202401_bjd.csv       # Legal dong
│   ├── fusion_202401_both.csv      # Unified (HJD + BJD)
│   └── ...
├── fusion_weather_2024_hjd.csv     # Yearly (admin dong)
├── fusion_weather_2024_bjd.csv     # Yearly (legal dong)
└── fusion_weather_2024_both.csv    # Yearly (unified)
```

**HJD (`_hjd`) CSV columns:**
```
date     | HJD_CD     | t0001 | t0102 | ... | t2324 | p0001 | ... | s0003 | ...
20240101 | 1168064000 | -2.3  | -2.5  | ... | -1.8  | 0.0   | ... | 0.5   | ...
```

**BJD (`_bjd`) CSV columns:**
```
date     | EMD_CD     | t0001 | t0102 | ... | t2324 | p0001 | ... | s0003 | ...
20240101 | 1168010100 | -2.1  | -2.4  | ... | -1.7  | 0.0   | ... | 0.3   | ...
```

**Unified (`_both`) CSV columns -- both HJD_CD and EMD_CD in a single file:**
```
date     | HJD_CD     | EMD_CD     | t0001 | t0102 | ... | p0001 | ... | s0003 | ...
20240101 | 1168064000 | 1168010100 | -2.2  | -2.5  | ... | 0.0   | ... | 0.4   | ...
```

The unified mode aggregates by `(HJD_CD, EMD_CD)` pairs.
Where one administrative dong contains multiple legal dongs (or vice versa), each combination produces a separate row.

## Notes

- Snowfall (`sd_3hr`) is available from 2020 onwards and is not produced in summer (June-September).
- Stages A and B can be run independently (run A first, then B later).
- The same raw cache can be processed with different `--region-type` options without re-downloading.
- Administrative dong (HJD) and legal dong (BJD) are different administrative boundary systems, so the same grid point may map to different HJD_CD and EMD_CD values.
