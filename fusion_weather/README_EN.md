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
  | Type | Description | Shapefile | Output Columns |
  |------|-------------|-----------|----------------|
  | `hjd` (default) | Administrative dong | `bnd_dong_00_2022_4Q` | `HJD_CD`, `HJD_NM` |
  | `bjd` | Legal dong (eup/myeon/dong) | `LSMD_ADM_SECT_UMD_*` (17 provinces) | `EMD_CD`, `EMD_NM` |
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
│   ├── geodata_hjd/        # Administrative dong Shapefile
│   ├── geodata_umd/        # Legal dong (UMD) Shapefiles
│   ├── fusion_raw/         # [A] Raw cache (parquet)
│   ├── fusion_interim/     # [B] Intermediate results
│   └── fusion_output/      # [B] Final output
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
```

The Fusion Weather system uses `fusion_weather_authKey`.
You can obtain an API key from [KMA API Hub](https://apihub.kma.go.kr/).

### 3. Download Shapefiles

#### Administrative Dong (HJD) — required for `--region-type hjd`

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

#### Legal Dong (BJD/UMD) — required for `--region-type bjd`

Download "Legal Boundary (eup/myeon/dong)" Shapefiles from the National Spatial Information Portal and place them in `data/geodata_umd/`.

```
data/geodata_umd/
├── LSMD_ADM_SECT_UMD_서울/LSMD_ADM_SECT_UMD_11_*.shp
├── LSMD_ADM_SECT_UMD_경기/LSMD_ADM_SECT_UMD_41_*.shp
└── ... (17 provinces)
```

## Usage

### Stage A: Download Raw Data

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

### Stage B: Post-Processing (Spatial Aggregation)

```bash
# Administrative dong (default)
python fusion_weather/run_process.py \
    --start-year 2024 --end-year 2024 \
    --start-month 1 --end-month 12 \
    --variables ta,rn_60m,sd_3hr

# Legal dong
python fusion_weather/run_process.py \
    --region-type bjd \
    --start-year 2024 --end-year 2024 \
    --variables ta,rn_60m

# Both HJD and BJD
python fusion_weather/run_process.py \
    --region-type both \
    --test-day 20241128 \
    --variables ta,rn_60m,sd_3hr

# Force rebuild mapping tables
python fusion_weather/run_process.py --force-rebuild-mapping --test-day 20241128

# Process from custom path (must match Stage A --output-path)
python fusion_weather/run_process.py --output-path E:\kma --start-year 2024 --end-year 2024
```

## Processing Pipeline

```
[Stage A] API call → Parse → data/fusion_raw/{YYYY}/{MM}/{var}_{date}_parsed.parquet
    ↓
[Stage B] Load cache → Time pivot → Grid→Region spatial aggregation → Merge variables
    ↓
Output: data/fusion_output/{YYYY}/fusion_{YYYYMM}[_{region_type}].csv
        data/fusion_output/fusion_weather_{YYYY}[_{region_type}].csv
```

## Notes

- Snowfall (`sd_3hr`) is available from 2020 onwards and is not produced in summer (June-September).
- Stages A and B can be run independently (run A first, then B later).
- Mapping tables are generated only once; subsequent runs reuse cached files (`grid_to_hjd.parquet` / `grid_to_emd_umd.parquet`).
- With `--region-type both`, HJD and BJD outputs are saved as separate files.
