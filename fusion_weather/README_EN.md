# Fusion Weather System

A data pipeline that downloads gridded weather data from the KMA (Korea Meteorological Administration) Fusion Weather Observation API and spatially aggregates it to **administrative dong (행정동)** level.

## Overview

- **Data Source**: [KMA API Hub](https://apihub.kma.go.kr/) - Fusion Weather tab
- **API Endpoint**: `https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-sfc_obs_nc_api`
- **Spatial Aggregation**: Administrative dong boundaries (2022 Q4 Shapefile)
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
│   ├── geocode.py          # Grid-to-administrative-dong mapping
│   ├── aggregate.py        # Temporal / spatial aggregation
│   └── pipeline.py         # Full pipeline orchestration
├── data/
│   ├── geodata/            # Grid coordinate NetCDF, mapping files
│   ├── geodata_hjd/        # Administrative dong Shapefile (separate download)
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

### 3. Download Administrative Dong Shapefile

Download the administrative dong boundary Shapefile (2022 Q4) and place it in the `data/geodata_hjd/` directory.

- **Download Link**: [Google Drive](https://drive.google.com/file/d/1OHMMUa5lezsSURUztnVS4t1YJYeNKndZ/view?usp=drive_link)

Expected structure after extraction:
```
data/geodata_hjd/
├── bnd_dong_00_2022_4Q/        ← Administrative dong (used)
│   ├── bnd_dong_00_2022_4Q.shp
│   ├── bnd_dong_00_2022_4Q.shx
│   ├── bnd_dong_00_2022_4Q.dbf
│   ├── bnd_dong_00_2022_4Q.cpg
│   └── bnd_dong_00_2022_4Q.prj
├── bnd_sigungu_00_2022_4Q/     (Si/Gun/Gu – not used)
└── bnd_sido_00_2022_4Q/        (Sido – not used)
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
```

### Stage B: Post-Processing (Administrative Dong Aggregation)

```bash
# Process by year/month range
python fusion_weather/run_process.py \
    --start-year 2024 --end-year 2024 \
    --start-month 1 --end-month 12 \
    --variables ta,rn_60m,sd_3hr

# Force rebuild administrative dong mapping
python fusion_weather/run_process.py --force-rebuild-mapping --test-day 20241128
```

## Processing Pipeline

```
[Stage A] API call → Parse → data/fusion_raw/{YYYY}/{MM}/{var}_{date}_parsed.parquet
    ↓
[Stage B] Load cache → Time pivot → Grid→Admin-dong spatial aggregation → Merge variables
    ↓
Output: data/fusion_output/{YYYY}/{MM}/fusion_{date}.csv
```

## Notes

- Snowfall (`sd_3hr`) is available from 2020 onwards and is not produced in summer (June–September).
- Stages A and B can be run independently (run A first, then B later).
- The administrative dong mapping table is generated only once; subsequent runs reuse the cached `grid_to_hjd.parquet`.
