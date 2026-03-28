# KMA API Data Pipeline

Two independent pipelines for collecting and processing meteorological data from the Korea Meteorological Administration (KMA) API.
Each system operates independently in its own folder, with API keys managed centrally via a single root `.env` file.

## Project Structure

```
kma_api/
├── asos/                   # Surface Observation (ASOS) system
│   ├── run.py              #   Execution script
│   ├── process_data.py     #   Data parsing / pre-processing
│   ├── get_station_info.py #   Station info download
│   └── data/               #   Raw / processed data
│
├── fusion_weather/         # Fusion Weather system
│   ├── run_download.py     #   [Stage A] Raw download
│   ├── run_process.py      #   [Stage B] Spatial aggregation / post-processing
│   ├── fusion/             #   Core pipeline package
│   └── data/               #   Grid coordinates, Shapefiles, static data
│
├── .env                    # Unified API keys + path config
├── .env.example            # .env template
├── requirements.txt        # pip dependencies
├── environment.yml         # conda environment
└── README_EN.md            # This document
```

---

## ASOS — Surface Observation Data

| Item | Description |
|------|-------------|
| **Location** | `asos/` |
| **API** | [KMA API Hub — Surface Observation](https://apihub.kma.go.kr/) |
| **Features** | Download ASOS daily data, fixed-width text → CSV conversion, station info mapping |

```bash
python asos/run.py
```

See [`asos/README_EN.md`](asos/README_EN.md) for details.

---

## Fusion Weather — Gridded Observation Data

| Item | Description |
|------|-------------|
| **Location** | `fusion_weather/` |
| **API** | [KMA API Hub — Fusion Weather](https://apihub.kma.go.kr/) |
| **Features** | Grid weather download → time pivot → spatial aggregation by admin/legal dong → CSV output |
| **Variables** | Temperature (`ta`), Precipitation (`rn_60m`), Snowfall (`sd_3hr`) |
| **Aggregation** | Administrative dong (`hjd`), Legal dong (`bjd`), Unified (`both`) |

```bash
# Stage A: Download raw data
python fusion_weather/run_download.py \
    --start-year 2024 --end-year 2024 \
    --variables ta,rn_60m

# Stage B: Spatial aggregation
python fusion_weather/run_process.py \
    --region-type hjd \
    --start-year 2024 --end-year 2024 \
    --variables ta,rn_60m,sd_3hr
```

See [`fusion_weather/README_EN.md`](fusion_weather/README_EN.md) for details.

---

## Setup

### Conda (Recommended)

```bash
conda env create -f environment.yml
conda activate kma-api
```

### pip

```bash
pip install -r requirements.txt
```

---

## API Keys & Path Configuration

Both systems require an authentication key from the [KMA API Hub](https://apihub.kma.go.kr/).
Keys are managed in the project root `.env` file. (Template: [`.env.example`](.env.example))

```env
# API keys
asos_authKey=YOUR_ASOS_KEY
fusion_weather_authKey=YOUR_FUSION_KEY

# Dynamic data storage path (defaults to project data/ if not set)
# FUSION_DATA_ROOT=E:\kma
```

You may use the same key for both systems or separate keys.
