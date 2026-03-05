# KMA Fusion Weather Data Pipeline

[한국어 버전](README_KOR.md)

## Overview

This project downloads grid-based meteorological data from the Korea Meteorological Administration (KMA) API, performs temporal aggregation, and then spatially aggregates from **grid cells to legal dong (Eup/Myeon/Dong) administrative boundaries** to produce daily weather data.

The pipeline is divided into two stages:
- **Stage A (Download/Cache)**: `run_download_fusion.py`
- **Stage B (Post-processing)**: `run_process_fusion.py`

Internally, the pipeline uses `fusion/pipeline.py`'s `FusionPipeline` and `fusion/geocode.py`'s `GridToLawIdMapper`.

---

## Prerequisites

### 1. Environment Setup

Create and activate the conda environment:

```bash
conda env create -f environment.yml
conda activate kma-api
```

### 2. API Authentication

**Required**: Create a `.env` file in the project root with your KMA API Hub authentication key:

```bash
authKey=your_api_key_here
```

Get your API key from [KMA API Hub](https://apihub.kma.go.kr/).

### 3. Required External Data Files

You need to download and place the following data files in the correct directories:

#### Legal Dong (Eup/Myeon/Dong) Boundary Shapefiles

**Location**: `data/geodata_umd/`

**What to download**: 17 province-level legal dong shapefiles from the Korean government's geospatial data portal.

**Source**: [Korean National Spatial Data Infrastructure Portal](http://data.nsdi.go.kr/)
- Search for "법정경계(읍면동)" or "LSMD_ADM_SECT_UMD"
- Download all 17 province shapefiles (one for each province/metropolitan city)

**Expected structure**:
```
data/geodata_umd/
├── LSMD_ADM_SECT_UMD_11/  # Seoul
│   ├── LSMD_ADM_SECT_UMD_11_202602.shp
│   ├── LSMD_ADM_SECT_UMD_11_202602.shx
│   ├── LSMD_ADM_SECT_UMD_11_202602.dbf
│   └── LSMD_ADM_SECT_UMD_11_202602.prj
├── LSMD_ADM_SECT_UMD_26/  # Busan
│   └── ...
├── LSMD_ADM_SECT_UMD_41/  # Gyeonggi
│   └── ...
...
└── LSMD_ADM_SECT_UMD_50/  # Jeju
    └── ...
```

**Required columns**: `EMD_CD` (legal dong code), `EMD_NM` (legal dong name), `geometry`

#### Grid Coordinate NetCDF File

**Location**: `data/geodata/sfc_grid_latlon.nc`

**What**: NetCDF file containing latitude/longitude coordinates for each grid cell in the KMA fusion weather data system.

**Source**: Contact KMA API Hub support or check the API documentation for grid coordinate data.

**Expected structure**:
- Dimensions: `(ny: 2049, nx: 2049)`
- Variables: `lat(ny, nx)`, `lon(ny, nx)`
- CRS information: Lambert Conformal Conic projection attributes

**Note**: This file defines the mapping between grid indices and geographic coordinates. If you update this file, you must regenerate the grid-to-legal-dong mapping.

---

## Grid-to-Legal-Dong Mapping

### Automatic Generation

The post-processing stage (B) requires a grid-to-legal-dong mapping file: `data/geodata/grid_to_emd_umd.parquet`

- If the file exists, it will be loaded automatically
- If the file doesn't exist, it will be generated automatically on first run (10-30 minutes, one-time operation)
- The mapping integrates all 17 province shapefiles and performs point-in-polygon spatial join

### Manual Generation

Generate mapping (creates if missing, loads if exists):
```bash
python -c "import os; from fusion.config import FusionConfig; from fusion.geocode import GridToLawIdMapper; cfg=FusionConfig(project_root=os.getcwd()); GridToLawIdMapper(cfg).build_mapping(force_rebuild=False)"
```

Force regenerate mapping:
```bash
python -c "import os; from fusion.config import FusionConfig; from fusion.geocode import GridToLawIdMapper; cfg=FusionConfig(project_root=os.getcwd()); GridToLawIdMapper(cfg).build_mapping(force_rebuild=True)"
```

Or use the `--force-rebuild-mapping` flag when running `run_process_fusion.py`.

---

## Usage

### Recommended Workflow: A (Download) → B (Post-processing)

Large-scale grid API calls are time-consuming and require careful retry/logging management. Therefore, we recommend separating the **download stage (A)** and **cache-based post-processing stage (B)**.

### Stage A: Raw Data Download/Caching

**Script**: `run_download_fusion.py`

**Purpose**: Populate `data/fusion_raw/YYYY/MM/{var}_{date}_parsed.parquet` cache files

**Features**:
- Date-level parallel processing (default `--max-workers 4`)
- Automatic retry with exponential backoff
- Validation logging to `data/fusion_raw/_validation_logs/`

**Examples**:

Download specific date range:
```bash
python run_download_fusion.py \
  --start-year 2024 \
  --end-year 2024 \
  --start-month 6 \
  --end-month 7 \
  --variables ta,rn_60m,sd_3hr \
  --max-workers 4
```

Test with single day:
```bash
python run_download_fusion.py \
  --test-day 20241128 \
  --variables ta,rn_60m
```

**Parameters**:
- `--start-year`, `--end-year`: Year range (inclusive)
- `--start-month`, `--end-month`: Month range (inclusive)
- `--variables`: Comma-separated variable list (ta, rn_60m, sd_3hr)
- `--test-day`: Single day test mode (YYYYMMDD format)
- `--max-workers`: Number of parallel workers for date-level processing

### Stage B: Cache-Based Post-Processing

**Script**: `run_process_fusion.py`

**Purpose**: Use cached `*_parsed.parquet` files to generate `data/fusion_interim` and `data/fusion_output` results

**Policy**: Skips dates/variables with missing cache files (B policy) and outputs summary

**Examples**:

Process date range:
```bash
python run_process_fusion.py \
  --start-year 2024 \
  --end-year 2024 \
  --start-month 6 \
  --end-month 7 \
  --variables ta,rn_60m,sd_3hr
```

Test with single day:
```bash
python run_process_fusion.py \
  --test-day 20241128 \
  --variables ta,rn_60m
```

Force rebuild mapping:
```bash
python run_process_fusion.py \
  --test-day 20241128 \
  --variables ta,rn_60m \
  --force-rebuild-mapping
```

**Parameters**:
- `--start-year`, `--end-year`: Year range (inclusive)
- `--start-month`, `--end-month`: Month range (inclusive)
- `--variables`: Comma-separated variable list
- `--test-day`: Single day test mode (YYYYMMDD format)
- `--force-rebuild-mapping`: Force regenerate grid-to-legal-dong mapping

---

## Project Structure

```
kma_api/
├── data/
│   ├── geodata/                      # Geospatial reference data
│   │   ├── sfc_grid_latlon.nc        # Grid coordinates (REQUIRED)
│   │   └── grid_to_emd_umd.parquet   # Grid-to-legal-dong mapping (auto-generated)
│   ├── geodata_umd/                  # Legal dong boundaries (REQUIRED)
│   │   ├── LSMD_ADM_SECT_UMD_11/     # Seoul
│   │   ├── LSMD_ADM_SECT_UMD_26/     # Busan
│   │   ├── LSMD_ADM_SECT_UMD_41/     # Gyeonggi
│   │   └── ...                       # Other 14 provinces
│   ├── fusion_raw/                   # Raw API cache (Stage A output)
│   │   ├── YYYY/MM/                  # Organized by year/month
│   │   │   ├── ta_YYYYMMDD_parsed.parquet
│   │   │   ├── rn_60m_YYYYMMDD_parsed.parquet
│   │   │   └── sd_3hr_YYYYMMDD_parsed.parquet
│   │   └── _validation_logs/         # Validation/error logs
│   ├── fusion_interim/               # Intermediate results (Stage B output)
│   │   └── YYYY/
│   │       └── fusion_YYYYMMDD.parquet
│   └── fusion_output/                # Final CSV outputs (Stage B output)
│       ├── YYYY/
│       │   └── fusion_YYYYMM.csv     # Monthly data
│       └── fusion_weather_YYYY.csv   # Yearly data
├── fusion/                           # Core modules
│   ├── config.py                     # Configuration
│   ├── geocode.py                    # Grid-to-legal-dong mapping
│   ├── download.py                   # API downloader
│   ├── aggregate.py                  # Temporal & spatial aggregation
│   └── pipeline.py                   # Main pipeline orchestration
├── run_download_fusion.py            # Stage A: Download script
├── run_process_fusion.py             # Stage B: Post-processing script
├── environment.yml                   # Conda environment specification
├── .env                              # API key (create this, not in git)
└── README.md                         # This file
```

---

## Configuration

All settings are in `fusion/config.py` (`FusionConfig` class):

### Key Paths

- **Project root**: `FusionConfig.project_root`
- **Data root**: `data/`
- **Geodata**: `data/geodata/`, `data/geodata_umd/`
- **Raw cache**: `data/fusion_raw/` (API cache)
- **Interim**: `data/fusion_interim/` (intermediate results)
- **Output**: `data/fusion_output/` (final CSV files)

### Core Files

- **Legal dong boundaries**: `data/geodata_umd/LSMD_ADM_SECT_UMD_*/*.shp` (17 provinces)
- **Grid coordinates**: `data/geodata/sfc_grid_latlon.nc`
- **Grid mapping**: `data/geodata/grid_to_emd_umd.parquet`

---

## Data Variables

### Available Variables

| Variable | Description | Unit | Output Resolution | Start Year | Temporal Aggregation (Raw → Hourly) | Spatial Aggregation (Grid → Dong) |
|----------|-------------|------|-------------------|------------|-------------------------------------|-----------------------------------|
| `ta` | Temperature | ℃ | 1-hour | 1997 | Mean of 5-min observations | Mean of all grid cells in dong |
| `rn_60m` | 60-min Precipitation | mm | 1-hour | 1997 | Already 60-min cumulative | Mean of all grid cells in dong |
| `sd_3hr` | 3-hour New Snowfall | cm | 3-hour | 2020 | Already 3-hour cumulative (Oct-May only) | Mean of all grid cells in dong |

**Spatial Aggregation Details:**

When multiple grid cells fall within a single legal dong boundary, the pipeline aggregates them as follows:

1. **Temperature (`ta`)**:
   - Method: `mean()` - Average of all grid cell values in the legal dong
   - NaN handling: **Preserved** (excluded from mean calculation)
   - Rationale: Missing temperature data doesn't mean "zero temperature". Excluding NaN values prevents bias in the average calculation. If a grid cell has missing data, it's better to calculate the mean only from valid measurements.
   - Example: If legal dong "청운효자동" contains 5 grid cells with temperatures [15.2, 15.5, NaN, 15.3, 15.4]℃, the result is (15.2+15.5+15.3+15.4)/4 = 15.35℃
   - Note: The NaN is excluded, so we divide by 4 (valid values) not 5 (total cells)

2. **Precipitation (`rn_60m`)**:
   - Method: `mean()` - Average of all grid cell values in the legal dong
   - NaN handling: **Converted to 0** (assumption: no data = no precipitation)
   - Rationale: For precipitation events, missing data typically means no precipitation occurred at that location. Converting NaN to 0 is meteorologically sound - if there was significant rainfall, it would have been measured/estimated. This prevents overestimating precipitation by excluding dry areas.
   - Example: If a legal dong contains 5 grid cells with precipitation [0.5, 1.2, NaN, 0.0, 0.3]mm, the result is (0.5+1.2+0+0.0+0.3)/5 = 0.4mm
   - Note: The NaN is treated as 0, and we divide by 5 (all cells) to get representative area-average precipitation

3. **Snowfall (`sd_3hr`)**:
   - Method: `mean()` - Average of all grid cell values in the legal dong
   - NaN handling: **Converted to 0** (assumption: no data = no snowfall)
   - Rationale: Same logic as precipitation - missing snowfall data indicates no snow event at that location
   - Seasonal: Only produced October-May (automatically skipped for Jun-Sep)

**Key Implementation Details:**
- Grid cells not mapped to any legal dong (ocean, North Korea, boundaries) are excluded before aggregation
- Each legal dong's final value = arithmetic mean of all grid cells within its polygon boundary
- Missing data (`NaN`) handling differs by variable type to avoid bias in aggregation

### Output Column Format

**Temperature**: `t0001`, `t0102`, ..., `t2324` (24 columns, hourly)
**Precipitation**: `p0001`, `p0102`, ..., `p2324` (24 columns, hourly)
**Snowfall**: `s0003`, `s0306`, ..., `s2124` (8 columns, 3-hourly)

Column naming: `{prefix}{start_hour:02d}{end_hour:02d}`

---

## Geospatial Mapping Logic

### Grid Structure

- **Source**: `data/geodata/sfc_grid_latlon.nc`
- **Format**: `(ny, nx)` 2D grid (currently 2049×2049)
- Each grid cell has center coordinates `(lat, lon)` used for spatial join

### Legal Dong Boundaries

- **Source**: `data/geodata_umd/LSMD_ADM_SECT_UMD_*/*.shp` (17 provinces)
- **CRS**: Original CRS preserved, converted to `EPSG:4326` for spatial join if needed

### Mapping Method: Point-in-Polygon (`within`)

**Implementation**: `fusion/geocode.py` → `GridToLawIdMapper.build_mapping()`

**Process**:
1. Load all grid cell coordinates from NetCDF → `grid_df(grid_idx, lat, lon)`
2. Load and merge 17 province shapefiles
3. Convert `grid_df` to `GeoDataFrame`: `geometry = Point(lon, lat)`, CRS=`EPSG:4326`
4. Convert legal dong `GeoDataFrame` to `EPSG:4326` if needed
5. Perform spatial join: `geopandas.sjoin(grid_points, dong_gdf, how='left', predicate='within')`
   - **Meaning**: "Find which legal dong polygon completely contains this grid point"

### Unmapped Grid Cells

Grid cells with `NaN` in `EMD_CD` are "unmapped" and typically represent:
- **Ocean/sea areas**: Legal dong polygons cover land only
- **North Korea/outside boundaries**: Areas not covered by shapefiles
- **Boundary edge points**: `predicate='within'` may not include points exactly on polygon boundaries
- **(Rare) Coordinate/alignment issues**: Data quality issues

**Storage**: Unmapped cells are retained in `grid_to_emd_umd.parquet` with `EMD_CD`/`EMD_NM` = `NaN`

**Processing**: During spatial aggregation (`fusion/aggregate.py`), rows with `EMD_CD = NaN` are removed before aggregation, so they don't contribute to final legal dong results.

### Missing Data Handling by Variable Type

During spatial aggregation, different variables handle missing data differently:
- **Precipitation/Snowfall** (`p*`, `s*` columns): `NaN` → `0` (assumption: no observation = no phenomenon)
- **Temperature** (`t*` columns): `NaN` preserved (excluded from mean calculation to avoid bias)

---

## Pipeline Workflow

### Stage A: Download (Parallel)

```
run_download_fusion.py
    ↓
For each date (parallel workers):
    For each hour:
        API call → validate → retry if needed
        Parse grid response → strict validation
        Save to: fusion_raw/YYYY/MM/{var}_{date}_parsed.parquet
    Log failures to: fusion_raw/_validation_logs/YYYY/MM/{date}_{var}.txt
```

### Stage B: Post-Processing (Sequential by date)

```
run_process_fusion.py
    ↓
Load/build grid-to-legal-dong mapping
    ↓
For each date:
    Load cached parquet files (skip if missing)
    Temporal aggregation (already 1-hour/3-hour in cache)
    Pivot: time → columns (t0001, t0102, ...)
    Spatial aggregation: grid → legal dong (mean of grid cells in each dong)
    Remove unmapped grid cells (EMD_CD = NaN)
    Merge variables (temperature, precipitation, snowfall)
    Save:
        - Interim: fusion_interim/YYYY/fusion_YYYYMMDD.parquet
        - Monthly: fusion_output/YYYY/fusion_YYYYMM.csv
        - Yearly: fusion_output/fusion_weather_YYYY.csv
```

---

## Operational Notes

### When to Regenerate Grid Mapping

**Regenerate `grid_to_emd_umd.parquet` if**:
- `sfc_grid_latlon.nc` is updated/replaced (grid structure/dimensions changed)
- Legal dong shapefiles are updated (boundary changes)
- High unmapped grid cell ratio (check data quality)

**Command**:
```bash
python run_process_fusion.py --force-rebuild-mapping --test-day 20241128 --variables ta
```

### Validation Logs

All download/parsing failures are logged to:
```
data/fusion_raw/_validation_logs/YYYY/MM/{date}_{var}.txt
```

Each log entry includes:
- Timestamp
- Severity level (INFO, WARN, ERROR)
- Time code (tm)
- Error message
- Response preview (for debugging)

### Missing Cache Files (B Stage)

When running Stage B, if cache files are missing:
- Date/variable is skipped (not an error)
- Summary printed at end showing all skipped items
- Re-run Stage A for missing dates to fill cache

---

## Troubleshooting

### Problem: "ModuleNotFoundError: No module named 'geopandas'"

**Solution**: Create conda environment:
```bash
conda env create -f environment.yml
conda activate kma-api
```

### Problem: "grid_to_emd_umd.parquet not found"

**Solution**: File will auto-generate on first B stage run (10-30 min). Or manually generate:
```bash
python run_process_fusion.py --force-rebuild-mapping --test-day 20241128 --variables ta
```

### Problem: "HTTP 403 Forbidden" during download

**Solution**:
1. Check `.env` file has correct `authKey`
2. Verify API permissions at [KMA API Hub My Page](https://apihub.kma.go.kr/mypage)
3. Ensure the specific variable (`ta`, `rn_60m`, `sd_3hr`) is enabled for your API key

### Problem: High number of unmapped grid cells

**Solution**:
1. Verify legal dong shapefiles are complete (all 17 provinces)
2. Check CRS compatibility in shapefiles (should auto-convert to EPSG:4326)
3. Review grid coordinate file (`sfc_grid_latlon.nc`) coverage
4. Consider tolerance for ocean/boundary cells (this is expected)

### Problem: Missing data in summer months for snowfall

**Expected behavior**: Snowfall (`sd_3hr`) is only produced October-May. Pipeline automatically skips `sd_3hr` for months 6-9.

### Problem: Download failures/retries

**Check**:
1. Validation logs: `data/fusion_raw/_validation_logs/`
2. Network connectivity
3. API rate limits (default: 0.5s between calls)
4. Retry settings in `fusion/config.py`:
   - `download_retry_attempts` (default: 3)
   - `download_retry_initial_sleep_seconds` (default: 10.0)
   - `download_retry_backoff` (default: 2.0)

---

## Technical Details

### Grid Index (`grid_idx`)

- **Definition**: Sequential index (0 to N-1) assigned after flattening `(ny, nx)` 2D grid coordinates
- **Stability**: Stable as long as `sfc_grid_latlon.nc` dimensions/structure unchanged
- **Change impact**: If NetCDF changes (resolution/ordering), grid_idx meaning changes → regenerate mapping

### Legal Dong Codes

- **Format**: `EMD_CD` (e.g., "1111010100" - 10-digit code)
- **Hierarchy**: Province(2) + City(3) + Legal dong(5)
- **Name**: `EMD_NM` (Korean name, e.g., "청운효자동")

### Column Name Mapping (Internal)

- **Source data**: Uses `EMD_CD`, `EMD_NM`
- **Internal pipeline**: Renames to `LAW_ID`, `LAW_NM` for compatibility
- **Output files**: Converted back to `EMD_CD`, `EMD_NM`

### CRS Handling

- **Grid coordinates**: Assumed `EPSG:4326` (WGS84)
- **Shapefiles**: Original CRS preserved, auto-converted to `EPSG:4326` for spatial join
- **Spatial join**: All operations in `EPSG:4326`

---

## Data Sources & References

### KMA API Hub
- **Website**: https://apihub.kma.go.kr/
- **Documentation**: https://apihub.kma.go.kr/api/guide
- **Variables**: Fusion weather data (격자형 융합기상정보)

### Legal Dong Boundaries
- **Source**: National Spatial Data Infrastructure Portal (국가공간정보포털)
- **Website**: http://data.nsdi.go.kr/
- **Dataset**: 법정경계(읍면동) / LSMD_ADM_SECT_UMD
- **Update**: Check for quarterly/annual updates

### Grid Coordinates
- **Source**: KMA API Hub documentation or support
- **File**: `sfc_grid_latlon.nc`
- **Projection**: Lambert Conformal Conic (LCC)

---

## Contributing

This is a data processing pipeline project. For contributions:
1. Maintain A/B stage separation
2. Preserve validation/logging for production reliability
3. Update mapping generation if adding new geospatial sources
4. Document any configuration changes in `fusion/config.py`

---

## License

Project license and terms TBD.

---

## Contact

For KMA API access issues: https://apihub.kma.go.kr/support

For pipeline issues: Check validation logs in `data/fusion_raw/_validation_logs/`
