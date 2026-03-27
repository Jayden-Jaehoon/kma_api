# ASOS (Automated Surface Observing System) KMA API Pipeline

A simple pipeline to download annual daily meteorological data from the Korea Meteorological Administration (KMA) API (ASOS), pre-process it, and save it as CSV files. It also includes tools for downloading station information.

- Execution Entry Point: `run.py`
- Data Pre-processing: `process_data.py`
- Station Info Download: `get_station_info.py`
- Data/Meta Files: `data/`

---

## 1) Installation and Environment

Due to binary compatibility issues with NumPy 2.x (especially for C-extension modules like pandas, matplotlib), this project uses fixed dependency versions for stable operation.

Prepare the environment using one of the following methods:

### Method A) Using Conda (Recommended)

```bash
conda env create -f environment.yml
conda activate kma-api
```

### Method B) Using Python venv + pip

```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -U pip
pip install -r ../requirements.txt
```

---

## 2) .env Configuration (Important)

An authentication key is required for KMA API calls. Create a `.env` file within the `asos` folder and enter it as follows. (Operates independently from the root `.env`.)

```env
authKey=YOUR_API_KEY_HERE
```

- The key name must be exactly `authKey`. It is read via `os.getenv("authKey")` in the code.

---

## 3) Project Structure

```
asos/
├── run.py                        # Execution entry point (Download + pre-process annual range)
├── process_data.py               # Download/Parsing/Pre-processing logic
├── get_station_info.py           # Station information download script
├── environment.yml               # Conda environment definition
├── .env                          # Individual API key configuration
└── data/
    ├── raw_data/                 # Original text storage (Auto-generated)
    ├── post_process_data/        # Pre-processed CSV storage (Auto-generated)
    ├── station_info_structured.csv  # Station metadata (For STN_ID to LAW_ID mapping)
    ├── station_info_SFC.md       # Station info documentation (Reference)
    └── weather_data_stn.md       # Original field descriptions (Reference)
```

---

## 4) Quick Start (run.py)

Once the `.env` in the `asos` folder is ready, you can run the example with a single command:

```bash
cd asos
python run.py
```

Upon execution:
- Original text files are saved in `asos/data/raw_data/`.
- Pre-processed CSV files are generated in `asos/data/post_process_data/`.

To change execution parameters, modify `start_year`, `end_year`, `stn`, and `BASE_DATA_DIR` inside `run.py`.

---

## 5) Detailed Operation (process_data.py)

Summary of core functions:

- `download_year_txt(auth_key, base_data_dir, year, stn="0")`
  - Downloads original text (including help) for a specific year and saves it to `data/raw_data`.

- `process_raw_txt_to_csv(input_txt_path, output_csv_path)`
  - Parses text using fixed-width (FWF) format after removing comments/help sections and saves as CSV.
  - Special Handling:
    - Removes `#7777END, ...` lines often found at the end of files.
    - Adds `LAW_ID` column by mapping `STN_ID` to `LAW_ID` using `data/station_info_structured.csv`.

- `run_year_range(auth_key, base_data_dir, start_year, end_year, stn="0")`
  - Iterates through the specified year range for download and pre-processing.

Output CSV encoding is `utf-8-sig` (for Excel compatibility).

---

## 6) Station Info Download (get_station_info.py)

You can download station metadata in its original format. Default is surface stations (ASOS, `SFC`).

```bash
cd asos
python get_station_info.py
```

---

## 7) FAQ / Troubleshooting

- **ImportError or NumPy/Pandas issues?**
  - Use the provided `requirements.txt` or `environment.yml` to ensure compatible versions.
  - Manually: `pip install "numpy<2" "pandas==1.5.3"`.

- **Missing .env or authKey?**
  - Ensure the `.env` file exists **inside the `asos` folder** with the correct `authKey=...` entry.
