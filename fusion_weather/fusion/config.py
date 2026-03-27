"""
Fusion Weather Information Configuration
"""

import os
from dataclasses import dataclass, field
from typing import List, Dict


@dataclass
class FusionConfig:
    """Fusion weather information processing configuration"""

    # Project root path
    project_root: str = field(default_factory=lambda: os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Custom data root path (if None, uses project_root/data)
    custom_data_root: str = None

    # Data paths
    @property
    def data_dir(self) -> str:
        if self.custom_data_root:
            return self.custom_data_root
        return os.path.join(self.project_root, "data")

    @property
    def geodata_dir(self) -> str:
        return os.path.join(self.data_dir, "geodata")

    @property
    def fusion_raw_dir(self) -> str:
        return os.path.join(self.data_dir, "fusion_raw")

    @property
    def fusion_interim_dir(self) -> str:
        return os.path.join(self.data_dir, "fusion_interim")

    @property
    def fusion_output_dir(self) -> str:
        return os.path.join(self.data_dir, "fusion_output")

    # File paths
    @property
    def geodata_hjd_dir(self) -> str:
        """Administrative dong (HJD) shapefile directory"""
        return os.path.join(self.data_dir, "geodata_hjd")

    @property
    def grid_latlon_nc(self) -> str:
        return os.path.join(self.geodata_dir, "sfc_grid_latlon.nc")

    @property
    def grid_hjd_mapping_file(self) -> str:
        """Grid-to-administrative-dong mapping file path"""
        return os.path.join(self.geodata_dir, "grid_to_hjd.parquet")

    # API configuration
    api_base_url: str = "https://apihub-org.kma.go.kr/api/typ01"
    max_query_minutes: int = 60  # Maximum API query period (minutes)
    api_sleep_seconds: float = 0.5  # API call interval

    # Download retry configuration
    # - Handles temporary network failures (ChunkedEncodingError, etc.) and intermittent API errors
    # - If all retries fail, the exception propagates to the upper loop (process_month, etc.) to skip the date
    download_retry_attempts: int = 3  # Total number of attempts (= 1 initial request + retries)
    download_retry_initial_sleep_seconds: float = 10.0  # Wait time after first failure (seconds)
    download_retry_backoff: float = 2.0  # Retry wait time multiplier (exponential backoff)
    
    # Variable configuration
    variables: Dict[str, Dict] = field(default_factory=lambda: {
        'ta': {
            'name': 'Temperature',
            'unit': '℃',
            'hourly_agg': 'mean',  # 1-hour aggregation: mean
            'col_prefix': 't',
            'hours': 24,  # 24 columns
        },
        'rn_60m': {
            'name': '60-min Precipitation',
            'unit': 'mm',
            'hourly_agg': 'last',  # Already 60-min cumulative value
            'col_prefix': 'p',
            'hours': 24,
        },
        'sd_3hr': {
            'name': '3-hour New Snowfall',
            'unit': 'cm',
            'hourly_agg': 'last',  # 3-hour interval as is
            'col_prefix': 's',
            'hours': 8,  # 8 columns (3-hour intervals)
            'seasonal': True,  # October-May only (not produced in summer)
            'start_year': 2020,  # Available from 2020
        },
    })

    # Data period
    data_start_year: int = 1997
    snow_start_year: int = 2020  # Snowfall data start year

    def ensure_dirs(self):
        """Create necessary directories"""
        for dir_path in [
            self.geodata_hjd_dir,
            self.fusion_raw_dir,
            self.fusion_interim_dir,
            self.fusion_output_dir,
        ]:
            os.makedirs(dir_path, exist_ok=True)

    def get_hourly_columns(self, var_key: str) -> List[str]:
        """Return list of hourly column names for each variable"""
        var_info = self.variables[var_key]
        prefix = var_info['col_prefix']
        hours = var_info['hours']

        if hours == 24:
            # 1-hour intervals: t0001, t0102, ..., t2324
            return [f"{prefix}{h:02d}{(h+1) % 24:02d}" for h in range(24)]
        elif hours == 8:
            # 3-hour intervals: s0003, s0306, ..., s2124
            return [f"{prefix}{h*3:02d}{(h+1)*3:02d}" for h in range(8)]
        else:
            raise ValueError(f"Unsupported hours: {hours}")


# Default configuration instance
DEFAULT_CONFIG = FusionConfig()
