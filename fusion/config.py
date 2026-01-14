"""
융합기상정보 설정
"""

import os
from dataclasses import dataclass, field
from typing import List, Dict


@dataclass
class FusionConfig:
    """융합기상정보 처리 설정"""
    
    # 프로젝트 경로
    project_root: str = "/Users/jaehoon/liminal_ego/git_clones/kma_api"
    
    # 데이터 경로
    @property
    def data_dir(self) -> str:
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
    
    # 파일 경로
    @property
    def legal_dong_shp(self) -> str:
        return os.path.join(self.geodata_dir, "BND_ADM_DONG_PG.shp")
    
    @property
    def grid_latlon_nc(self) -> str:
        return os.path.join(self.geodata_dir, "sfc_grid_latlon.nc")
    
    @property
    def grid_mapping_file(self) -> str:
        return os.path.join(self.geodata_dir, "grid_to_lawid.parquet")
    
    # API 설정
    api_base_url: str = "https://apihub.kma.go.kr/api/typ01"
    max_query_minutes: int = 60  # API 최대 조회 기간 (분)
    api_sleep_seconds: float = 0.5  # API 호출 간격

    # 다운로드 재시도 설정
    # - 네트워크 일시 장애(ChunkedEncodingError 등)나 간헐적인 API 오류에 대비합니다.
    # - 최종적으로도 실패하면 상위 루프(process_month 등)에서 날짜를 건너뛰도록 예외가 전파됩니다.
    download_retry_attempts: int = 3  # 총 시도 횟수(= 1회 요청 + 재시도)
    download_retry_initial_sleep_seconds: float = 10.0  # 1회 실패 후 대기(초)
    download_retry_backoff: float = 2.0  # 재시도 대기시간 배수(지수 backoff)
    
    # 변수 설정
    variables: Dict[str, Dict] = field(default_factory=lambda: {
        'ta': {
            'name': '기온',
            'unit': '℃',
            'hourly_agg': 'mean',  # 1시간 집계: 평균
            'col_prefix': 't',
            'hours': 24,  # 24개 컬럼
        },
        'rn_60m': {
            'name': '60분강수량',
            'unit': 'mm',
            'hourly_agg': 'last',  # 이미 60분 누적값
            'col_prefix': 'p',
            'hours': 24,
        },
        'sd_3hr': {
            'name': '3시간신적설',
            'unit': 'cm',
            'hourly_agg': 'last',  # 3시간 단위 그대로
            'col_prefix': 's',
            'hours': 8,  # 8개 컬럼 (3시간 단위)
            'seasonal': True,  # 10월~5월만 (여름철 미생산)
            'start_year': 2020,  # 2020년부터 제공
        },
    })
    
    # 데이터 기간
    data_start_year: int = 1997
    snow_start_year: int = 2020  # 적설 데이터 시작 연도
    
    def ensure_dirs(self):
        """필요한 디렉토리 생성"""
        for dir_path in [
            self.fusion_raw_dir,
            self.fusion_interim_dir,
            self.fusion_output_dir,
        ]:
            os.makedirs(dir_path, exist_ok=True)
    
    def get_hourly_columns(self, var_key: str) -> List[str]:
        """변수별 시간 컬럼명 리스트 반환"""
        var_info = self.variables[var_key]
        prefix = var_info['col_prefix']
        hours = var_info['hours']
        
        if hours == 24:
            # 1시간 단위: t0001, t0102, ..., t2324
            return [f"{prefix}{h:02d}{(h+1) % 24:02d}" for h in range(24)]
        elif hours == 8:
            # 3시간 단위: s0003, s0306, ..., s2124
            return [f"{prefix}{h*3:02d}{(h+1)*3:02d}" for h in range(8)]
        else:
            raise ValueError(f"Unsupported hours: {hours}")


# 기본 설정 인스턴스
DEFAULT_CONFIG = FusionConfig()
