"""
융합기상정보 API 다운로드 모듈

기상청 API허브에서 고해상도 격자자료를 다운로드
"""

import os
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import requests

import pandas as pd
import numpy as np

from .config import FusionConfig, DEFAULT_CONFIG


class FusionDataDownloader:
    """융합기상정보 다운로드 클래스"""
    
    # API 엔드포인트
    POINT_MULTI_VAR_URL = "{base}/url/sfc_nc_var.php"  # 특정 지점 다중요소
    GRID_NC_URL = "{base}/cgi-bin/url/nph-sfc_obs_nc_api"  # 전체영역 단일요소
    GRID_NC_DOWNLOAD_URL = "{base}/url/sfc_grid_nc_down.php"  # NetCDF 다운로드
    
    def __init__(self, auth_key: str, config: Optional[FusionConfig] = None):
        self.auth_key = auth_key
        self.config = config or DEFAULT_CONFIG
        self.config.ensure_dirs()
    
    def download_hour_all_grid(
        self,
        tm: str,
        obs: str,
        save_dir: Optional[str] = None,
        disp: str = 'A',  # A: ASCII, B: Binary
    ) -> Optional[str]:
        """
        특정 시각의 전체 영역 단일 요소 다운로드
        
        Args:
            tm: 조회 시각 (YYYYMMDDHHmm, KST)
            obs: 요소 (ta, rn_60m, sd_3hr 등)
            save_dir: 저장 디렉토리
            disp: 출력 형태 (A: ASCII, B: Binary)
            
        Returns:
            저장된 파일 경로 또는 None
        """
        url = self.GRID_NC_URL.format(base=self.config.api_base_url)
        
        params = {
            'tm': tm,
            'obs': obs,
            'disp': disp,
            'authKey': self.auth_key,
        }
        
        try:
            resp = requests.get(url, params=params, timeout=120)
            
            if resp.status_code == 403:
                print(f"다운로드 거부 (403 Forbidden) - {obs}: API 권한을 확인해주세요.")
                print(f"  기상청 API 허브(apihub.kma.go.kr) 마이페이지에서 '{obs}' 요소의 사용 권한이 있는지 확인이 필요합니다.")
                return None
                
            resp.raise_for_status()
            
            # 저장
            if save_dir:
                os.makedirs(save_dir, exist_ok=True)
                filename = f"{obs}_{tm}.txt" if disp == 'A' else f"{obs}_{tm}.bin"
                filepath = os.path.join(save_dir, filename)
                
                mode = 'w' if disp == 'A' else 'wb'
                content = resp.text if disp == 'A' else resp.content
                
                with open(filepath, mode, encoding='utf-8' if disp == 'A' else None) as f:
                    f.write(content)
                
                return filepath
            
            return resp.text if disp == 'A' else resp.content
            
        except Exception as e:
            print(f"다운로드 실패 ({tm}, {obs}): {e}")
            return None
    
    def download_point_multi_var(
        self,
        tm1: str,
        tm2: str,
        lat: float,
        lon: float,
        obs_list: List[str],
        itv: int = 5,
    ) -> Optional[pd.DataFrame]:
        """
        특정 지점의 다중 변수 데이터 다운로드
        
        Args:
            tm1: 시작 시각 (YYYYMMDDHHmm)
            tm2: 종료 시각 (YYYYMMDDHHmm) - 최대 60분
            lat: 위도
            lon: 경도
            obs_list: 요소 목록 ['ta', 'rn_60m', ...]
            itv: 조회 간격 (분)
            
        Returns:
            DataFrame 또는 None
        """
        url = self.POINT_MULTI_VAR_URL.format(base=self.config.api_base_url)
        
        params = {
            'tm1': tm1,
            'tm2': tm2,
            'lat': lat,
            'lon': lon,
            'obs': ','.join(obs_list),
            'itv': itv,
            'help': 0,
            'authKey': self.auth_key,
        }
        
        try:
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            
            # 응답 파싱
            df = self._parse_point_response(resp.text, obs_list)
            return df
            
        except Exception as e:
            print(f"다운로드 실패 ({tm1}~{tm2}, {lat},{lon}): {e}")
            return None
    
    def download_day_data(
        self,
        date: str,
        obs: str,
        save_dir: Optional[str] = None,
    ) -> List[str]:
        """
        하루 전체 데이터 다운로드 (24시간 × API 호출)
        
        Args:
            date: 날짜 (YYYYMMDD)
            obs: 요소
            save_dir: 저장 디렉토리
            
        Returns:
            저장된 파일 경로 리스트
        """
        if save_dir is None:
            save_dir = os.path.join(self.config.fusion_raw_dir, date[:4], date[4:6])
        
        os.makedirs(save_dir, exist_ok=True)
        
        saved_files = []
        
        # 적설은 30분 간격, 나머지는 5분 간격
        if obs.startswith('sd_'):
            # 적설: 00:00, 00:30, 01:00, ... (48회)
            times = [f"{date}{h:02d}{m:02d}" for h in range(24) for m in [0, 30]]
        else:
            # 기온, 강수: 00:00, 01:00, ... (매시 정각만 - 60분 강수량은 정각 기준)
            # 또는 5분 단위로 모두 다운로드
            times = [f"{date}{h:02d}00" for h in range(24)]
        
        for tm in times:
            filepath = self.download_hour_all_grid(tm, obs, save_dir)
            if filepath:
                saved_files.append(filepath)
            
            # API 호출 간격
            time.sleep(self.config.api_sleep_seconds)
        
        return saved_files
    
    def download_netcdf(
        self,
        tm: str,
        obs: str,
        save_dir: Optional[str] = None,
    ) -> Optional[str]:
        """
        NetCDF4 형식으로 전체 영역 다운로드
        
        Args:
            tm: 조회 시각 (YYYYMMDDHHmm)
            obs: 요소
            save_dir: 저장 디렉토리
            
        Returns:
            저장된 파일 경로
        """
        url = self.GRID_NC_DOWNLOAD_URL.format(base=self.config.api_base_url)
        
        params = {
            'tm': tm,
            'obs': obs,
            'authKey': self.auth_key,
        }
        
        try:
            resp = requests.get(url, params=params, timeout=120)
            resp.raise_for_status()
            
            if save_dir:
                os.makedirs(save_dir, exist_ok=True)
                filename = f"{obs}_{tm}.nc"
                filepath = os.path.join(save_dir, filename)
                
                with open(filepath, 'wb') as f:
                    f.write(resp.content)
                
                return filepath
            
            return resp.content
            
        except Exception as e:
            print(f"NetCDF 다운로드 실패 ({tm}, {obs}): {e}")
            return None
    
    def _parse_point_response(self, text: str, obs_list: List[str]) -> Optional[pd.DataFrame]:
        """
        특정 지점 API 응답 파싱
        
        응답 형식 예시:
        # TM, LAT, LON, TA, TD, HM
        202306110000, 33.361, 126.5329, 22.5, 18.3, 78.2
        202306110005, 33.361, 126.5329, 22.4, 18.2, 78.5
        ...
        """
        lines = text.strip().split('\n')
        data_lines = [l for l in lines if l.strip() and not l.strip().startswith('#')]
        
        if not data_lines:
            return None
        
        # 헤더 추출 (주석에서)
        header_line = None
        for line in lines:
            if line.strip().startswith('#') and 'TM' in line.upper():
                header_line = line.strip('#').strip()
                break
        
        if header_line:
            columns = [c.strip() for c in header_line.split(',')]
        else:
            # 기본 컬럼 구성
            columns = ['TM', 'LAT', 'LON'] + [o.upper() for o in obs_list]
        
        # 데이터 파싱
        records = []
        for line in data_lines:
            values = [v.strip() for v in line.split(',')]
            if len(values) >= len(columns):
                records.append(values[:len(columns)])
        
        if not records:
            return None
        
        df = pd.DataFrame(records, columns=columns)
        
        # 타입 변환
        if 'TM' in df.columns:
            df['TM'] = pd.to_datetime(df['TM'], format='%Y%m%d%H%M')
        
        for col in df.columns:
            if col not in ['TM']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    @staticmethod
    def parse_grid_ascii(text: str, nx: int = None, ny: int = None) -> np.ndarray:
        """
        ASCII 형식 격자 데이터 파싱
        
        Args:
            text: ASCII 응답 텍스트
            nx: X축 격자 수 (없으면 자동 감지)
            ny: Y축 격자 수 (없으면 자동 감지)
            
        Returns:
            2D numpy 배열
        """
        lines = text.strip().split('\n')
        data_lines = [l for l in lines if l.strip() and not l.strip().startswith('#')]
        
        # 값 추출
        values = []
        for line in data_lines:
            row_values = [float(v) for v in line.split() if v.strip()]
            values.extend(row_values)
        
        values = np.array(values)
        
        # 격자 크기 추론
        if nx and ny:
            return values.reshape((ny, nx))
        else:
            # 정사각형에 가깝게 추론
            n = len(values)
            ny_guess = int(np.sqrt(n))
            nx_guess = n // ny_guess
            if nx_guess * ny_guess == n:
                return values.reshape((ny_guess, nx_guess))
            else:
                return values  # 1D로 반환


def download_fusion_data(
    auth_key: str,
    date: str,
    variables: List[str] = None,
    config: Optional[FusionConfig] = None,
) -> Dict[str, List[str]]:
    """
    하루치 융합기상 데이터 다운로드 (편의 함수)
    
    Args:
        auth_key: API 인증키
        date: 날짜 (YYYYMMDD)
        variables: 변수 목록 (기본: ['ta', 'rn_60m', 'sd_3hr'])
        config: 설정
        
    Returns:
        {변수: [파일 경로 리스트]} 딕셔너리
    """
    if variables is None:
        variables = ['ta', 'rn_60m', 'sd_3hr']
    
    downloader = FusionDataDownloader(auth_key, config)
    
    result = {}
    for var in variables:
        print(f"  다운로드 중: {var} ({date})")
        files = downloader.download_day_data(date, var)
        result[var] = files
        print(f"    완료: {len(files)} 파일")
    
    return result


if __name__ == "__main__":
    import dotenv
    dotenv.load_dotenv()
    
    auth_key = os.getenv("authKey")
    if not auth_key:
        raise ValueError("authKey를 .env 파일에 설정해주세요")
    
    # 테스트: 하루 데이터 다운로드
    downloader = FusionDataDownloader(auth_key)
    
    # 단일 시각 테스트
    result = downloader.download_hour_all_grid(
        tm="202401011200",
        obs="ta",
        save_dir="/tmp/fusion_test",
    )
    print(f"테스트 결과: {result}")
