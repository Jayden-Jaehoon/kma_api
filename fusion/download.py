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

    def _get_validation_log_path(self, date: str, obs: str) -> str:
        """검증/다운로드 오류 로그 파일 경로.

        요청사항: 검증 오류가 생긴 부분을 `data/fusion_raw` 아래 txt로 남깁니다.
        - 너무 많은 파일이 생기지 않도록 하루/변수 단위로 append 합니다.
        """
        year = date[:4]
        month = date[4:6]
        log_dir = os.path.join(self.config.fusion_raw_dir, "_validation_logs", year, month)
        os.makedirs(log_dir, exist_ok=True)
        return os.path.join(log_dir, f"{date}_{obs}.txt")

    def _append_validation_log(
        self,
        *,
        date: str,
        obs: str,
        tm: Optional[str],
        level: str,
        message: str,
        response_preview: Optional[str] = None,
    ) -> None:
        """검증 로그를 파일에 append."""
        path = self._get_validation_log_path(date=date, obs=obs)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tm_part = tm if tm is not None else "-"

        lines = [
            f"[{ts}] [{level}] tm={tm_part} obs={obs} :: {message}",
        ]
        if response_preview:
            lines.append("  response_preview: " + response_preview.replace("\n", " ")[:500])

        with open(path, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

    @staticmethod
    def _looks_like_error_response(text: str) -> bool:
        """ASCII 응답이 데이터가 아닌 에러/HTML 본문처럼 보이는지 빠르게 판별."""
        if text is None:
            return True

        t = text.strip()
        if not t:
            return True

        head = t[:400].lower()
        if "<html" in head or "<!doctype html" in head:
            return True
        if "forbidden" in head or "unauthorized" in head:
            return True
        if "error" in head and "#" not in head:
            # 데이터 포맷이 아닌 에러 메시지일 가능성
            return True
        return False
    
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
                self._append_validation_log(
                    date=tm[:8],
                    obs=obs,
                    tm=tm,
                    level="ERROR",
                    message="HTTP 403 Forbidden",
                    response_preview=resp.text if resp is not None else None,
                )
                return None
                
            resp.raise_for_status()

            # 최소 검증: ASCII인데 데이터가 아닌 에러/HTML 응답이면 실패로 처리하고 로그를 남깁니다.
            if disp == 'A' and self._looks_like_error_response(resp.text):
                self._append_validation_log(
                    date=tm[:8],
                    obs=obs,
                    tm=tm,
                    level="ERROR",
                    message="응답 본문이 비어있거나 에러/HTML로 보입니다.",
                    response_preview=resp.text,
                )
                return None
            
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
            self._append_validation_log(
                date=tm[:8],
                obs=obs,
                tm=tm,
                level="ERROR",
                message=f"다운로드 예외: {type(e).__name__}: {e}",
            )
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

            # 응답 인코딩 보정
            # - API는 종종 `text/plain;charset=EUC-KR` 형태로 내려옵니다.
            # - requests가 추정에 실패하면 한글/특수문자 파싱이 깨질 수 있어 헤더 기반으로 보정합니다.
            content_type = (resp.headers.get("content-type") or "").lower()
            if "euc-kr" in content_type or "euckr" in content_type:
                resp.encoding = "euc-kr"
            elif "cp949" in content_type:
                resp.encoding = "cp949"
            
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

        retry_attempts = max(1, int(getattr(self.config, "download_retry_attempts", 1)))
        retry_initial_sleep = float(getattr(self.config, "download_retry_initial_sleep_seconds", 0.0))
        retry_backoff = float(getattr(self.config, "download_retry_backoff", 1.0))
        
        # 적설은 30분 간격, 나머지는 5분 간격
        if obs.startswith('sd_'):
            # 적설: 00:00, 00:30, 01:00, ... (48회)
            times = [f"{date}{h:02d}{m:02d}" for h in range(24) for m in [0, 30]]
        else:
            # 기온, 강수: 00:00, 01:00, ... (매시 정각만 - 60분 강수량은 정각 기준)
            # 또는 5분 단위로 모두 다운로드
            times = [f"{date}{h:02d}00" for h in range(24)]
        
        for tm in times:
            filepath = None
            for attempt in range(1, retry_attempts + 1):
                filepath = self.download_hour_all_grid(tm, obs, save_dir)
                if filepath:
                    break

                self._append_validation_log(
                    date=date,
                    obs=obs,
                    tm=tm,
                    level="WARN" if attempt < retry_attempts else "ERROR",
                    message=f"다운로드 실패/빈 응답 (attempt {attempt}/{retry_attempts})",
                )

                if attempt < retry_attempts:
                    sleep_seconds = retry_initial_sleep * (retry_backoff ** (attempt - 1))
                    self._append_validation_log(
                        date=date,
                        obs=obs,
                        tm=tm,
                        level="INFO",
                        message=f"재시도 대기: {sleep_seconds:.1f}s 후 재요청 (next_attempt {attempt + 1}/{retry_attempts})",
                    )
                    time.sleep(max(0.0, sleep_seconds))

            if filepath:
                saved_files.append(filepath)
            
            # API 호출 간격
            time.sleep(self.config.api_sleep_seconds)

        # 시간대 누락 검증: 기대한 tm 수만큼 파일이 만들어졌는지 확인
        if len(saved_files) != len(times):
            saved_set = set(os.path.basename(p) for p in saved_files)
            expected_set = set(f"{obs}_{tm}.txt" for tm in times)
            missing = sorted(expected_set - saved_set)
            self._append_validation_log(
                date=date,
                obs=obs,
                tm=None,
                level="ERROR",
                message=f"하루 다운로드 누락: saved={len(saved_files)}/{len(times)} missing_files={len(missing)}",
                response_preview=("missing=" + ",".join(missing[:20])) if missing else None,
            )
            raise RuntimeError(
                f"하루 다운로드 누락: {date} {obs} saved={len(saved_files)}/{len(times)}. "
                f"validation_log={self._get_validation_log_path(date=date, obs=obs)}"
            )

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
        if text is None:
            return None

        lines = [l.rstrip() for l in text.strip().split('\n') if l.strip()]
        if not lines:
            return None

        # 1) 컬럼명 결정
        # - `help=1`일 때 종종 아래처럼 내려옵니다.
        #   "# tm, ta, rn_60m, sd_3hr"
        # - 어떤 경우에는 LAT/LON이 포함될 수도 있고, 포함되지 않을 수도 있습니다.
        header_line = None
        for line in lines:
            s = line.strip()
            if not s.startswith('#'):
                continue
            # '# tm, ...' 또는 '# TM, ...' 형태를 우선적으로 잡습니다.
            body = s.lstrip('#').strip()
            if body.lower().startswith('tm'):
                header_line = body
                break

        if header_line:
            columns = [c.strip() for c in header_line.split(',') if c.strip()]
        else:
            columns = []

        # 2) 데이터 라인 수집
        # - '#START7777' 같은 시작 마커/주석은 제외
        # - 일부 응답은 마지막에 'YYYYMMDD' 같은 단일 토큰 라인이 붙기도 해서 스킵
        data_rows: list[list[str]] = []
        for line in lines:
            s = line.strip()
            if not s or s.startswith('#'):
                continue

            parts = [p.strip() for p in s.split(',') if p.strip()]
            if not parts:
                continue

            # 말미에 붙는 '20241128' 같은 라인(콤마 없이 날짜만) 방어
            if len(parts) == 1 and parts[0].isdigit() and len(parts[0]) == 8:
                continue

            data_rows.append(parts)

        if not data_rows:
            return None

        # 3) 헤더가 없을 때는 첫 데이터의 컬럼 수로 추론
        if not columns:
            ncols = len(data_rows[0])
            # 흔한 케이스: tm + obs_list
            if ncols == 1 + len(obs_list):
                columns = ['tm'] + obs_list
            # 대안: tm + lat + lon + obs_list
            elif ncols == 3 + len(obs_list):
                columns = ['tm', 'lat', 'lon'] + obs_list
            else:
                # 그래도 모르겠으면 일단 tm + v1..vn 형태로 생성
                columns = ['tm'] + [f"v{i}" for i in range(1, ncols)]

        # 4) 데이터 길이 맞는 행만 취함 (불일치 행은 스킵)
        records = [row[:len(columns)] for row in data_rows if len(row) >= len(columns)]
        if not records:
            return None

        df = pd.DataFrame(records, columns=columns)

        # 5) 타입 변환
        # - tm: YYYYMMDDHHmm 형식(문서 기준)
        # - 나머지: 숫자 변환
        tm_col = None
        for c in ['TM', 'tm']:
            if c in df.columns:
                tm_col = c
                break

        if tm_col is not None:
            # 이전 코드와의 호환을 위해 TM 컬럼은 datetime으로 변환
            df[tm_col] = pd.to_datetime(df[tm_col].astype(str), format='%Y%m%d%H%M', errors='coerce')

        for col in df.columns:
            if tm_col is not None and col == tm_col:
                continue
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
