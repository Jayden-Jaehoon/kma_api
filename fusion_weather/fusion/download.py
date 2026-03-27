"""
융합기상정보 API 다운로드 모듈

기상청 API허브에서 고해상도 격자자료를 다운로드
"""

import os
from datetime import datetime
from typing import Optional
import requests

from .config import FusionConfig, DEFAULT_CONFIG


class FusionDataDownloader:
    """융합기상정보 다운로드 클래스"""
    
    # API 엔드포인트
    GRID_NC_URL = "{base}/cgi-bin/url/nph-sfc_obs_nc_api"  # 전체영역 단일요소
    
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
