"""
융합기상정보 전체 처리 파이프라인

다운로드 → 파싱 → 시간 집계 → 공간 집계 → 출력
"""

import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import time

import numpy as np
import pandas as pd
from tqdm import tqdm

from .config import FusionConfig, DEFAULT_CONFIG
from .geocode import GridToLawIdMapper
from .download import FusionDataDownloader
from .aggregate import TimeAggregator, SpatialAggregator, OutputFormatter


class FusionPipeline:
    """융합기상정보 처리 파이프라인"""
    
    def __init__(
        self,
        auth_key: str,
        config: Optional[FusionConfig] = None,
    ):
        self.auth_key = auth_key
        self.config = config or DEFAULT_CONFIG
        self.config.ensure_dirs()
        
        # 컴포넌트 초기화
        self.downloader = FusionDataDownloader(auth_key, self.config)
        self.mapper = GridToLawIdMapper(self.config)
        self.time_agg = TimeAggregator(self.config)
        self.formatter = OutputFormatter(self.config)
        
        # 매핑 테이블
        self._grid_mapping: Optional[pd.DataFrame] = None
        self._spatial_agg: Optional[SpatialAggregator] = None

        # 다운로드/파싱 단계에서만 필요할 때 사용할 “기대 격자 수” 캐시.
        # - 후처리(공간집계)에서는 매핑 테이블이 필요하지만,
        # - 다운로드 전용(A 단계)에서는 매핑 테이블 전체(4.2M)를 매번 로드하는 것이 부담이므로
        #   NetCDF 차원(ny*nx)에서 기대 격자 수를 빠르게 계산해 Strict 검증에 사용합니다.
        self._expected_grid_n: Optional[int] = None
    
    def ensure_mapping(self, force_rebuild: bool = False):
        """격자-법정동 매핑 테이블 확보"""
        if self._grid_mapping is None or force_rebuild:
            self._grid_mapping = self.mapper.build_mapping(force_rebuild=force_rebuild)
            self._spatial_agg = SpatialAggregator(self._grid_mapping, self.config)

    def _get_expected_grid_n(self) -> Optional[int]:
        """격자 API 응답의 기대 값 개수(N)를 반환.

        우선순위:
        1) 이미 매핑 테이블이 로드되어 있으면 `len(self._grid_mapping)` 사용
        2) 아니면 NetCDF(`sfc_grid_latlon.nc`)의 차원/변수 크기에서 계산(ny*nx 또는 lat.size)

        목적:
        - A(다운로드 전용) 단계에서 매핑 테이블 전체 로드 없이도 Strict 파싱 검증을 할 수 있게 함.
        """
        if self._grid_mapping is not None:
            return len(self._grid_mapping)

        if self._expected_grid_n is not None:
            return self._expected_grid_n

        nc_path = getattr(self.config, "grid_latlon_nc", None)
        if not nc_path or not os.path.exists(nc_path):
            return None

        try:
            import xarray as xr

            with xr.open_dataset(nc_path) as ds:
                sizes = dict(getattr(ds, "sizes", {}))
                if "ny" in sizes and "nx" in sizes:
                    n = int(sizes["ny"]) * int(sizes["nx"])
                elif "lat" in ds:
                    # ds['lat'].size는 데이터를 실제로 로드하지 않고 shape로부터 계산됩니다.
                    n = int(ds["lat"].size)
                else:
                    return None

            self._expected_grid_n = n
            return n
        except Exception:
            return None

    def ensure_day_cache(
        self,
        date: str,
        variables: List[str],
    ) -> Dict[str, Dict[str, str]]:
        """하루치 raw 캐시(`*_parsed.parquet`)를 보장(A 단계용).

        - 다운로드/파싱/검증/캐시 저장만 수행하고, 후처리(피벗/공간집계)는 하지 않습니다.
        - 변수별 성공/실패를 요약해서 반환합니다.

        Returns:
            {
              "ok": {var: cache_path},
              "failed": {var: "error message"}
            }
        """
        year = date[:4]
        month = date[4:6]

        # 적설 데이터 가능 여부 확인(기존 process_day와 동일한 정책)
        var_list = list(variables)
        if "sd_3hr" in var_list:
            int_year = int(year)
            int_month = int(month)
            if int_year < self.config.snow_start_year:
                var_list = [v for v in var_list if v != "sd_3hr"]
            if int_month in [6, 7, 8, 9]:
                var_list = [v for v in var_list if v != "sd_3hr"]

        raw_dir = os.path.join(self.config.fusion_raw_dir, year, month)

        ok: Dict[str, str] = {}
        failed: Dict[str, str] = {}
        for var in var_list:
            cache_path = os.path.join(raw_dir, f"{var}_{date}_parsed.parquet")
            try:
                self._load_or_download_day(date, var, raw_dir)
                ok[var] = cache_path
            except Exception as e:
                failed[var] = f"{type(e).__name__}: {e}"

        return {"ok": ok, "failed": failed}

    def process_day_from_cache(
        self,
        date: str,
        variables: List[str] = None,
        save_interim: bool = True,
    ) -> pd.DataFrame:
        """캐시된 raw parquet만 사용해 하루치 후처리를 수행(B 단계용).

        주의:
        - 이 함수는 다운로드를 수행하지 않습니다.
        - `data/fusion_raw/YYYY/MM/{var}_{date}_parsed.parquet`가 없으면 해당 변수를 스킵합니다.
        """
        if variables is None:
            variables = ["ta", "rn_60m"]

        # 테스트/특수 실행에서는 이미 _grid_mapping/_spatial_agg를 주입할 수 있어,
        # 존재하지 않으면 그때만 매핑을 로드합니다.
        if self._grid_mapping is None or self._spatial_agg is None:
            self.ensure_mapping()

        year = date[:4]
        month = date[4:6]

        # 적설 데이터 가능 여부 확인(기존 process_day와 동일한 정책)
        var_list = list(variables)
        if "sd_3hr" in var_list:
            int_year = int(year)
            int_month = int(month)
            if int_year < self.config.snow_start_year:
                var_list = [v for v in var_list if v != "sd_3hr"]
            if int_month in [6, 7, 8, 9]:
                var_list = [v for v in var_list if v != "sd_3hr"]

        raw_dir = os.path.join(self.config.fusion_raw_dir, year, month)
        results: Dict[str, pd.DataFrame] = {}

        for var in var_list:
            var_info = self.config.variables.get(var, {})
            col_prefix = var_info.get("col_prefix", var[0])
            is_3hourly = var_info.get("hours", 24) == 8

            cache_path = os.path.join(raw_dir, f"{var}_{date}_parsed.parquet")
            if not os.path.exists(cache_path):
                # B 정책: 누락은 스킵 (상위 스크립트에서 요약)
                continue

            df_raw = pd.read_parquet(cache_path)
            if df_raw is None or len(df_raw) == 0:
                continue

            # 시간 집계: 현재 raw가 이미 정각/3시간 정각 단위이므로 그대로 사용
            df_hourly = df_raw

            # 피벗 (시간 → 컬럼)
            df_pivot = self.time_agg.pivot_hourly_to_columns(
                df_hourly,
                var_col="value",
                col_prefix=col_prefix,
                is_3hourly=is_3hourly,
            )

            # 공간 집계 (격자 → 법정동)
            value_cols = [c for c in df_pivot.columns if c.startswith(col_prefix)]
            df_lawid = self._spatial_agg.aggregate_grid_to_lawid(
                df_pivot,
                value_cols=value_cols,
                method="mean",
            )
            results[var] = df_lawid

        if not results:
            return pd.DataFrame()

        final_df = self.formatter.merge_variables(results)

        if save_interim:
            interim_dir = os.path.join(self.config.fusion_interim_dir, year)
            os.makedirs(interim_dir, exist_ok=True)
            interim_path = os.path.join(interim_dir, f"fusion_{date}.parquet")
            final_df.to_parquet(interim_path, index=False)

        return final_df

    def _get_validation_log_path(self, date: str, var: str) -> str:
        """검증/다운로드 오류 로그 파일 경로 (`data/fusion_raw` 하위 txt)."""
        year = date[:4]
        month = date[4:6]
        log_dir = os.path.join(self.config.fusion_raw_dir, "_validation_logs", year, month)
        os.makedirs(log_dir, exist_ok=True)
        return os.path.join(log_dir, f"{date}_{var}.txt")

    def _append_validation_log(
        self,
        *,
        date: str,
        var: str,
        tm: str,
        level: str,
        message: str,
        exception: Optional[BaseException] = None,
        response_preview: Optional[str] = None,
    ) -> str:
        """검증 로그를 파일에 append 하고, 로그 파일 경로를 반환."""
        path = self._get_validation_log_path(date=date, var=var)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        lines = [
            f"[{ts}] [{level}] tm={tm} var={var} :: {message}",
        ]
        if exception is not None:
            lines.append(f"  exception: {type(exception).__name__}: {exception}")
        if response_preview:
            lines.append("  response_preview: " + response_preview.replace("\n", " ")[:500])

        with open(path, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        return path

    @staticmethod
    def _write_response_snippet(
        *,
        raw_dir: str,
        var: str,
        tm: str,
        response_text: Optional[str],
        exception: Optional[BaseException] = None,
    ) -> Optional[str]:
        """파싱/검증 실패 시 원문 전체를 저장하지 않고, 디버깅용 스니펫만 저장."""
        if response_text is None:
            return None

        os.makedirs(raw_dir, exist_ok=True)
        path = os.path.join(raw_dir, f"{var}_{tm}_error_snippet.txt")

        lines = response_text.splitlines()
        head_lines = lines[:30]
        tail_lines = lines[-30:] if len(lines) > 30 else []

        with open(path, "w", encoding="utf-8") as f:
            f.write(f"tm={tm} var={var}\n")
            f.write(f"total_chars={len(response_text):,} total_lines={len(lines):,}\n")
            if exception is not None:
                f.write(f"exception={type(exception).__name__}: {exception}\n")
            f.write("\n--- head (first 30 lines) ---\n")
            f.write("\n".join(head_lines) + "\n")
            if tail_lines:
                f.write("\n--- tail (last 30 lines) ---\n")
                f.write("\n".join(tail_lines) + "\n")

        return path
    
    def process_day(
        self,
        date: str,
        variables: List[str] = None,
        save_interim: bool = True,
    ) -> pd.DataFrame:
        """
        하루 데이터 처리
        
        Args:
            date: 날짜 (YYYYMMDD)
            variables: 변수 목록 (기본: ['ta', 'rn_60m'])
            save_interim: 중간 결과 저장 여부
            
        Returns:
            법정동별 일별 집계 DataFrame
        """
        if variables is None:
            variables = ['ta', 'rn_60m']
        
        self.ensure_mapping()
        
        year = date[:4]
        month = date[4:6]
        
        # 적설 데이터 가능 여부 확인
        if 'sd_3hr' in variables:
            int_year = int(year)
            int_month = int(month)
            
            # 2020년 이전이면 적설 제외
            if int_year < self.config.snow_start_year:
                variables = [v for v in variables if v != 'sd_3hr']
                print(f"  적설 데이터는 {self.config.snow_start_year}년부터 제공됩니다.")
            
            # 여름철 (6~9월)이면 적설 제외
            if int_month in [6, 7, 8, 9]:
                variables = [v for v in variables if v != 'sd_3hr']
                print(f"  여름철({int_month}월)에는 적설 데이터가 생산되지 않습니다.")
        
        results = {}
        
        for var in variables:
            var_info = self.config.variables.get(var, {})
            col_prefix = var_info.get('col_prefix', var[0])
            agg_method = var_info.get('hourly_agg', 'mean')
            is_3hourly = var_info.get('hours', 24) == 8
            
            print(f"  [{var}] 처리 중...")
            
            # 1. 다운로드 (또는 캐시 로드)
            raw_dir = os.path.join(self.config.fusion_raw_dir, year, month)
            df_raw = self._load_or_download_day(date, var, raw_dir)
            
            if df_raw is None or len(df_raw) == 0:
                print(f"    데이터 없음, 건너뜀")
                continue
            
            # 2. 시간 집계 (이미 1시간 단위라면 스킵)
            if is_3hourly:
                # 3시간 적설은 그대로 사용
                df_hourly = df_raw
            else:
                # 기온/강수는 이미 1시간 단위로 다운로드됨
                df_hourly = df_raw
            
            # 3. 피벗 (시간 → 컬럼)
            df_pivot = self.time_agg.pivot_hourly_to_columns(
                df_hourly,
                var_col='value',
                col_prefix=col_prefix,
                is_3hourly=is_3hourly,
            )
            
            # 4. 공간 집계 (격자 → 법정동)
            value_cols = [c for c in df_pivot.columns if c.startswith(col_prefix)]
            df_lawid = self._spatial_agg.aggregate_grid_to_lawid(
                df_pivot,
                value_cols=value_cols,
                method='mean',
            )
            
            results[var] = df_lawid
            print(f"    완료: {len(df_lawid)} 법정동")
        
        # 5. 변수 병합
        if results:
            final_df = self.formatter.merge_variables(results)
            
            # 중간 저장
            if save_interim:
                interim_dir = os.path.join(self.config.fusion_interim_dir, year)
                os.makedirs(interim_dir, exist_ok=True)
                interim_path = os.path.join(interim_dir, f"fusion_{date}.parquet")
                final_df.to_parquet(interim_path, index=False)
            
            return final_df
        
        return pd.DataFrame()
    
    def process_month(
        self,
        year: int,
        month: int,
        variables: List[str] = None,
    ) -> pd.DataFrame:
        """
        한 달 데이터 처리
        
        Args:
            year: 연도
            month: 월
            variables: 변수 목록
            
        Returns:
            월별 집계 DataFrame
        """
        if variables is None:
            variables = ['ta', 'rn_60m']
        
        # 해당 월의 일수 계산
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        
        first_day = datetime(year, month, 1)
        num_days = (next_month - first_day).days
        
        print(f"\n{'='*60}")
        print(f"월별 처리: {year}년 {month}월 ({num_days}일)")
        print(f"{'='*60}")
        
        monthly_dfs = []
        
        for day in tqdm(range(1, num_days + 1), desc=f"{year}-{month:02d}"):
            date = f"{year}{month:02d}{day:02d}"
            try:
                df = self.process_day(date, variables, save_interim=True)
                if len(df) > 0:
                    monthly_dfs.append(df)
            except Exception as e:
                print(f"\n  {date} 처리 실패: {e}")
                continue
        
        if monthly_dfs:
            result = pd.concat(monthly_dfs, ignore_index=True)
            
            # 월별 결과 저장
            output_dir = os.path.join(self.config.fusion_output_dir, str(year))
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"fusion_{year}{month:02d}.csv")
            result.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"\n저장 완료: {output_path}")
            
            return result
        
        return pd.DataFrame()
    
    def process_year(
        self,
        year: int,
        variables: List[str] = None,
        start_month: int = 1,
        end_month: int = 12,
    ) -> str:
        """
        연도별 데이터 처리
        
        Args:
            year: 연도
            variables: 변수 목록
            start_month: 시작 월
            end_month: 종료 월
            
        Returns:
            저장된 파일 경로
        """
        if variables is None:
            variables = ['ta', 'rn_60m']
            if year >= self.config.snow_start_year:
                variables.append('sd_3hr')
        
        print(f"\n{'#'*60}")
        print(f"연도별 처리: {year}년")
        print(f"변수: {variables}")
        print(f"{'#'*60}")
        
        yearly_dfs = []
        
        for month in range(start_month, end_month + 1):
            try:
                df = self.process_month(year, month, variables)
                if len(df) > 0:
                    yearly_dfs.append(df)
            except Exception as e:
                print(f"\n{year}년 {month}월 처리 실패: {e}")
                continue
        
        if yearly_dfs:
            result = pd.concat(yearly_dfs, ignore_index=True)
            
            # 연도별 결과 저장
            output_path = os.path.join(
                self.config.fusion_output_dir,
                f"fusion_weather_{year}.csv"
            )
            result.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"\n연도별 저장 완료: {output_path}")
            print(f"총 레코드 수: {len(result):,}")
            
            return output_path
        
        return ""
    
    def process_year_range(
        self,
        start_year: int,
        end_year: int,
        variables: List[str] = None,
    ) -> List[str]:
        """
        연도 범위 데이터 처리
        
        Args:
            start_year: 시작 연도
            end_year: 종료 연도
            variables: 변수 목록
            
        Returns:
            저장된 파일 경로 리스트
        """
        results = []
        
        for year in range(start_year, end_year + 1):
            try:
                output_path = self.process_year(year, variables)
                if output_path:
                    results.append(output_path)
            except Exception as e:
                print(f"\n{year}년 처리 실패: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        return results
    
    def _load_or_download_day(
        self,
        date: str,
        var: str,
        raw_dir: str,
    ) -> Optional[pd.DataFrame]:
        """
        일별 데이터 로드 또는 다운로드
        
        캐시된 파일이 있으면 로드, 없으면 API에서 다운로드
        """
        # 캐시 파일 확인
        cache_path = os.path.join(raw_dir, f"{var}_{date}_parsed.parquet")
        
        if os.path.exists(cache_path):
            return pd.read_parquet(cache_path)
        
        # API 다운로드
        var_info = self.config.variables.get(var, {})
        is_3hourly = var_info.get('hours', 24) == 8
        
        # 시간 목록 생성
        if is_3hourly:
            # 적설: 3시간 간격 (00:00, 03:00, 06:00, ...)
            hours = list(range(0, 24, 3))
        else:
            # 기온/강수: 1시간 간격
            hours = list(range(24))
        
        expected_n = self._get_expected_grid_n()
        all_data = []

        retry_attempts = max(1, int(getattr(self.config, "download_retry_attempts", 1)))
        retry_initial_sleep = float(getattr(self.config, "download_retry_initial_sleep_seconds", 0.0))
        retry_backoff = float(getattr(self.config, "download_retry_backoff", 1.0))

        for hour in hours:
            tm = f"{date}{hour:02d}00"

            grid_values = None
            last_log_path = None
            last_exception: Optional[BaseException] = None
            last_response_preview: Optional[str] = None

            for attempt in range(1, retry_attempts + 1):
                # API 호출
                response = self.downloader.download_hour_all_grid(tm, var, save_dir=None, disp='A')
                last_response_preview = response[:500] if isinstance(response, str) else None

                if not response:
                    last_log_path = self._append_validation_log(
                        date=date,
                        var=var,
                        tm=tm,
                        level="WARN" if attempt < retry_attempts else "ERROR",
                        message=f"다운로드 실패/빈 응답 (attempt {attempt}/{retry_attempts})",
                    )
                else:
                    # 응답 파싱 (Strict)
                    try:
                        grid_values = self._parse_grid_response(response)
                    except Exception as e:
                        last_exception = e
                        snippet_path = self._write_response_snippet(
                            raw_dir=raw_dir,
                            var=var,
                            tm=tm,
                            response_text=response,
                            exception=e,
                        )
                        last_log_path = self._append_validation_log(
                            date=date,
                            var=var,
                            tm=tm,
                            level="WARN" if attempt < retry_attempts else "ERROR",
                            message=f"파싱 실패(격자 개수/포맷 불일치 가능) (attempt {attempt}/{retry_attempts}). snippet={snippet_path}",
                            exception=e,
                            response_preview=last_response_preview,
                        )
                    else:
                        if grid_values is None or len(grid_values) == 0:
                            snippet_path = self._write_response_snippet(
                                raw_dir=raw_dir,
                                var=var,
                                tm=tm,
                                response_text=response,
                            )
                            last_log_path = self._append_validation_log(
                                date=date,
                                var=var,
                                tm=tm,
                                level="WARN" if attempt < retry_attempts else "ERROR",
                                message=f"파싱 결과가 비어있음 (attempt {attempt}/{retry_attempts}). snippet={snippet_path}",
                                response_preview=last_response_preview,
                            )
                            grid_values = None
                        elif expected_n is not None and len(grid_values) != expected_n:
                            # 원칙적으로 `_parse_grid_response`에서 이미 걸러져야 하지만, 방어적으로 한 번 더 체크
                            snippet_path = self._write_response_snippet(
                                raw_dir=raw_dir,
                                var=var,
                                tm=tm,
                                response_text=response,
                            )
                            last_log_path = self._append_validation_log(
                                date=date,
                                var=var,
                                tm=tm,
                                level="WARN" if attempt < retry_attempts else "ERROR",
                                message=(
                                    f"격자 길이 불일치: parsed={len(grid_values):,}, expected={expected_n:,} "
                                    f"(attempt {attempt}/{retry_attempts}). snippet={snippet_path}"
                                ),
                                response_preview=last_response_preview,
                            )
                            grid_values = None

                if grid_values is not None and len(grid_values) > 0:
                    break

                if attempt < retry_attempts:
                    sleep_seconds = retry_initial_sleep * (retry_backoff ** (attempt - 1))
                    # 0초면 실질적으로 즉시 재시도(테스트/디버깅에서 유용)
                    self._append_validation_log(
                        date=date,
                        var=var,
                        tm=tm,
                        level="INFO",
                        message=f"재시도 대기: {sleep_seconds:.1f}s 후 재요청 (next_attempt {attempt + 1}/{retry_attempts})",
                    )
                    time.sleep(max(0.0, sleep_seconds))

            if grid_values is None or len(grid_values) == 0:
                # 최종 실패: 상위 루프(process_month 등)에서 날짜를 건너뛰도록 예외 전파
                final_log_path = self._append_validation_log(
                    date=date,
                    var=var,
                    tm=tm,
                    level="ERROR",
                    message=(
                        f"재시도 {retry_attempts}회 후에도 실패하여 날짜 스킵 대상입니다. "
                        f"(상위 루프에서 continue) last_log={last_log_path}"
                    ),
                    exception=last_exception,
                    response_preview=last_response_preview,
                )
                raise RuntimeError(f"다운로드/파싱 재시도 실패: {tm} {var}. validation_log={final_log_path}")

            df_hour = pd.DataFrame({
                'grid_idx': range(len(grid_values)),
                'date': date,
                'hour': hour,
                'value': grid_values,
            })
            all_data.append(df_hour)
            
            # API 호출 간격
            time.sleep(self.config.api_sleep_seconds)
        
        # 시간대(컬럼) 누락 검증
        if len(all_data) != len(hours):
            missing_hours = sorted(set(hours) - set(df['hour'].iloc[0] for df in all_data))
            log_path = self._append_validation_log(
                date=date,
                var=var,
                tm=f"{date}----",
                level="ERROR",
                message=f"시간대 누락: got={len(all_data)}/{len(hours)} missing_hours={missing_hours}",
            )
            raise RuntimeError(f"시간대 누락: {date} {var}. validation_log={log_path}")

        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            
            # 캐시 저장
            os.makedirs(raw_dir, exist_ok=True)
            result.to_parquet(cache_path, index=False)
            
            return result
        
        return None
    
    def _parse_grid_response(self, response_text: str) -> Optional[np.ndarray]:
        """
        격자 API 응답 파싱
        
        ASCII 형식 응답에서 값 배열 추출
        """
        if not response_text:
            return None
        
        try:
            lines = response_text.strip().split('\n')
            
            # 주석/헤더 제외
            data_lines = [l for l in lines if l.strip() and not l.strip().startswith('#')]
            
            if not data_lines:
                return None
            
            # 모든 숫자 토큰 추출 (헤더/메타 포함 가능)
            raw_numbers = []
            for line in data_lines:
                parts = line.replace(',', ' ').split()
                for p in parts:
                    try:
                        val = float(p)
                        raw_numbers.append(val)
                    except ValueError:
                        continue

            if not raw_numbers:
                return None

            # 기대 격자 수(= 매핑 테이블 길이)를 알면, 응답 값 개수는 반드시 일치해야 합니다.
            #
            # 예외적으로 API 응답 초반에 (nx, ny) 같은 격자 차원 숫자 헤더가 섞이는 경우가 있어,
            # 이 경우에만 헤더를 제거한 뒤 `expected_n`과 정확히 일치하는지 재검증합니다.
            #
            # 중요: 개수가 맞지 않는 상태에서 자르기/패딩으로 "보정"하면 `grid_idx`↔(lat,lon)
            # 매핑이 조용히 틀어질 수 있으므로, 여기서는 엄격하게 오류를 발생시킵니다.
            expected_n = self._get_expected_grid_n()
            if expected_n is not None and expected_n > 0:
                original_n = len(raw_numbers)
                header_cut = None
                header_pair = None
                after_strip_n = original_n

                if original_n != expected_n:
                    # 값이 "부족"한 경우에는 헤더 제거로 해결될 수 없으므로 즉시 오류로 처리합니다.
                    # (헤더가 있다면 오히려 값 개수는 더 줄어듭니다.)
                    if original_n > expected_n:
                        # 흔한 케이스: 앞쪽에 (nx, ny) 같은 격자 차원 정보가 포함되는 경우
                        # - 숫자만 뽑아오면 2개가 추가로 붙어 `expected_n + 2`가 될 수 있음
                        # - `nx * ny == expected_n`인 연속된 두 정수를 초반에서 찾으면 그 앞을 헤더로 간주
                        scan_limit = min(20, original_n - 1)
                        for i in range(scan_limit):
                            a = raw_numbers[i]
                            b = raw_numbers[i + 1]
                            if float(int(a)) == a and float(int(b)) == b:
                                if int(a) * int(b) == expected_n:
                                    header_cut = i + 2
                                    header_pair = (int(a), int(b))
                                    break

                        if header_cut is not None:
                            raw_numbers = raw_numbers[header_cut:]
                            after_strip_n = len(raw_numbers)

                    if len(raw_numbers) != expected_n:
                        # 디버깅을 위해 일부 토큰만 요약해서 메시지에 포함
                        head_preview = raw_numbers[:10]
                        tail_preview = raw_numbers[-10:] if len(raw_numbers) > 10 else raw_numbers
                        raise ValueError(
                            "격자 응답 값 개수 불일치: "
                            f"parsed={original_n:,}, expected={expected_n:,}, "
                            f"after_strip={after_strip_n:,}. "
                            f"header_detected={header_pair if header_pair is not None else None}. "
                            f"values_head={head_preview}, values_tail={tail_preview}"
                        )

            # 결측값/특수 코드 처리
            # -999: 결측, 2049: 데이터 없음 등 특수 코드 처리
            values = []
            for val in raw_numbers:
                if val is None or (isinstance(val, float) and np.isnan(val)):
                    values.append(np.nan)
                    continue
                if val < -900 or val > 2000:
                    values.append(np.nan)
                else:
                    values.append(val)

            return np.array(values) if values else None
            
        except Exception as e:
            # 여기서 조용히 `None`을 반환하면 이후 단계에서 `grid_idx` 정합성 오류를 놓치기 쉬워집니다.
            # 반드시 예외를 전파해, 데이터/파싱 포맷 변경을 즉시 감지하도록 합니다.
            print(f"파싱 오류: {e}")
            raise


def run_fusion_pipeline(
    auth_key: str,
    start_year: int,
    end_year: int,
    variables: List[str] = None,
    config: Optional[FusionConfig] = None,
) -> List[str]:
    """
    융합기상정보 처리 실행 (편의 함수)
    
    Args:
        auth_key: API 인증키
        start_year: 시작 연도
        end_year: 종료 연도
        variables: 변수 목록 (기본: ['ta', 'rn_60m', 'sd_3hr'])
        config: 설정
        
    Returns:
        저장된 파일 경로 리스트
    """
    if variables is None:
        variables = ['ta', 'rn_60m', 'sd_3hr']
    
    pipeline = FusionPipeline(auth_key, config)
    return pipeline.process_year_range(start_year, end_year, variables)


if __name__ == "__main__":
    import dotenv
    dotenv.load_dotenv()
    
    auth_key = os.getenv("authKey")
    if not auth_key:
        raise ValueError("authKey를 .env 파일에 설정해주세요")
    
    # 테스트: 하루 데이터 처리
    pipeline = FusionPipeline(auth_key)
    
    # 매핑 테이블 생성
    pipeline.ensure_mapping()
    
    # 하루 처리 테스트
    result = pipeline.process_day("20240101", variables=['ta'])
    print(f"\n결과 샘플:")
    print(result.head())
