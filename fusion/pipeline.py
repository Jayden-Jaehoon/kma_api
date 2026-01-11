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
    
    def ensure_mapping(self, force_rebuild: bool = False):
        """격자-법정동 매핑 테이블 확보"""
        if self._grid_mapping is None or force_rebuild:
            self._grid_mapping = self.mapper.build_mapping(force_rebuild=force_rebuild)
            self._spatial_agg = SpatialAggregator(self._grid_mapping, self.config)
    
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
        
        all_data = []
        
        for hour in hours:
            tm = f"{date}{hour:02d}00"
            
            # API 호출
            response = self.downloader.download_hour_all_grid(tm, var, save_dir=None, disp='A')
            
            if response:
                # 응답 파싱
                grid_values = self._parse_grid_response(response)
                
                if grid_values is not None and len(grid_values) > 0:
                    df_hour = pd.DataFrame({
                        'grid_idx': range(len(grid_values)),
                        'date': date,
                        'hour': hour,
                        'value': grid_values,
                    })
                    all_data.append(df_hour)
            
            # API 호출 간격
            time.sleep(self.config.api_sleep_seconds)
        
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
            
            # 모든 값 추출
            values = []
            for line in data_lines:
                parts = line.replace(',', ' ').split()
                for p in parts:
                    try:
                        val = float(p)
                        # 결측값 처리
                        # -999: 결측, 2049: 데이터 없음 등 특수 코드 처리
                        if val < -900 or val > 2000:
                            val = np.nan
                        values.append(val)
                    except ValueError:
                        continue
            
            return np.array(values) if values else None
            
        except Exception as e:
            print(f"파싱 오류: {e}")
            return None


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
