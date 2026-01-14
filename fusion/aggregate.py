"""
시간/공간 집계 모듈

- 시간 집계: 5분 → 1시간/3시간
- 공간 집계: 격자 → 법정동
"""

from typing import Optional, List, Dict, Literal
import numpy as np
import pandas as pd

from .config import FusionConfig, DEFAULT_CONFIG


class TimeAggregator:
    """시간 집계 클래스"""
    
    def __init__(self, config: Optional[FusionConfig] = None):
        self.config = config or DEFAULT_CONFIG
    
    def aggregate_5min_to_hourly(
        self,
        df: pd.DataFrame,
        var_col: str,
        time_col: str = 'datetime',
        method: Literal['mean', 'sum', 'last', 'max', 'min'] = 'mean',
        grid_col: str = 'grid_idx',
    ) -> pd.DataFrame:
        """
        5분 데이터를 1시간 단위로 집계
        
        Args:
            df: 입력 DataFrame (grid_idx, datetime, value)
            var_col: 집계할 변수 컬럼명
            time_col: 시간 컬럼명
            method: 집계 방법 (mean, sum, last, max, min)
            grid_col: 격자 인덱스 컬럼명
            
        Returns:
            시간별 집계된 DataFrame
        """
        df = df.copy()
        
        # datetime 파싱
        if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
            df[time_col] = pd.to_datetime(df[time_col])
        
        # 날짜와 시간 추출
        df['date'] = df[time_col].dt.date
        df['hour'] = df[time_col].dt.hour
        
        # 그룹별 집계
        group_cols = [grid_col, 'date', 'hour']
        
        if method == 'mean':
            result = df.groupby(group_cols)[var_col].mean().reset_index()
        elif method == 'sum':
            result = df.groupby(group_cols)[var_col].sum().reset_index()
        elif method == 'last':
            result = df.groupby(group_cols)[var_col].last().reset_index()
        elif method == 'max':
            result = df.groupby(group_cols)[var_col].max().reset_index()
        elif method == 'min':
            result = df.groupby(group_cols)[var_col].min().reset_index()
        else:
            raise ValueError(f"Unknown method: {method}")
        
        return result
    
    def aggregate_to_3hourly(
        self,
        df: pd.DataFrame,
        var_col: str,
        time_col: str = 'datetime',
        method: Literal['mean', 'sum', 'last', 'max', 'min'] = 'last',
        grid_col: str = 'grid_idx',
    ) -> pd.DataFrame:
        """
        데이터를 3시간 단위로 집계 (적설용)
        
        Args:
            df: 입력 DataFrame
            var_col: 집계할 변수 컬럼명
            time_col: 시간 컬럼명
            method: 집계 방법
            grid_col: 격자 인덱스 컬럼명
            
        Returns:
            3시간별 집계된 DataFrame
        """
        df = df.copy()
        
        if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
            df[time_col] = pd.to_datetime(df[time_col])
        
        df['date'] = df[time_col].dt.date
        df['hour'] = df[time_col].dt.hour
        # 3시간 구간: 0-2→0, 3-5→3, 6-8→6, ...
        df['hour_3h'] = (df['hour'] // 3) * 3
        
        group_cols = [grid_col, 'date', 'hour_3h']
        
        if method == 'mean':
            result = df.groupby(group_cols)[var_col].mean().reset_index()
        elif method == 'sum':
            result = df.groupby(group_cols)[var_col].sum().reset_index()
        elif method == 'last':
            result = df.groupby(group_cols)[var_col].last().reset_index()
        else:
            raise ValueError(f"Unknown method: {method}")
        
        result = result.rename(columns={'hour_3h': 'hour'})
        return result
    
    def pivot_hourly_to_columns(
        self,
        df: pd.DataFrame,
        var_col: str,
        col_prefix: str,
        grid_col: str = 'grid_idx',
        is_3hourly: bool = False,
    ) -> pd.DataFrame:
        """
        시간별 데이터를 컬럼으로 피벗
        
        Args:
            df: 입력 DataFrame (grid_idx, date, hour, value)
            var_col: 값 컬럼명
            col_prefix: 컬럼 접두어 (t, p, s)
            grid_col: 격자 인덱스 컬럼명
            is_3hourly: 3시간 단위 여부
            
        Returns:
            피벗된 DataFrame (grid_idx, date, t0001, t0102, ...)
        """
        df = df.copy()
        
        # 컬럼명 생성
        if is_3hourly:
            # s0003, s0306, ..., s2124
            df['col_name'] = df['hour'].apply(
                lambda h: f"{col_prefix}{h:02d}{h+3:02d}"
            )
        else:
            # t0001, t0102, ..., t2324
            df['col_name'] = df['hour'].apply(
                lambda h: f"{col_prefix}{h:02d}{(h+1) % 24:02d}"
            )
        
        # 피벗
        # 피벗 수행: 시간별 데이터를 컬럼으로 변환
        # - index: 각 행을 고유하게 식별하는 키 컬럼 (grid_idx: 격자 인덱스, date: 날짜)
        # - columns: 새로운 컬럼명이 될 값들 (col_name: 위에서 생성한 t0001, t0102 등)
        # - values: 각 셀에 채워질 실제 값 (var_col: ta, rn_60m, sd_3hr 등의 관측값)
        # - aggfunc='first': 동일한 (grid_idx, date, col_name) 조합이 중복 존재하면 첫 번째 값만 사용
        #   (정상적인 데이터에서는 중복이 없어야 하지만, 만약의 경우 대비)
        pivot_df = df.pivot_table(
            index=[grid_col, 'date'],  # 행 인덱스: (격자번호, 날짜)
            columns='col_name',  # 컬럼으로 펼칠 값: 시간대별 컬럼명
            values=var_col,  # 채워질 값: 관측 변수
            aggfunc='first',  # 중복 시 첫 번째 값 선택 (중복 방지용)
        ).reset_index()
        
        # 컬럼 순서 정렬
        if is_3hourly:
            col_order = [f"{col_prefix}{h*3:02d}{(h+1)*3:02d}" for h in range(8)]
        else:
            col_order = [f"{col_prefix}{h:02d}{(h+1) % 24:02d}" for h in range(24)]
        
        # 존재하는 컬럼만 선택
        existing_cols = [c for c in col_order if c in pivot_df.columns]
        result_cols = [grid_col, 'date'] + existing_cols
        
        return pivot_df[result_cols]


class SpatialAggregator:
    """공간 집계 클래스 (격자 → 법정동)"""
    
    def __init__(self, grid_mapping: pd.DataFrame, config: Optional[FusionConfig] = None):
        """
        Args:
            grid_mapping: 격자-법정동 매핑 테이블 (grid_idx, LAW_ID)
        """
        self.grid_mapping = grid_mapping
        self.config = config or DEFAULT_CONFIG
    
    def aggregate_grid_to_lawid(
        self,
        df: pd.DataFrame,
        value_cols: List[str],
        grid_col: str = 'grid_idx',
        method: Literal['mean', 'sum', 'median'] = 'mean',
    ) -> pd.DataFrame:
        """
        격자 데이터를 법정동 단위로 집계
        
        Args:
            df: 입력 DataFrame (grid_idx, date, value_cols...)
            value_cols: 집계할 값 컬럼 리스트
            grid_col: 격자 인덱스 컬럼명
            method: 집계 방법 (mean, sum, median)
            
        Returns:
            법정동별 집계된 DataFrame
        """
        # 매핑 조인
        df_with_lawid = df.merge(
            self.grid_mapping[[grid_col, 'LAW_ID']],
            on=grid_col,
            how='left',
        )
        
        # 매핑 실패 행 제거 (해양 등)
        df_with_lawid = df_with_lawid[df_with_lawid['LAW_ID'].notna()]

        # 강수/적설 관련 컬럼은 NaN을 0으로 처리 (데이터가 없는 곳은 현상이 없는 것으로 간주)
        # p: 강수량, s: 적설량
        # 기온(t) 등 다른 변수는 NaN을 유지하여 평균 계산 시 제외되도록 함
        precip_snow_cols = [c for c in value_cols if c.startswith('p') or c.startswith('s')]
        if precip_snow_cols:
            df_with_lawid[precip_snow_cols] = df_with_lawid[precip_snow_cols].fillna(0)
        
        # 기온(t) 컬럼에 0이 비정상적으로 발생하는지 확인하기 위해 
        # 집계 전 NaN 상태를 유지하는지 명시적으로 확인 (수정 불필요, 확인용 주석)
        
        # 그룹 컬럼 결정
        group_cols = ['date', 'LAW_ID']
        if 'date' not in df.columns:
            group_cols = ['LAW_ID']
        
        # 집계
        if method == 'mean':
            result = df_with_lawid.groupby(group_cols)[value_cols].mean().reset_index()
        elif method == 'sum':
            result = df_with_lawid.groupby(group_cols)[value_cols].sum().reset_index()
        elif method == 'median':
            result = df_with_lawid.groupby(group_cols)[value_cols].median().reset_index()
        else:
            raise ValueError(f"Unknown method: {method}")
        
        return result
    
    def get_lawid_statistics(self) -> pd.DataFrame:
        """법정동별 격자 수 통계"""
        stats = self.grid_mapping.groupby('LAW_ID').agg({
            'grid_idx': 'count',
            'lat': ['min', 'max', 'mean'],
            'lon': ['min', 'max', 'mean'],
        }).reset_index()
        
        stats.columns = ['LAW_ID', 'grid_count', 
                        'lat_min', 'lat_max', 'lat_mean',
                        'lon_min', 'lon_max', 'lon_mean']
        
        return stats


class OutputFormatter:
    """최종 출력 포맷 변환"""
    
    def __init__(self, config: Optional[FusionConfig] = None):
        self.config = config or DEFAULT_CONFIG
    
    def merge_variables(
        self,
        dfs: Dict[str, pd.DataFrame],
        index_cols: List[str] = ['date', 'LAW_ID'],
    ) -> pd.DataFrame:
        """
        여러 변수의 DataFrame을 하나로 병합
        
        Args:
            dfs: {변수명: DataFrame} 딕셔너리
            index_cols: 인덱스 컬럼 리스트
            
        Returns:
            병합된 DataFrame
        """
        if not dfs:
            return pd.DataFrame()
        
        # 첫 번째 df를 기준으로 병합
        result = None
        for var_name, df in dfs.items():
            if result is None:
                result = df
            else:
                result = result.merge(df, on=index_cols, how='outer')
        
        # 컬럼 순서 정렬: index_cols + t* + p* + s*
        sorted_cols = list(index_cols)
        
        # 기온 (t)
        t_cols = sorted([c for c in result.columns if c.startswith('t') and c not in index_cols])
        sorted_cols.extend(t_cols)
        
        # 강수량 (p)
        p_cols = sorted([c for c in result.columns if c.startswith('p') and c not in index_cols])
        sorted_cols.extend(p_cols)
        
        # 적설량 (s)
        s_cols = sorted([c for c in result.columns if c.startswith('s') and c not in index_cols])
        sorted_cols.extend(s_cols)
        
        # 나머지 컬럼
        other_cols = [c for c in result.columns if c not in sorted_cols]
        sorted_cols.extend(other_cols)
        
        return result[sorted_cols]
    
    def add_lawid_name(
        self,
        df: pd.DataFrame,
        grid_mapping: pd.DataFrame,
    ) -> pd.DataFrame:
        """법정동명 컬럼 추가"""
        lawid_names = grid_mapping[['LAW_ID', 'LAW_NM']].drop_duplicates()
        return df.merge(lawid_names, on='LAW_ID', how='left')
    
    def format_date_column(
        self,
        df: pd.DataFrame,
        date_col: str = 'date',
        format_str: str = '%Y-%m-%d',
    ) -> pd.DataFrame:
        """날짜 컬럼 포맷팅"""
        df = df.copy()
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col]).dt.strftime(format_str)
        return df


if __name__ == "__main__":
    # 테스트
    import numpy as np
    
    # 테스트 데이터 생성
    np.random.seed(42)
    n_grids = 100
    n_hours = 24
    
    # 격자별 시간별 데이터
    test_data = []
    for grid_idx in range(n_grids):
        for hour in range(n_hours):
            test_data.append({
                'grid_idx': grid_idx,
                'date': '2024-01-01',
                'hour': hour,
                'ta': 10 + np.random.randn() * 5,
            })
    
    df = pd.DataFrame(test_data)
    
    # 시간 집계 테스트
    time_agg = TimeAggregator()
    pivot_df = time_agg.pivot_hourly_to_columns(df, 'ta', 't')
    
    print("시간 집계 결과:")
    print(pivot_df.head())
    print(f"Shape: {pivot_df.shape}")
