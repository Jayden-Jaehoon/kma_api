"""
시간/공간 집계 모듈

- 시간 집계: 시간별 데이터를 컬럼으로 피벗
- 공간 집계: 격자 → 행정동
"""

from typing import Optional, List, Dict, Literal
import numpy as np
import pandas as pd

from .config import FusionConfig, DEFAULT_CONFIG


class TimeAggregator:
    """시간 집계 클래스"""
    
    def __init__(self, config: Optional[FusionConfig] = None):
        self.config = config or DEFAULT_CONFIG
    
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
    """공간 집계 클래스 (격자 → 행정동)"""
    
    def __init__(self, grid_mapping: pd.DataFrame, config: Optional[FusionConfig] = None):
        """
        Args:
            grid_mapping: 격자-행정동코드 매핑 테이블 (grid_idx, HJD_CD)
        """
        self.grid_mapping = grid_mapping
        self.config = config or DEFAULT_CONFIG
        
        # 매핑 테이블에서 지역 코드 컬럼 자동 감지 (HJD 또는 BJD)
        if 'HJD_CD' in grid_mapping.columns:
            self.id_col = 'HJD_CD'
            self.nm_col = 'HJD_NM'
        elif 'EMD_CD' in grid_mapping.columns:
            self.id_col = 'EMD_CD'
            self.nm_col = 'EMD_NM'
        else:
            raise ValueError("매핑 테이블에서 지역 코드 컬럼(HJD_CD 또는 EMD_CD)을 찾을 수 없습니다.")
    
    def aggregate_grid_to_region(
        self,
        df: pd.DataFrame,
        value_cols: List[str],
        grid_col: str = 'grid_idx',
        method: Literal['mean', 'sum', 'median'] = 'mean',
    ) -> pd.DataFrame:
        """
        격자 데이터를 지역 단위로 집계
        
        Args:
            df: 입력 DataFrame (grid_idx, date, value_cols...)
            value_cols: 집계할 값 컬럼 리스트
            grid_col: 격자 인덱스 컬럼명
            method: 집계 방법 (mean, sum, median)
            
        Returns:
            지역별 집계된 DataFrame
        """
        # 매핑 조인
        df_with_id = df.merge(
            self.grid_mapping[[grid_col, self.id_col]],
            on=grid_col,
            how='left',
        )
        
        # 매핑 실패 행 제거 (해양 등)
        df_with_id = df_with_id[df_with_id[self.id_col].notna()]

        # 강수/적설 관련 컬럼의 NaN 처리 정책:
        # - sum 집계: NaN을 0으로 처리 (누적 합산 시 "관측 없음 = 0"으로 간주)
        # - mean/median 집계: NaN 유지 (pandas groupby가 자동으로 NaN을 제외하고 평균 계산)
        #   → fillna(0)을 하면 실제 관측이 없는 격자가 0으로 취급되어 평균이 낮아지는 문제 방지
        if method == 'sum':
            precip_snow_cols = [c for c in value_cols if c.startswith('p') or c.startswith('s')]
            if precip_snow_cols:
                df_with_id[precip_snow_cols] = df_with_id[precip_snow_cols].fillna(0)
        
        # 그룹 컬럼 결정
        group_cols = ['date', self.id_col]
        if 'date' not in df.columns:
            group_cols = [self.id_col]
        
        # 집계
        if method == 'mean':
            result = df_with_id.groupby(group_cols)[value_cols].mean().reset_index()
        elif method == 'sum':
            result = df_with_id.groupby(group_cols)[value_cols].sum().reset_index()
        elif method == 'median':
            result = df_with_id.groupby(group_cols)[value_cols].median().reset_index()
        else:
            raise ValueError(f"Unknown method: {method}")
        
        return result
    
    def get_region_statistics(self) -> pd.DataFrame:
        """지역별 격자 수 통계"""
        stats = self.grid_mapping.groupby(self.id_col).agg({
            'grid_idx': 'count',
            'lat': ['min', 'max', 'mean'],
            'lon': ['min', 'max', 'mean'],
        }).reset_index()
        
        stats.columns = [self.id_col, 'grid_count', 
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
        index_cols: Optional[List[str]] = None,
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

        # index_cols 자동 감지: date + 지역코드 컬럼
        if index_cols is None:
            first_df = next(iter(dfs.values()))
            for col in ['HJD_CD', 'EMD_CD']:
                if col in first_df.columns:
                    index_cols = ['date', col]
                    break
            if index_cols is None:
                index_cols = ['date']

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
    
    def add_region_name(
        self,
        df: pd.DataFrame,
        grid_mapping: pd.DataFrame,
        id_col: str,
        nm_col: str,
    ) -> pd.DataFrame:
        """지역명 컬럼 추가 (행정동/법정동 공용)"""
        names = grid_mapping[[id_col, nm_col]].drop_duplicates()
        return df.merge(names, on=id_col, how='left')
    
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
