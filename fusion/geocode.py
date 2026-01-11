"""
격자 좌표 → 법정동 매핑 모듈

격자 중심점(centroid)이 속한 법정동에 할당하는 Point-in-Polygon 방식 사용
"""

import os
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import xarray as xr

from .config import FusionConfig, DEFAULT_CONFIG


class GridToLawIdMapper:
    """격자 좌표를 법정동 코드로 매핑하는 클래스"""
    
    def __init__(self, config: Optional[FusionConfig] = None):
        self.config = config or DEFAULT_CONFIG
        self._mapping_df: Optional[pd.DataFrame] = None
        self._dong_gdf: Optional[gpd.GeoDataFrame] = None
    
    def build_mapping(self, force_rebuild: bool = False) -> pd.DataFrame:
        """
        격자 → 법정동 매핑 테이블 생성
        
        Args:
            force_rebuild: True면 기존 파일 있어도 재생성
            
        Returns:
            매핑 테이블 DataFrame (grid_idx, lat, lon, LAW_ID, LAW_NM)
        """
        mapping_path = self.config.grid_mapping_file
        
        # 기존 매핑 파일 확인
        if not force_rebuild and os.path.exists(mapping_path):
            print(f"기존 매핑 파일 로드: {mapping_path}")
            self._mapping_df = pd.read_parquet(mapping_path)
            return self._mapping_df
        
        print("격자-법정동 매핑 테이블 생성 중...")
        
        # 1. 격자 좌표 로드
        print("  1/4. 격자 좌표 로드...")
        grid_df = self._load_grid_coordinates()
        print(f"       격자점 수: {len(grid_df):,}")
        
        # 2. 법정동 경계 로드
        print("  2/4. 법정동 경계 로드...")
        dong_gdf = self._load_legal_dong()
        print(f"       법정동 수: {len(dong_gdf):,}")
        
        # 3. 격자점을 GeoDataFrame으로 변환
        print("  3/4. Spatial Join 수행 중...")
        grid_points = gpd.GeoDataFrame(
            grid_df,
            geometry=[Point(lon, lat) for lon, lat in zip(grid_df['lon'], grid_df['lat'])],
            crs='EPSG:4326'
        )
        
        # 4. Spatial Join으로 매핑
        # 법정동 좌표계를 WGS84로 변환 (필요시)
        if dong_gdf.crs != 'EPSG:4326':
            print(f"       좌표계 변환: {dong_gdf.crs} → EPSG:4326")
            dong_gdf = dong_gdf.to_crs('EPSG:4326')
        
        # Point-in-Polygon Join
        mapping = gpd.sjoin(grid_points, dong_gdf, how='left', predicate='within')
        
        # 필요한 컬럼만 선택
        # 법정동 코드/명칭 컬럼명은 파일마다 다를 수 있으므로 확인
        law_id_col = self._find_column(mapping, ['ADM_DR_CD', 'ADM_CD', 'EMD_CD', 'BJDONG_CD', 'LAW_ID'])
        law_nm_col = self._find_column(mapping, ['ADM_DR_NM', 'ADM_NM', 'EMD_NM', 'BJDONG_NM', 'LAW_NM'])
        
        result_df = pd.DataFrame({
            'grid_idx': mapping['grid_idx'],
            'lat': mapping['lat'],
            'lon': mapping['lon'],
            'LAW_ID': mapping[law_id_col] if law_id_col else None,
            'LAW_NM': mapping[law_nm_col] if law_nm_col else None,
        })
        
        # 매핑 실패 (해양 등) 통계
        null_count = result_df['LAW_ID'].isna().sum()
        print(f"  4/4. 매핑 완료")
        print(f"       매핑 성공: {len(result_df) - null_count:,}")
        print(f"       매핑 실패 (해양/북한 등): {null_count:,}")
        
        # 저장
        os.makedirs(os.path.dirname(mapping_path), exist_ok=True)
        result_df.to_parquet(mapping_path, index=False)
        print(f"       저장 완료: {mapping_path}")
        
        self._mapping_df = result_df
        return result_df
    
    def load_mapping(self) -> pd.DataFrame:
        """저장된 매핑 테이블 로드"""
        if self._mapping_df is not None:
            return self._mapping_df
        
        mapping_path = self.config.grid_mapping_file
        if os.path.exists(mapping_path):
            self._mapping_df = pd.read_parquet(mapping_path)
            return self._mapping_df
        else:
            raise FileNotFoundError(
                f"매핑 파일이 없습니다: {mapping_path}\n"
                f"먼저 build_mapping()을 실행하세요."
            )
    
    def get_lawid_for_grid(self, grid_idx: int) -> Optional[str]:
        """특정 격자점의 법정동 코드 반환"""
        mapping = self.load_mapping()
        row = mapping[mapping['grid_idx'] == grid_idx]
        if len(row) > 0:
            return row['LAW_ID'].values[0]
        return None
    
    def get_grids_in_lawid(self, law_id: str) -> pd.DataFrame:
        """특정 법정동에 속한 모든 격자점 반환"""
        mapping = self.load_mapping()
        return mapping[mapping['LAW_ID'] == law_id]
    
    def get_unique_lawids(self) -> pd.DataFrame:
        """매핑된 모든 법정동 목록 반환"""
        mapping = self.load_mapping()
        return mapping[['LAW_ID', 'LAW_NM']].drop_duplicates().dropna()
    
    def _load_grid_coordinates(self) -> pd.DataFrame:
        """격자 좌표 NetCDF 파일 로드"""
        nc_path = self.config.grid_latlon_nc
        
        with xr.open_dataset(nc_path) as ds:
            # NetCDF 구조 확인
            lat_var = self._find_variable(ds, ['lat', 'latitude', 'LAT'])
            lon_var = self._find_variable(ds, ['lon', 'longitude', 'LON'])
            
            if lat_var is None or lon_var is None:
                # 변수가 없으면 좌표로 시도
                if 'lat' in ds.coords and 'lon' in ds.coords:
                    lat_data = ds.coords['lat'].values
                    lon_data = ds.coords['lon'].values
                else:
                    raise ValueError(f"위경도 변수를 찾을 수 없습니다. 변수 목록: {list(ds.data_vars)} / 좌표: {list(ds.coords)}")
            else:
                lat_data = ds[lat_var].values
                lon_data = ds[lon_var].values
            
            # 1D 또는 2D 배열 처리
            if lat_data.ndim == 2:
                # 2D 격자: flatten
                lat_flat = lat_data.flatten()
                lon_flat = lon_data.flatten()
            else:
                # 1D 좌표: meshgrid 생성
                lon_grid, lat_grid = np.meshgrid(lon_data, lat_data)
                lat_flat = lat_grid.flatten()
                lon_flat = lon_grid.flatten()
            
            grid_df = pd.DataFrame({
                'grid_idx': range(len(lat_flat)),
                'lat': lat_flat,
                'lon': lon_flat,
            })
            
            return grid_df
    
    def _load_legal_dong(self) -> gpd.GeoDataFrame:
        """법정동 경계 Shapefile 로드"""
        shp_path = self.config.legal_dong_shp
        
        # 인코딩 시도 (한글 처리)
        for encoding in ['utf-8', 'cp949', 'euc-kr']:
            try:
                gdf = gpd.read_file(shp_path, encoding=encoding)
                self._dong_gdf = gdf
                return gdf
            except Exception:
                continue
        
        # 마지막 시도 (인코딩 없이)
        gdf = gpd.read_file(shp_path)
        self._dong_gdf = gdf
        return gdf
    
    @staticmethod
    def _find_variable(ds: xr.Dataset, candidates: list) -> Optional[str]:
        """데이터셋에서 후보 변수명 중 존재하는 것 찾기"""
        for name in candidates:
            if name in ds.data_vars:
                return name
            if name.lower() in [v.lower() for v in ds.data_vars]:
                for v in ds.data_vars:
                    if v.lower() == name.lower():
                        return v
        return None
    
    @staticmethod
    def _find_column(df: pd.DataFrame, candidates: list) -> Optional[str]:
        """DataFrame에서 후보 컬럼명 중 존재하는 것 찾기"""
        for name in candidates:
            if name in df.columns:
                return name
        return None


def build_grid_mapping(config: Optional[FusionConfig] = None, force: bool = False) -> pd.DataFrame:
    """격자-법정동 매핑 테이블 생성 (편의 함수)"""
    mapper = GridToLawIdMapper(config)
    return mapper.build_mapping(force_rebuild=force)


if __name__ == "__main__":
    # 테스트 실행
    mapper = GridToLawIdMapper()
    mapping = mapper.build_mapping(force_rebuild=True)
    
    print("\n" + "="*60)
    print("매핑 결과 샘플:")
    print(mapping.head(20))
    
    print("\n법정동 통계:")
    print(f"  총 법정동 수: {mapping['LAW_ID'].nunique():,}")
    print(f"  법정동당 평균 격자 수: {len(mapping) / mapping['LAW_ID'].nunique():.1f}")
