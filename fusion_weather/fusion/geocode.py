"""fusion.geocode

격자(위경도) 좌표 → 행정동 코드 매핑 모듈

이 모듈의 목적
----------------
- `sfc_grid_latlon.nc`에 들어있는 격자점(각 격자의 중심 좌표)을 읽고
- 행정동 경계 Shapefile(`bnd_dong_00_2022_4Q.shp`)과의 공간 조인(점-폴리곤)
  *Point-in-Polygon* 으로 "이 격자점이 어느 행정동 폴리곤 안에 들어가는지"를 찾아
- 격자 인덱스(`grid_idx`)별로 행정동 코드/명칭(`HJD_CD`, `HJD_NM`)을 붙인 테이블을
  Parquet로 저장합니다.

데이터 특징
-----------
- 2022년 4분기 행정동 Shapefile을 base year로 사용 (BC카드 연구 기간 기준)
- `bnd_dong_00_2022_4Q` 폴더의 shapefile만 사용 (시도/시군구 제외)

데이터 흐름(요약)
-----------------
1) NetCDF에서 위경도 배열을 추출 → 1D/2D 구조에 맞게 펼쳐서(flatten) 격자점 목록 생성
2) 행정동 Shapefile로 행정동 경계(폴리곤) 로드
3) 격자점 → `GeoDataFrame`(Point geometry, `EPSG:4326`)
4) 행정동 폴리곤을 필요 시 `EPSG:4326`으로 재투영
5) `geopandas.sjoin(..., predicate='within')`로 공간 조인 수행

주의/가정
---------
- 격자점 좌표는 `EPSG:4326`(WGS84 경위도)라고 가정합니다.
- Shapefile은 `.prj`에 정의된 좌표계를 따르며, 다를 경우 `EPSG:4326`으로 변환합니다.
- 공간 조인은 `within`(점이 폴리곤 내부에 있을 때만 매칭) 조건을 사용합니다.
- 매핑 실패(`HJD_CD`가 `NaN`)는 주로 해양/북한/경계 밖 점일 가능성이 큽니다.
"""

import os
import glob
from typing import Optional, List

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import xarray as xr

from .config import FusionConfig, DEFAULT_CONFIG


class GridToHjdMapper:
    """격자 좌표를 행정동 코드로 매핑하는 클래스.

    이 클래스는 크게 두 가지 리소스를 다룹니다.

    - 격자 위경도: `FusionConfig.grid_latlon_nc` (NetCDF)
    - 행정동 폴리곤: `FusionConfig.geodata_hjd_dir` 하위의 Shapefile

    결과로 생성되는 매핑 테이블은 `FusionConfig.grid_hjd_mapping_file`(Parquet)에 저장됩니다.

    캐시
    ----
    - `self._hjd_mapping_df`: 한 번 로드/생성한 매핑 테이블을 메모리에 캐시합니다.
    - `self._hjd_gdf`: 한 번 로드한 행정동 GeoDataFrame을 캐시합니다.
    """

    def __init__(self, config: Optional[FusionConfig] = None):
        self.config = config or DEFAULT_CONFIG
        self._hjd_mapping_df: Optional[pd.DataFrame] = None
        self._hjd_gdf: Optional[gpd.GeoDataFrame] = None

    def build_hjd_mapping(self, force_rebuild: bool = False) -> pd.DataFrame:
        """
        격자 → 행정동 매핑 테이블 생성

        Args:
            force_rebuild: True면 기존 파일 있어도 재생성

        Returns:
            매핑 테이블 DataFrame (grid_idx, lat, lon, HJD_CD, HJD_NM)
        """
        mapping_path = self.config.grid_hjd_mapping_file

        if not force_rebuild and os.path.exists(mapping_path):
            print(f"기존 행정동 매핑 파일 로드: {mapping_path}")
            self._hjd_mapping_df = pd.read_parquet(mapping_path)
            return self._hjd_mapping_df

        print("격자-행정동 매핑 테이블 생성 중...")

        # 1. 격자 좌표 로드
        grid_df = self._load_grid_coordinates()

        # 2. 행정동 경계 로드
        hjd_gdf = self._load_hjd_shapefile()
        print(f"       행정동 수: {len(hjd_gdf):,}")

        # 3. 격자점을 GeoDataFrame으로 변환
        grid_points = gpd.GeoDataFrame(
            grid_df,
            geometry=[Point(lon, lat) for lon, lat in zip(grid_df['lon'], grid_df['lat'])],
            crs='EPSG:4326'
        )

        # 4. Spatial Join으로 매핑
        if hjd_gdf.crs != 'EPSG:4326':
            print(f"       좌표계 변환: {hjd_gdf.crs} → EPSG:4326")
            hjd_gdf = hjd_gdf.to_crs('EPSG:4326')

        mapping = gpd.sjoin(grid_points, hjd_gdf, how='left', predicate='within')

        # 행정동 데이터 컬럼 매핑
        # bnd_dong shapefile: adm_cd/adm_nm (소문자)
        # 기타 shapefile: ADSTRD_CD/ADSTRD_NM, ADM_CD/ADM_NM 등
        hjd_cd_col = self._find_column(mapping, ['adm_cd', 'ADSTRD_CD', 'HJD_CD', 'ADM_CD', 'ADMD_CD'])
        hjd_nm_col = self._find_column(mapping, ['adm_nm', 'ADSTRD_NM', 'HJD_NM', 'ADM_NM', 'ADMD_NM'])

        if hjd_cd_col is None or hjd_nm_col is None:
            available_cols = [c for c in mapping.columns if c not in ['grid_idx', 'lat', 'lon', 'geometry', 'index_right']]
            print(f"       경고: 행정동 코드/명칭 컬럼을 찾지 못했습니다.")
            print(f"       사용 가능한 컬럼: {available_cols}")
            print(f"       찾은 코드 컬럼: {hjd_cd_col}, 명칭 컬럼: {hjd_nm_col}")

        result_df = pd.DataFrame({
            'grid_idx': mapping['grid_idx'],
            'lat': mapping['lat'],
            'lon': mapping['lon'],
            'HJD_CD': mapping[hjd_cd_col] if hjd_cd_col else None,
            'HJD_NM': mapping[hjd_nm_col] if hjd_nm_col else None,
        })

        null_count = result_df['HJD_CD'].isna().sum()
        print(f"       매핑 성공: {len(result_df) - null_count:,}")
        print(f"       매핑 실패: {null_count:,}")

        os.makedirs(os.path.dirname(mapping_path), exist_ok=True)
        result_df.to_parquet(mapping_path, index=False)
        print(f"       저장 완료: {mapping_path}")

        self._hjd_mapping_df = result_df
        return result_df

    def load_hjd_mapping(self) -> pd.DataFrame:
        """저장된 행정동 매핑 테이블 로드."""
        if self._hjd_mapping_df is not None:
            return self._hjd_mapping_df

        mapping_path = self.config.grid_hjd_mapping_file
        if os.path.exists(mapping_path):
            self._hjd_mapping_df = pd.read_parquet(mapping_path)
            return self._hjd_mapping_df
        else:
            raise FileNotFoundError(
                f"행정동 매핑 파일이 없습니다: {mapping_path}\n"
                f"먼저 build_hjd_mapping()을 실행하세요."
            )

    def get_hjdcd_for_grid(self, grid_idx: int) -> Optional[str]:
        """특정 격자점(`grid_idx`)의 행정동 코드(`HJD_CD`) 반환."""
        mapping = self.load_hjd_mapping()
        row = mapping[mapping['grid_idx'] == grid_idx]
        if len(row) > 0:
            return row['HJD_CD'].values[0]
        return None

    def get_grids_in_hjdcd(self, hjd_cd: str) -> pd.DataFrame:
        """특정 행정동(`hjd_cd`)에 속한 모든 격자점 반환."""
        mapping = self.load_hjd_mapping()
        return mapping[mapping['HJD_CD'] == hjd_cd]

    def get_unique_hjdcds(self) -> pd.DataFrame:
        """매핑된 모든 행정동 목록 반환."""
        mapping = self.load_hjd_mapping()
        return mapping[['HJD_CD', 'HJD_NM']].drop_duplicates().dropna()

    def _load_grid_coordinates(self) -> pd.DataFrame:
        """격자 좌표 NetCDF 파일 로드.

        반환
        ----
        - `grid_idx`: 0..N-1 순번(행 인덱스와 독립적인 "격자점 ID")
        - `lat`, `lon`: 각 격자점의 위도/경도
        """
        nc_path = self.config.grid_latlon_nc

        with xr.open_dataset(nc_path) as ds:
            # NetCDF 구조 확인
            lat_var = self._find_variable(ds, ['lat', 'latitude', 'LAT'])
            lon_var = self._find_variable(ds, ['lon', 'longitude', 'LON'])

            if lat_var is None or lon_var is None:
                # 변수가 없으면 좌표(coords)로 시도
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

    def _load_hjd_shapefile(self) -> gpd.GeoDataFrame:
        """행정동(HJD) 경계 Shapefile 로드.

        파일 구조 예시 (2022년 4분기 기준):
        -----------------------------------------
        data/geodata_hjd/
        ├── bnd_dong_00_2022_4Q/        ← 행정동 (이 폴더만 사용)
        │   ├── bnd_dong_00_2022_4Q.shp
        │   ├── bnd_dong_00_2022_4Q.shx
        │   ├── bnd_dong_00_2022_4Q.dbf
        │   ├── bnd_dong_00_2022_4Q.cpg
        │   └── bnd_dong_00_2022_4Q.prj
        ├── bnd_sigungu_00_2022_4Q/     (시군구 – 사용하지 않음)
        └── bnd_sido_00_2022_4Q/        (시도 – 사용하지 않음)
        """
        hjd_dir = self.config.geodata_hjd_dir

        if not os.path.exists(hjd_dir):
            raise FileNotFoundError(f"행정동 데이터 디렉토리가 없습니다: {hjd_dir}")

        # bnd_dong 폴더 하위의 .shp 파일만 검색 (시도/시군구 shapefile 제외)
        shp_files = glob.glob(os.path.join(hjd_dir, "bnd_dong*", "*.shp"))

        # bnd_dong 패턴이 없으면 전체 .shp 재귀 탐색으로 폴백
        if not shp_files:
            shp_files = glob.glob(os.path.join(hjd_dir, "**", "*.shp"), recursive=True)

        if not shp_files:
            raise FileNotFoundError(
                f"행정동 shapefile을 찾을 수 없습니다: {hjd_dir}\n"
                f"bnd_dong_00_2022_4Q 형식의 폴더가 있는지 확인하세요."
            )

        print(f"       발견된 행정동 shapefile: {len(shp_files)}개")
        for f in shp_files:
            print(f"         - {os.path.basename(f)}")

        gdf_list = []
        for shp_path in shp_files:
            gdf = None
            for encoding in ['utf-8', 'cp949', 'euc-kr']:
                try:
                    gdf = gpd.read_file(shp_path, encoding=encoding)
                    break
                except Exception:
                    continue

            if gdf is None:
                try:
                    gdf = gpd.read_file(shp_path)
                except Exception as e:
                    print(f"       경고: {shp_path} 로드 실패 - {e}")
                    continue

            if gdf is not None:
                gdf_list.append(gdf)

        if not gdf_list:
            raise RuntimeError("행정동 shapefile 로드에 실패했습니다.")

        combined_gdf = pd.concat(gdf_list, ignore_index=True)
        combined_gdf = gpd.GeoDataFrame(combined_gdf, geometry='geometry')

        if combined_gdf.crs is None and gdf_list[0].crs is not None:
            combined_gdf.set_crs(gdf_list[0].crs, inplace=True)

        self._hjd_gdf = combined_gdf
        return combined_gdf

    @staticmethod
    def _find_variable(ds: xr.Dataset, candidates: list) -> Optional[str]:
        """`xarray.Dataset`에서 후보 변수명 중 존재하는 것 찾기."""
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
        """`DataFrame`에서 후보 컬럼명 중 존재하는 것 찾기."""
        for name in candidates:
            if name in df.columns:
                return name
        return None


if __name__ == "__main__":
    # 테스트 실행
    mapper = GridToHjdMapper()
    mapping = mapper.build_hjd_mapping(force_rebuild=True)

    print("\n" + "="*60)
    print("매핑 결과 샘플:")
    print(mapping.head(20))

    print("\n행정동 통계:")
    print(f"  총 행정동 수: {mapping['HJD_CD'].nunique():,}")
    print(f"  행정동당 평균 격자 수: {len(mapping) / mapping['HJD_CD'].nunique():.1f}")
