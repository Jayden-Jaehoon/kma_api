"""fusion.geocode

격자(위경도) 좌표 → 법정동(읍면동) 코드 매핑 모듈

이 모듈의 목적
----------------
- `sfc_grid_latlon.nc`에 들어있는 격자점(각 격자의 중심 좌표)을 읽고
- 법정동(읍면동) 경계 Shapefile(`LSMD_ADM_SECT_UMD_*.shp`)과의 공간 조인(점-폴리곤)
  *Point-in-Polygon* 으로 "이 격자점이 어느 법정동 폴리곤 안에 들어가는지"를 찾아
- 격자 인덱스(`grid_idx`)별로 법정동 코드/명칭(`EMD_CD`, `EMD_NM`)을 붙인 테이블을
  Parquet로 저장합니다.

데이터 특징
-----------
- 전국 17개 시도별로 분리된 shapefile을 통합하여 사용
- 법정동(읍면동) 경계 사용: EMD_CD/EMD_NM 컬럼

데이터 흐름(요약)
-----------------
1) NetCDF에서 위경도 배열을 추출 → 1D/2D 구조에 맞게 펼쳐서(flatten) 격자점 목록 생성
2) 17개 시도별 Shapefile로 법정동 경계(폴리곤) 로드 후 병합
3) 격자점 → `GeoDataFrame`(Point geometry, `EPSG:4326`)
4) 법정동 폴리곤을 필요 시 `EPSG:4326`으로 재투영
5) `geopandas.sjoin(..., predicate='within')`로 공간 조인 수행

주의/가정
---------
- 격자점 좌표는 `EPSG:4326`(WGS84 경위도)라고 가정합니다.
- Shapefile은 `.prj`에 정의된 좌표계를 따르며, 다를 경우 `EPSG:4326`으로 변환합니다.
- 공간 조인은 `within`(점이 폴리곤 내부에 있을 때만 매칭) 조건을 사용합니다.
- 매핑 실패(`EMD_CD`가 `NaN`)는 주로 해양/북한/경계 밖 점일 가능성이 큽니다.
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


class GridToLawIdMapper:
    """격자 좌표를 법정동(읍면동) 코드로 매핑하는 클래스.

    이 클래스는 크게 두 가지 리소스를 다룹니다.

    - 격자 위경도: `FusionConfig.grid_latlon_nc` (NetCDF)
    - 법정동 폴리곤: `FusionConfig.geodata_umd_dir` 하위의 시도별 Shapefile

    결과로 생성되는 매핑 테이블은 `FusionConfig.grid_mapping_file`(Parquet)에 저장됩니다.

    캐시
    ----
    - `self._mapping_df`: 한 번 로드/생성한 매핑 테이블을 메모리에 캐시합니다.
    - `self._dong_gdf`: 한 번 로드한 법정동 GeoDataFrame을 캐시합니다.
    """

    def __init__(self, config: Optional[FusionConfig] = None):
        self.config = config or DEFAULT_CONFIG
        self._mapping_df: Optional[pd.DataFrame] = None
        self._dong_gdf: Optional[gpd.GeoDataFrame] = None

    def build_mapping(self, force_rebuild: bool = False) -> pd.DataFrame:
        """
        격자 → 법정동(읍면동) 매핑 테이블 생성

        Args:
            force_rebuild: True면 기존 파일 있어도 재생성

        Returns:
            매핑 테이블 DataFrame (grid_idx, lat, lon, EMD_CD, EMD_NM)
        """
        mapping_path = self.config.grid_mapping_file

        # 기존 매핑 파일이 있고 `force_rebuild=False`면, 비용이 큰 공간 조인을 다시 하지 않고
        # 저장된 결과(Parquet)를 그대로 재사용합니다.
        if not force_rebuild and os.path.exists(mapping_path):
            print(f"기존 법정동 매핑 파일 로드: {mapping_path}")
            self._mapping_df = pd.read_parquet(mapping_path)
            return self._mapping_df

        print("격자-법정동(읍면동) 매핑 테이블 생성 중...")

        # 1. 격자 좌표 로드
        print("  1/4. 격자 좌표 로드...")
        grid_df = self._load_grid_coordinates()
        print(f"       격자점 수: {len(grid_df):,}")

        # 2. 법정동 경계 로드 (17개 시도 통합)
        print("  2/4. 법정동(읍면동) 경계 로드 (17개 시도 통합)...")
        dong_gdf = self._load_legal_dong_umd()
        print(f"       법정동 수: {len(dong_gdf):,}")

        # 3. 격자점을 GeoDataFrame으로 변환
        print("  3/4. Spatial Join 수행 중...")
        grid_points = gpd.GeoDataFrame(
            grid_df,
            geometry=[Point(lon, lat) for lon, lat in zip(grid_df['lon'], grid_df['lat'])],
            crs='EPSG:4326'
        )

        # 4. Spatial Join으로 매핑
        # 공간 조인 전에 CRS(좌표계)를 반드시 맞춰야 합니다.
        if dong_gdf.crs != 'EPSG:4326':
            print(f"       좌표계 변환: {dong_gdf.crs} → EPSG:4326")
            dong_gdf = dong_gdf.to_crs('EPSG:4326')

        # Point-in-Polygon Join
        mapping = gpd.sjoin(grid_points, dong_gdf, how='left', predicate='within')

        # 필요한 컬럼만 선택
        # 법정동 데이터는 EMD_CD, EMD_NM 컬럼을 사용
        emd_cd_col = self._find_column(mapping, ['EMD_CD', 'ADM_CD', 'BJDONG_CD'])
        emd_nm_col = self._find_column(mapping, ['EMD_NM', 'ADM_NM', 'BJDONG_NM'])

        result_df = pd.DataFrame({
            'grid_idx': mapping['grid_idx'],
            'lat': mapping['lat'],
            'lon': mapping['lon'],
            'EMD_CD': mapping[emd_cd_col] if emd_cd_col else None,
            'EMD_NM': mapping[emd_nm_col] if emd_nm_col else None,
        })

        # 매핑 실패(법정동이 없는 점) 통계
        null_count = result_df['EMD_CD'].isna().sum()
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
        """저장된 매핑 테이블 로드.

        동작
        ----
        - 이미 `self._mapping_df`가 있으면(메모리 캐시) 즉시 반환
        - 없으면 `FusionConfig.grid_mapping_file`(Parquet)을 읽어 캐시에 저장 후 반환
        - 파일이 없으면 `build_mapping()`을 먼저 실행하도록 예외를 발생
        """
        if self._mapping_df is not None:
            return self._mapping_df

        mapping_path = self.config.grid_mapping_file
        if os.path.exists(mapping_path):
            self._mapping_df = pd.read_parquet(mapping_path)
            return self._mapping_df
        else:
            raise FileNotFoundError(
                f"법정동 매핑 파일이 없습니다: {mapping_path}\n"
                f"먼저 build_mapping()을 실행하세요."
            )

    def get_emdcd_for_grid(self, grid_idx: int) -> Optional[str]:
        """특정 격자점(`grid_idx`)의 법정동 코드(`EMD_CD`) 반환.

        참고
        ----
        - 매핑되지 않은 격자(해양 등)는 `None`을 반환합니다.
        - 내부적으로 `load_mapping()`을 호출하므로, 매핑 파일이 없으면 예외가 발생합니다.
        """
        mapping = self.load_mapping()
        row = mapping[mapping['grid_idx'] == grid_idx]
        if len(row) > 0:
            return row['EMD_CD'].values[0]
        return None

    def get_grids_in_emdcd(self, emd_cd: str) -> pd.DataFrame:
        """특정 법정동(`emd_cd`)에 속한 모든 격자점 반환.

        반환되는 DataFrame에는 원본 매핑 테이블의 컬럼(`grid_idx`, `lat`, `lon`, `EMD_CD`, `EMD_NM`)이
        그대로 포함됩니다.
        """
        mapping = self.load_mapping()
        return mapping[mapping['EMD_CD'] == emd_cd]

    def get_unique_emdcds(self) -> pd.DataFrame:
        """매핑된 모든 법정동 목록 반환.

        - `EMD_CD`, `EMD_NM`만 뽑아 중복 제거
        - `NaN`(미매칭) 행은 제거
        """
        mapping = self.load_mapping()
        return mapping[['EMD_CD', 'EMD_NM']].drop_duplicates().dropna()

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

    def _load_legal_dong_umd(self) -> gpd.GeoDataFrame:
        """법정동(읍면동) 경계 Shapefile 로드 (17개 시도 통합).

        `geodata_umd_dir` 하위의 모든 `.shp` 파일을 탐색하여 하나로 병합합니다.
        """
        umd_dir = self.config.geodata_umd_dir

        if not os.path.exists(umd_dir):
            raise FileNotFoundError(f"법정동 데이터 디렉토리가 없습니다: {umd_dir}")

        # 모든 .shp 파일 탐색
        shp_pattern = os.path.join(umd_dir, "**", "*.shp")
        shp_files = glob.glob(shp_pattern, recursive=True)

        if not shp_files:
            raise FileNotFoundError(f"법정동 shapefile을 찾을 수 없습니다: {umd_dir}")

        print(f"       발견된 shapefile: {len(shp_files)}개")

        # 각 shapefile을 로드하여 리스트에 저장
        gdf_list: List[gpd.GeoDataFrame] = []

        for shp_path in shp_files:
            # 인코딩 시도 (한글 처리)
            gdf = None
            for encoding in ['utf-8', 'cp949', 'euc-kr']:
                try:
                    gdf = gpd.read_file(shp_path, encoding=encoding)
                    break
                except Exception:
                    continue

            # 마지막 시도 (인코딩 없이)
            if gdf is None:
                try:
                    gdf = gpd.read_file(shp_path)
                except Exception as e:
                    print(f"       경고: {shp_path} 로드 실패 - {e}")
                    continue

            gdf_list.append(gdf)

        if not gdf_list:
            raise RuntimeError("모든 shapefile 로드에 실패했습니다.")

        # 모든 GeoDataFrame 병합
        combined_gdf = pd.concat(gdf_list, ignore_index=True)

        # GeoDataFrame으로 다시 변환 (concat 후 geometry 컬럼 유지)
        combined_gdf = gpd.GeoDataFrame(combined_gdf, geometry='geometry')

        # CRS 통일 (첫 번째 파일의 CRS 사용)
        if combined_gdf.crs is None and gdf_list[0].crs is not None:
            combined_gdf.set_crs(gdf_list[0].crs, inplace=True)

        self._dong_gdf = combined_gdf
        return combined_gdf

    @staticmethod
    def _find_variable(ds: xr.Dataset, candidates: list) -> Optional[str]:
        """`xarray.Dataset`에서 후보 변수명 중 존재하는 것 찾기.

        - 대소문자 차이를 흡수하기 위해 `lower()` 비교도 수행합니다.
        - 찾으면 실제로 존재하는 변수명을 그대로 반환합니다.
        """
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
        """`DataFrame`에서 후보 컬럼명 중 존재하는 것 찾기.

        Shapefile/공간조인 결과의 스키마가 조금씩 달라도 동작하도록,
        여러 후보 이름 중 실제 존재하는 첫 번째 컬럼명을 반환합니다.
        """
        for name in candidates:
            if name in df.columns:
                return name
        return None


def build_grid_mapping(config: Optional[FusionConfig] = None, force: bool = False) -> pd.DataFrame:
    """격자-법정동(읍면동) 매핑 테이블 생성 (편의 함수).

    스크립트/노트북 등에서 클래스를 직접 만들지 않고도 한 줄로 실행하기 위한 래퍼입니다.

    Args:
        config: `FusionConfig` (생략 시 `DEFAULT_CONFIG` 사용)
        force: True면 기존 매핑 파일이 있어도 재생성
    """
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
    print(f"  총 법정동 수: {mapping['EMD_CD'].nunique():,}")
    print(f"  법정동당 평균 격자 수: {len(mapping) / mapping['EMD_CD'].nunique():.1f}")
