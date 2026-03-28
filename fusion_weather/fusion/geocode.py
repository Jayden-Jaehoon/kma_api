"""fusion.geocode

격자(위경도) 좌표 → 행정구역 코드 매핑 모듈

지원하는 매핑 유형
-----------------
1. **행정동(HJD)**: `bnd_dong_00_2022_4Q.shp` → `HJD_CD`, `HJD_NM`
2. **법정동(BJD/UMD)**: `LSMD_ADM_SECT_UMD_*.shp` (17개 시도) → `EMD_CD`, `EMD_NM`

데이터 흐름(공통)
-----------------
1) NetCDF에서 위경도 배열을 추출 → flatten → 격자점 목록 생성
2) Shapefile로 경계(폴리곤) 로드
3) 격자점 → `GeoDataFrame`(Point geometry, `EPSG:4326`)
4) `geopandas.sjoin(..., predicate='within')`로 공간 조인 수행
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


# ── 공통 유틸리티 ──────────────────────────────────────────────


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


def _find_column(df: pd.DataFrame, candidates: list) -> Optional[str]:
    """`DataFrame`에서 후보 컬럼명 중 존재하는 것 찾기."""
    for name in candidates:
        if name in df.columns:
            return name
    return None


def _load_grid_coordinates(config: FusionConfig) -> pd.DataFrame:
    """격자 좌표 NetCDF 파일 로드.

    반환: DataFrame (grid_idx, lat, lon)
    """
    nc_path = config.grid_latlon_nc

    with xr.open_dataset(nc_path) as ds:
        lat_var = _find_variable(ds, ['lat', 'latitude', 'LAT'])
        lon_var = _find_variable(ds, ['lon', 'longitude', 'LON'])

        if lat_var is None or lon_var is None:
            if 'lat' in ds.coords and 'lon' in ds.coords:
                lat_data = ds.coords['lat'].values
                lon_data = ds.coords['lon'].values
            else:
                raise ValueError(
                    f"위경도 변수를 찾을 수 없습니다. "
                    f"변수 목록: {list(ds.data_vars)} / 좌표: {list(ds.coords)}"
                )
        else:
            lat_data = ds[lat_var].values
            lon_data = ds[lon_var].values

        if lat_data.ndim == 2:
            lat_flat = lat_data.flatten()
            lon_flat = lon_data.flatten()
        else:
            lon_grid, lat_grid = np.meshgrid(lon_data, lat_data)
            lat_flat = lat_grid.flatten()
            lon_flat = lon_grid.flatten()

        return pd.DataFrame({
            'grid_idx': range(len(lat_flat)),
            'lat': lat_flat,
            'lon': lon_flat,
        })


def _load_shapefiles(
    shp_dir: str,
    glob_pattern: str,
    label: str,
) -> gpd.GeoDataFrame:
    """디렉토리에서 shapefile을 로드하고 병합.

    Args:
        shp_dir: shapefile 디렉토리
        glob_pattern: glob 검색 패턴 (예: "bnd_dong*/*.shp")
        label: 로그 출력용 라벨 (예: "행정동", "법정동")
    """
    if not os.path.exists(shp_dir):
        raise FileNotFoundError(f"{label} 데이터 디렉토리가 없습니다: {shp_dir}")

    shp_files = glob.glob(os.path.join(shp_dir, glob_pattern))

    # 패턴 매칭 실패 시 전체 .shp 재귀 탐색으로 폴백
    if not shp_files:
        shp_files = glob.glob(os.path.join(shp_dir, "**", "*.shp"), recursive=True)

    if not shp_files:
        raise FileNotFoundError(f"{label} shapefile을 찾을 수 없습니다: {shp_dir}")

    print(f"       발견된 {label} shapefile: {len(shp_files)}개")
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
        raise RuntimeError(f"{label} shapefile 로드에 실패했습니다.")

    combined_gdf = pd.concat(gdf_list, ignore_index=True)
    combined_gdf = gpd.GeoDataFrame(combined_gdf, geometry='geometry')

    if combined_gdf.crs is None and gdf_list[0].crs is not None:
        combined_gdf.set_crs(gdf_list[0].crs, inplace=True)

    return combined_gdf


def _build_mapping(
    config: FusionConfig,
    polygon_gdf: gpd.GeoDataFrame,
    cd_candidates: List[str],
    nm_candidates: List[str],
    cd_out: str,
    nm_out: str,
    mapping_path: str,
    label: str,
    force_rebuild: bool = False,
) -> pd.DataFrame:
    """격자→지역 매핑 테이블을 생성하는 공통 로직.

    Args:
        config: FusionConfig
        polygon_gdf: 지역 경계 GeoDataFrame
        cd_candidates: 코드 컬럼 후보 리스트
        nm_candidates: 명칭 컬럼 후보 리스트
        cd_out: 출력 코드 컬럼명 (예: 'HJD_CD', 'EMD_CD')
        nm_out: 출력 명칭 컬럼명 (예: 'HJD_NM', 'EMD_NM')
        mapping_path: 저장 파일 경로
        label: 로그 라벨
        force_rebuild: True면 기존 파일 무시
    """
    if not force_rebuild and os.path.exists(mapping_path):
        print(f"기존 {label} 매핑 파일 로드: {mapping_path}")
        return pd.read_parquet(mapping_path)

    print(f"격자-{label} 매핑 테이블 생성 중...")

    grid_df = _load_grid_coordinates(config)
    print(f"       {label} 수: {len(polygon_gdf):,}")

    grid_points = gpd.GeoDataFrame(
        grid_df,
        geometry=[Point(lon, lat) for lon, lat in zip(grid_df['lon'], grid_df['lat'])],
        crs='EPSG:4326',
    )

    if polygon_gdf.crs != 'EPSG:4326':
        print(f"       좌표계 변환: {polygon_gdf.crs} → EPSG:4326")
        polygon_gdf = polygon_gdf.to_crs('EPSG:4326')

    mapping = gpd.sjoin(grid_points, polygon_gdf, how='left', predicate='within')

    found_cd = _find_column(mapping, cd_candidates)
    found_nm = _find_column(mapping, nm_candidates)

    if found_cd is None or found_nm is None:
        available_cols = [
            c for c in mapping.columns
            if c not in ['grid_idx', 'lat', 'lon', 'geometry', 'index_right']
        ]
        print(f"       경고: {label} 코드/명칭 컬럼을 찾지 못했습니다.")
        print(f"       사용 가능한 컬럼: {available_cols}")
        print(f"       찾은 코드 컬럼: {found_cd}, 명칭 컬럼: {found_nm}")

    result_df = pd.DataFrame({
        'grid_idx': mapping['grid_idx'],
        'lat': mapping['lat'],
        'lon': mapping['lon'],
        cd_out: mapping[found_cd] if found_cd else None,
        nm_out: mapping[found_nm] if found_nm else None,
    })

    null_count = result_df[cd_out].isna().sum()
    print(f"       매핑 성공: {len(result_df) - null_count:,}")
    print(f"       매핑 실패: {null_count:,}")

    os.makedirs(os.path.dirname(mapping_path), exist_ok=True)
    result_df.to_parquet(mapping_path, index=False)
    print(f"       저장 완료: {mapping_path}")

    return result_df


# ── 행정동(HJD) 매퍼 ──────────────────────────────────────────


class GridToHjdMapper:
    """격자 좌표를 행정동 코드로 매핑하는 클래스.

    - 격자 위경도: `FusionConfig.grid_latlon_nc` (NetCDF)
    - 행정동 폴리곤: `FusionConfig.geodata_hjd_dir` 하위의 Shapefile
    - 결과: `FusionConfig.grid_hjd_mapping_file` (Parquet)
    """

    def __init__(self, config: Optional[FusionConfig] = None):
        self.config = config or DEFAULT_CONFIG
        self._mapping_df: Optional[pd.DataFrame] = None

    def build_mapping(self, force_rebuild: bool = False) -> pd.DataFrame:
        """격자 → 행정동 매핑 테이블 생성/로드."""
        hjd_gdf = _load_shapefiles(
            self.config.geodata_hjd_dir,
            glob_pattern=os.path.join("bnd_dong*", "*.shp"),
            label="행정동",
        )
        self._mapping_df = _build_mapping(
            config=self.config,
            polygon_gdf=hjd_gdf,
            cd_candidates=['adm_cd', 'ADSTRD_CD', 'HJD_CD', 'ADM_CD', 'ADMD_CD'],
            nm_candidates=['adm_nm', 'ADSTRD_NM', 'HJD_NM', 'ADM_NM', 'ADMD_NM'],
            cd_out='HJD_CD',
            nm_out='HJD_NM',
            mapping_path=self.config.grid_hjd_mapping_file,
            label="행정동",
            force_rebuild=force_rebuild,
        )
        return self._mapping_df

    def load_mapping(self) -> pd.DataFrame:
        """저장된 행정동 매핑 테이블 로드."""
        if self._mapping_df is not None:
            return self._mapping_df

        path = self.config.grid_hjd_mapping_file
        if os.path.exists(path):
            self._mapping_df = pd.read_parquet(path)
            return self._mapping_df
        else:
            raise FileNotFoundError(
                f"행정동 매핑 파일이 없습니다: {path}\n"
                f"먼저 build_mapping()을 실행하세요."
            )

    # 하위 호환 별칭
    build_hjd_mapping = build_mapping
    load_hjd_mapping = load_mapping


# ── 법정동(BJD/UMD) 매퍼 ──────────────────────────────────────


class GridToBjdMapper:
    """격자 좌표를 법정동 코드로 매핑하는 클래스.

    - 격자 위경도: `FusionConfig.grid_latlon_nc` (NetCDF)
    - 법정동(읍면동) 폴리곤: `FusionConfig.geodata_umd_dir` 하위 17개 시도 Shapefile
    - 결과: `FusionConfig.grid_bjd_mapping_file` (Parquet)

    Shapefile 구조 예시:
        data/geodata_umd/
        ├── LSMD_ADM_SECT_UMD_서울/LSMD_ADM_SECT_UMD_11_202602.shp
        ├── LSMD_ADM_SECT_UMD_경기/LSMD_ADM_SECT_UMD_41_202602.shp
        └── ...
    """

    def __init__(self, config: Optional[FusionConfig] = None):
        self.config = config or DEFAULT_CONFIG
        self._mapping_df: Optional[pd.DataFrame] = None

    def build_mapping(self, force_rebuild: bool = False) -> pd.DataFrame:
        """격자 → 법정동 매핑 테이블 생성/로드."""
        bjd_gdf = _load_shapefiles(
            self.config.geodata_umd_dir,
            glob_pattern=os.path.join("LSMD_ADM_SECT_UMD_*", "*.shp"),
            label="법정동",
        )
        self._mapping_df = _build_mapping(
            config=self.config,
            polygon_gdf=bjd_gdf,
            cd_candidates=['EMD_CD', 'emd_cd', 'ADM_CD', 'BJDONG_CD'],
            nm_candidates=['EMD_NM', 'emd_nm', 'ADM_NM', 'BJDONG_NM'],
            cd_out='EMD_CD',
            nm_out='EMD_NM',
            mapping_path=self.config.grid_bjd_mapping_file,
            label="법정동",
            force_rebuild=force_rebuild,
        )
        return self._mapping_df

    def load_mapping(self) -> pd.DataFrame:
        """저장된 법정동 매핑 테이블 로드."""
        if self._mapping_df is not None:
            return self._mapping_df

        path = self.config.grid_bjd_mapping_file
        if os.path.exists(path):
            self._mapping_df = pd.read_parquet(path)
            return self._mapping_df
        else:
            raise FileNotFoundError(
                f"법정동 매핑 파일이 없습니다: {path}\n"
                f"먼저 build_mapping()을 실행하세요."
            )


def build_unified_mapping(
    config: FusionConfig,
    force_rebuild: bool = False,
) -> pd.DataFrame:
    """HJD + BJD 통합 매핑 테이블 생성.

    두 매핑을 grid_idx 기준으로 조인하여 하나의 테이블로 만듭니다.
    컬럼: grid_idx, lat, lon, HJD_CD, HJD_NM, EMD_CD, EMD_NM
    """
    unified_path = config.grid_unified_mapping_file

    if not force_rebuild and os.path.exists(unified_path):
        print(f"기존 통합 매핑 파일 로드: {unified_path}")
        return pd.read_parquet(unified_path)

    print("통합 매핑 테이블(HJD+BJD) 생성 중...")

    hjd_mapper = GridToHjdMapper(config)
    bjd_mapper = GridToBjdMapper(config)

    hjd_df = hjd_mapper.build_mapping(force_rebuild=force_rebuild)
    bjd_df = bjd_mapper.build_mapping(force_rebuild=force_rebuild)

    # grid_idx 기준으로 조인 (lat, lon은 HJD 측에서 가져옴)
    unified = hjd_df[['grid_idx', 'lat', 'lon', 'HJD_CD', 'HJD_NM']].merge(
        bjd_df[['grid_idx', 'EMD_CD', 'EMD_NM']],
        on='grid_idx',
        how='outer',
    )

    os.makedirs(os.path.dirname(unified_path), exist_ok=True)
    unified.to_parquet(unified_path, index=False)

    both_valid = unified['HJD_CD'].notna() & unified['EMD_CD'].notna()
    print(f"       통합 매핑 완료: {both_valid.sum():,} 격자점 (양쪽 모두 매핑)")
    print(f"       HJD만: {(unified['HJD_CD'].notna() & unified['EMD_CD'].isna()).sum():,}")
    print(f"       BJD만: {(unified['HJD_CD'].isna() & unified['EMD_CD'].notna()).sum():,}")
    print(f"       저장: {unified_path}")

    return unified


if __name__ == "__main__":
    # 테스트 실행
    print("=== 행정동 매핑 ===")
    hjd_mapper = GridToHjdMapper()
    hjd_mapping = hjd_mapper.build_mapping(force_rebuild=True)
    print(f"  총 행정동 수: {hjd_mapping['HJD_CD'].nunique():,}")
    print(hjd_mapping.head(10))

    print("\n=== 법정동 매핑 ===")
    bjd_mapper = GridToBjdMapper()
    bjd_mapping = bjd_mapper.build_mapping(force_rebuild=True)
    print(f"  총 법정동 수: {bjd_mapping['EMD_CD'].nunique():,}")
    print(bjd_mapping.head(10))
