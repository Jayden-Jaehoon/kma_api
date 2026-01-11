"""geodata 파일 분석 스크립트"""
import pandas as pd

try:
    import xarray as xr
    HAS_XARRAY = True
except ImportError:
    HAS_XARRAY = False

# 1. NetCDF 격자 파일 분석
print('='*60)
print('1. NetCDF 격자 파일 분석: sfc_grid_latlon.nc')
print('='*60)

if HAS_XARRAY:
    nc_path = 'data/geodata/sfc_grid_latlon.nc'
    with xr.open_dataset(nc_path) as ds:
        print('\n[데이터셋 정보]')
        print(ds)
        
        print('\n[차원 정보]')
        for dim, size in ds.dims.items():
            print(f'  {dim}: {size}')
        
        print('\n[변수 정보]')
        for var in ds.data_vars:
            print(f'  {var}: shape={ds[var].shape}, dtype={ds[var].dtype}')
        
        # 위경도 범위
        if 'lat' in ds.data_vars:
            lat = ds['lat'].values
            lon = ds['lon'].values
            print(f'\n[위경도 범위]')
            print(f'  위도: {lat.min():.4f} ~ {lat.max():.4f}')
            print(f'  경도: {lon.min():.4f} ~ {lon.max():.4f}')
            print(f'  격자 크기: {lat.shape}')
            print(f'  총 격자점 수: {lat.size:,}')
            
            # 격자 해상도 추정
            if lat.ndim == 2:
                lat_diff = abs(lat[1,0] - lat[0,0])
                lon_diff = abs(lon[0,1] - lon[0,0])
                print(f'  격자 해상도 (추정): 위도 {lat_diff:.4f}°, 경도 {lon_diff:.4f}°')
else:
    print('\n[xarray 미설치 - NetCDF 분석 생략]')
    print('  파일: data/geodata/sfc_grid_latlon.nc')
    import os
    print(f'  파일 크기: {os.path.getsize("data/geodata/sfc_grid_latlon.nc") / 1024 / 1024:.2f} MB')

# 2. Shapefile 행정동 데이터 분석
print('\n' + '='*60)
print('2. Shapefile 행정동 데이터 분석: BND_ADM_DONG_PG.shp')
print('='*60)

# DBF 파일에서 속성 정보 읽기 (geopandas 없이)
import struct
dbf_path = 'data/geodata/BND_ADM_DONG_PG.dbf'
with open(dbf_path, 'rb') as f:
    numrec, lenheader = struct.unpack('<xxxxLH22x', f.read(32))
    numfields = (lenheader - 33) // 32
    print(f'\n[기본 정보]')
    print(f'  총 레코드 수: {numrec:,}')
    print(f'  필드 수: {numfields}')
    
    # 필드 정보 읽기
    fields = []
    for _ in range(numfields):
        field_data = f.read(32)
        name = field_data[:11].replace(b'\x00', b'').decode('cp949', errors='ignore')
        fields.append(name)
    print(f'\n[필드 목록]')
    for field in fields:
        print(f'  - {field}')

# PRJ 파일에서 좌표계 정보 읽기
prj_path = 'data/geodata/BND_ADM_DONG_PG.prj'
with open(prj_path, 'r') as f:
    prj_content = f.read()
    print(f'\n[좌표계 정보]')
    print(f'  {prj_content[:100]}...')

# 3. Parquet 매핑 테이블 분석
print('\n' + '='*60)
print('3. Parquet 매핑 테이블 분석: grid_to_lawid.parquet')
print('='*60)

parquet_path = 'data/geodata/grid_to_lawid.parquet'
mapping_df = pd.read_parquet(parquet_path)

print(f'\n[기본 정보]')
print(f'  총 격자점 수: {len(mapping_df):,}')
print(f'  매핑된 법정동 수: {mapping_df["LAW_ID"].nunique():,}')
print(f'  매핑 실패 (해양 등): {mapping_df["LAW_ID"].isna().sum():,}')

print(f'\n[컬럼 정보]')
for col in mapping_df.columns:
    print(f'  {col}: {mapping_df[col].dtype}')

print(f'\n[위경도 범위]')
print(f'  위도: {mapping_df["lat"].min():.4f} ~ {mapping_df["lat"].max():.4f}')
print(f'  경도: {mapping_df["lon"].min():.4f} ~ {mapping_df["lon"].max():.4f}')

print(f'\n[샘플 데이터 (처음 10개)]')
print(mapping_df.head(10).to_string())

print(f'\n[법정동별 격자 수 통계]')
grid_per_dong = mapping_df.groupby('LAW_ID').size()
print(f'  최소: {grid_per_dong.min()}')
print(f'  최대: {grid_per_dong.max()}')
print(f'  평균: {grid_per_dong.mean():.1f}')
print(f'  중앙값: {grid_per_dong.median():.1f}')
