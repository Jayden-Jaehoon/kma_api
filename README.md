### 프로젝트 개요 (`run_fusion.py` 기준)
이 프로젝트는 기상청 API로부터 “격자 단위” 기상 데이터를 내려받아(다운로드/캐시) 시간 집계 후, **격자 → 행정동(코드) 단위로 공간 집계**하여 일별 결과를 만드는 파이프라인입니다.

실행 엔트리포인트는 `run_fusion.py`이며, 내부적으로 `fusion/pipeline.py`의 `FusionPipeline`과 `fusion/geocode.py`의 `GridToLawIdMapper`를 사용합니다.

---

### 실행 방법 및 주요 옵션 (`run_fusion.py`)
`run_fusion.py`는 크게 2가지 일을 합니다.

#### 1) 격자-행정동(코드) 매핑 테이블 생성/재생성
- `python run_fusion.py --build-mapping`
- `python run_fusion.py --rebuild-mapping`

#### 2) 일/월/연 단위 처리 파이프라인 실행
- 기본: `python run_fusion.py`
- 기간/변수 지정: `python run_fusion.py --start-year 2020 --end-year 2024 --variables ta,rn_60m,sd_3hr`
- 특정 하루만 테스트: `python run_fusion.py --test-day 20240101 --variables ta,rn_60m`

**필수:**
- `.env`에 `authKey`가 있어야 합니다. (`run_fusion.py`에서 `dotenv.load_dotenv()` 후 `os.getenv("authKey")`로 읽음)

---

### 데이터/설정의 “기준 경로” (`fusion/config.py`)
설정은 `fusion/config.py`의 `FusionConfig`에 모여 있으며, 핵심 경로는 다음과 같습니다.

- **프로젝트 루트**: `FusionConfig.project_root` (현재 코드에 하드코딩: `/Users/jaehoon/liminal_ego/git_clones/kma_api`)
- **데이터 루트**: `data/` (`config.data_dir`)
- **지오데이터(격자/행정동) 폴더**: `data/geodata/` (`config.geodata_dir`)
- **산출/중간/원천**:
  - `data/fusion_raw/` (API 원본 캐시)
  - `data/fusion_interim/` (중간 산출)
  - `data/fusion_output/` (최종 산출)

**격자-행정동 매핑 관련 핵심 파일 경로:**
- 행정동 경계 Shapefile: `data/geodata/BND_ADM_DONG_PG.shp` (`config.legal_dong_shp`)
- 격자 위경도 NetCDF: `data/geodata/sfc_grid_latlon.nc` (`config.grid_latlon_nc`)
- 격자→행정동 매핑 결과: `data/geodata/grid_to_lawid.parquet` (`config.grid_mapping_file`)

---

### `data/geodata` 디렉터리 상세 설명
현재 `data/geodata`에는 다음 파일들이 있습니다.

#### 1) `BND_ADM_DONG_PG.*` (행정동 경계 Shapefile 세트)
- **구성**: `BND_ADM_DONG_PG.shp`, `BND_ADM_DONG_PG.shx`, `BND_ADM_DONG_PG.dbf`, `BND_ADM_DONG_PG.prj`, `BND_ADM_DONG_PG.cpg`
- **역할**: 행정동(폴리곤) 경계를 제공하여 격자점(Point)이 어느 행정동 폴리곤 안에 들어가는지를 판정하는 공간조인의 기준 데이터입니다.
- **실제 로딩/사용 위치**:
  - `fusion/geocode.py`의 `GridToLawIdMapper._load_legal_dong()`
  - `geopandas.read_file()`로 읽고, 필요시 좌표계를 `EPSG:4326`으로 변환한 뒤 `sjoin(..., predicate='within')`에 사용
- **확인된 스키마(실제 읽어본 결과)**:
  - 행(row) 수: 3558
  - CRS: `EPSG:5186`
  - 주요 컬럼: `ADM_CD`(행정동 코드), `ADM_NM`(행정동명), `BASE_DATE`, `geometry`
- **코드에서 코드/명칭 컬럼을 찾는 방식**:
  - 매핑 결과에서 코드 컬럼은 후보군 `['ADM_DR_CD','ADM_CD','EMD_CD','BJDONG_CD','LAW_ID']` 중 존재하는 것을 선택
  - 명칭 컬럼은 후보군 `['ADM_DR_NM','ADM_NM','EMD_NM','BJDONG_NM','LAW_NM']` 중 존재하는 것을 선택
  - 즉 현재 파일은 `ADM_CD`, `ADM_NM`가 매핑에 사용됩니다.

#### 2) `sfc_grid_latlon.nc` (격자 위경도 NetCDF)
- **역할**: 기상 데이터가 제공되는 “격자”의 각 지점이 가지는 위도/경도를 제공합니다. 이 위경도가 `GridToLawIdMapper`에서 `Point(lon, lat)`로 변환되어 행정동 폴리곤과 공간조인을 수행합니다.
- **실제 로딩/사용 위치**: `fusion/geocode.py`의 `GridToLawIdMapper._load_grid_coordinates()`
- **확인된 구조(실제 열어본 결과)**:
  - Dimensions: `(ny: 2049, nx: 2049)`
  - Data variables: `lat (ny,nx)`, `lon (ny,nx)`
  - Attributes: Lambert Conformal Conic 관련 속성(`map_pro`, `map_slon`, `map_slat`, `grid_size` 등)
- **“격자 인덱스(grid_idx)” 정의**:
  - 코드에서 `lat`/`lon`이 2D 배열이면 flatten하여 1차원으로 만들고, `grid_idx = range(len(lat_flat))`로 0부터 순차 부여합니다.
  - 따라서 `grid_idx`는 “(ny,nx) 2D 격자에서 flatten한 순서”에 종속됩니다.
    - 같은 NetCDF를 계속 쓰는 한 `grid_idx`의 의미/순서는 안정적입니다.
    - NetCDF가 바뀌면(해상도/차원/정렬 변경) `grid_idx` 의미가 바뀌므로 매핑도 재생성이 필요합니다.

#### 3) `grid_to_lawid.parquet` (격자→행정동 매핑 결과 캐시)
- **역할**: 매번 공간조인을 수행하지 않도록, 격자점마다 행정동 코드/명칭을 미리 계산해 저장한 캐시 테이블입니다.
- **생성/갱신 위치**:
  - `fusion/geocode.py`의 `GridToLawIdMapper.build_mapping()`
  - `run_fusion.py --build-mapping` 또는 파이프라인 실행 시 `FusionPipeline.ensure_mapping()`에서 필요하면 생성
- **스키마(코드 기준)**: `grid_idx`, `lat`, `lon`, `LAW_ID`, `LAW_NM`
- **주의**:
  - `LAW_ID`는 실제로는 이 Shapefile의 `ADM_CD`가 들어가며, 프로젝트에서는 이를 통칭해 `LAW_ID` 컬럼에 담습니다.
  - 즉 “법정동/행정동 용어”는 데이터 원천에 따라 다를 수 있으나, 파이프라인에서는 일관되게 `LAW_ID`라는 키로 취급합니다.

---

### 격자 구조와 행정동 매핑 로직 (핵심)

#### 1) 격자 구조 (Grid)
- **원천**: `data/geodata/sfc_grid_latlon.nc`
- **형태**: `(ny,nx)` 2D 격자(현재 2049×2049)
- 각 격자점은 중심 위경도(`lat`,`lon`)를 가지며, 이 점을 이용해 행정동 폴리곤 내부 포함 여부를 판정합니다.

#### 2) 행정동 경계 (Polygon)
- **원천**: `data/geodata/BND_ADM_DONG_PG.shp`
- **CRS**: 원본은 `EPSG:5186`이지만, 공간조인 전에 `EPSG:4326`으로 변환될 수 있습니다.

#### 3) 매핑 방식: Point-in-Polygon (`within`)
- **구현**: `fusion/geocode.py`의 `build_mapping()`
- **절차**:
  1. NetCDF에서 모든 격자점 위경도 로드 → `grid_df(grid_idx, lat, lon)` 생성
  2. `grid_df`를 `GeoDataFrame`으로 변환: `geometry = Point(lon, lat)`, CRS=`EPSG:4326`
  3. 행정동 `GeoDataFrame`을 필요시 `EPSG:4326`으로 변환
  4. `geopandas.sjoin(grid_points, dong_gdf, how='left', predicate='within')`
     - **의미**: “격자점이 폴리곤 내부에 완전히 포함(`within`) 되는 행정동을 찾음”

---

### 행정동에 매핑되지 않는 격자(미매핑 격자)와 처리 방식
`GridToLawIdMapper.build_mapping()` 결과에서 `LAW_ID`가 `NaN`인 격자들이 “미매핑 격자”입니다.

#### 1) 왜 미매핑이 발생하나?
코드와 데이터 특성상 대표적으로 다음 케이스가 있습니다.
- **해양(바다) 격자**: 행정동 폴리곤은 육지 중심이므로 바다 위 격자점은 어떤 폴리곤에도 속하지 않음
- **북한/국외 등 경계 밖**: Shapefile이 커버하지 않는 영역의 격자
- **경계선 위의 점**: `predicate='within'`은 “경계선 위(on boundary)”를 내부로 보지 않는 경우가 있어, 경계 부근 격자점이 미매핑될 수 있음
- **(드물게) 좌표계/정합 문제**: 폴리곤 또는 위경도 데이터의 정합 오류/누락

#### 2) 미매핑 격자는 어떻게 보관되나?
- `grid_to_lawid.parquet`에는 미매핑 격자도 행으로 존재하며, `LAW_ID`/`LAW_NM`만 `NaN`으로 남습니다.
- `build_mapping()` 실행 시 통계로 `매핑 성공` / `매핑 실패 (해양/북한 등)`을 출력합니다.

#### 3) 실제 집계 단계에서 미매핑 격자는 어떻게 처리되나?
- `fusion/aggregate.py`의 `SpatialAggregator.aggregate_grid_to_lawid()`에서
  1. 격자 데이터에 매핑을 `left merge`로 붙인 뒤
  2. `LAW_ID`가 `NaN`인 행을 집계 전에 제거합니다: `df_with_lawid = df_with_lawid[df_with_lawid['LAW_ID'].notna()]`
- 즉 최종 “행정동별 결과”에는 미매핑 격자 기여분이 포함되지 않습니다.

#### 4) 강수/적설 vs 기온: 결측 처리의 차이
공간 집계 직전에 변수 성격에 따라 결측 처리 정책이 다릅니다.
- **강수/적설(`p*`, `s*` 컬럼)**: `NaN`을 `0`으로 간주 (`fillna(0)`)
  - “관측/산출이 없는 곳은 현상이 없다고 본다”는 정책
- **기온 등(`t*`)**: `NaN` 유지
  - 평균 계산 시 `NaN`은 자동 제외되어 왜곡을 줄임

---

### 파이프라인에서 매핑이 쓰이는 지점 (큰 흐름)
1. `run_fusion.py` → `FusionPipeline(auth_key, config)` 생성
2. `FusionPipeline.ensure_mapping()`
   - 내부에서 `GridToLawIdMapper.build_mapping()` 실행(또스 `grid_to_lawid.parquet` 로드)
   - `SpatialAggregator(self._grid_mapping, config)` 준비
3. 이후 일별 처리(`FusionPipeline.process_day()` 등)에서
   - 시간 집계된 격자 데이터에 대해 `SpatialAggregator.aggregate_grid_to_lawid()`로 행정동별 집계
   - 최종적으로 `OutputFormatter`가 변수 병합/컬럼 정렬을 수행
   - 필요하면 `OutputFormatter.add_lawid_name(df, grid_mapping)`으로 `LAW_NM`을 덧붙일 수 있음

---

### 운영/갱신 시 실무 체크리스트 (특히 `data/geodata`)
- **`sfc_grid_latlon.nc`를 교체/갱신하면**: 격자 구조(차원/순서)가 바뀔 수 있으므로 **반드시 `--rebuild-mapping`으로 `grid_to_lawid.parquet` 재생성** 권장
- **`BND_ADM_DONG_PG.*`를 교체/갱신하면**: `ADM_CD`/`ADM_NM` 체계가 바뀌거나 경계가 업데이트될 수 있으므로 **재매핑 필요**
- **미매핑 격자 비율이 과도하게 높다면**: (1) Shapefile 커버리지, (2) 좌표계 변환, (3) `within` 경계 판정 특성(경계점) 이슈를 우선 의심

---

### 참고: 관련 파일 위치 요약
- **실행**: `run_fusion.py`
- **설정**: `fusion/config.py`
- **격자→행정동 매핑**: `fusion/geocode.py`
- **공간 집계(미매핑 제거 포함)**: `fusion/aggregate.py` (`SpatialAggregator`)
- **파이프라인**: `fusion/pipeline.py`
- **지오데이터**: `data/geodata/` (`BND_ADM_DONG_PG.*`, `sfc_grid_latlon.nc`, `grid_to_lawid.parquet`)