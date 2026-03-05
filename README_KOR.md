# KMA 융합기상정보 데이터 파이프라인

[English Version](README.md)

## 개요

이 프로젝트는 기상청(KMA) API로부터 격자 기반 기상 데이터를 다운로드하고, 시간 집계를 수행한 후 **격자에서 법정동(읍면동) 행정 경계로 공간 집계**하여 일별 기상 데이터를 생성합니다.

파이프라인은 두 단계로 구분됩니다:
- **A 단계 (다운로드/캐시)**: `run_download_fusion.py`
- **B 단계 (후처리)**: `run_process_fusion.py`

내부적으로 `fusion/pipeline.py`의 `FusionPipeline`과 `fusion/geocode.py`의 `GridToLawIdMapper`를 사용합니다.

---

## 사전 준비

### 1. 환경 설정

Conda 환경을 생성하고 활성화합니다:

```bash
conda env create -f environment.yml
conda activate kma-api
```

### 2. API 인증

**필수**: 프로젝트 루트에 KMA API Hub 인증 키가 포함된 `.env` 파일을 생성합니다:

```bash
authKey=발급받은_인증키
```

API 키는 [KMA API Hub](https://apihub.kma.go.kr/)에서 발급받을 수 있습니다.

### 3. 필수 외부 데이터 파일

다음 데이터 파일을 다운로드하여 올바른 디렉토리에 배치해야 합니다:

#### 법정동(읍면동) 경계 Shapefile

**위치**: `data/geodata_umd/`

**다운로드할 파일**: 정부 공간정보 포털에서 17개 시도별 법정동 shapefile

**출처**: [국가공간정보포털](http://data.nsdi.go.kr/)
- "법정경계(읍면동)" 또는 "LSMD_ADM_SECT_UMD" 검색
- 17개 시도(특별시/광역시/도) shapefile 모두 다운로드

**기대되는 구조**:
```
data/geodata_umd/
├── LSMD_ADM_SECT_UMD_11/  # 서울
│   ├── LSMD_ADM_SECT_UMD_11_202602.shp
│   ├── LSMD_ADM_SECT_UMD_11_202602.shx
│   ├── LSMD_ADM_SECT_UMD_11_202602.dbf
│   └── LSMD_ADM_SECT_UMD_11_202602.prj
├── LSMD_ADM_SECT_UMD_26/  # 부산
│   └── ...
├── LSMD_ADM_SECT_UMD_41/  # 경기
│   └── ...
...
└── LSMD_ADM_SECT_UMD_50/  # 제주
    └── ...
```

**필수 컬럼**: `EMD_CD` (법정동코드), `EMD_NM` (법정동명), `geometry`

#### 격자 좌표 NetCDF 파일

**위치**: `data/geodata/sfc_grid_latlon.nc`

**내용**: KMA 융합기상 데이터 시스템의 각 격자 셀에 대한 위도/경도 좌표가 포함된 NetCDF 파일

**출처**: KMA API Hub 지원팀 문의 또는 API 문서의 격자 좌표 데이터 참조

**기대되는 구조**:
- Dimensions: `(ny: 2049, nx: 2049)`
- Variables: `lat(ny, nx)`, `lon(ny, nx)`
- CRS 정보: Lambert Conformal Conic 투영법 속성

**참고**: 이 파일은 격자 인덱스와 지리 좌표 간의 매핑을 정의합니다. 이 파일을 업데이트하면 격자-법정동 매핑을 재생성해야 합니다.

---

## 격자-법정동 매핑

### 자동 생성

후처리 단계(B)에는 격자-법정동 매핑 파일이 필요합니다: `data/geodata/grid_to_emd_umd.parquet`

- 파일이 존재하면 자동으로 로드됩니다
- 파일이 없으면 첫 실행 시 자동으로 생성됩니다 (10-30분 소요, 1회 작업)
- 매핑은 17개 시도 shapefile을 통합하고 point-in-polygon 공간 조인을 수행합니다

### 수동 생성

매핑 생성 (없으면 생성, 있으면 로드):
```bash
python -c "import os; from fusion.config import FusionConfig; from fusion.geocode import GridToLawIdMapper; cfg=FusionConfig(project_root=os.getcwd()); GridToLawIdMapper(cfg).build_mapping(force_rebuild=False)"
```

강제 재생성:
```bash
python -c "import os; from fusion.config import FusionConfig; from fusion.geocode import GridToLawIdMapper; cfg=FusionConfig(project_root=os.getcwd()); GridToLawIdMapper(cfg).build_mapping(force_rebuild=True)"
```

또는 `run_process_fusion.py` 실행 시 `--force-rebuild-mapping` 플래그를 사용할 수 있습니다.

---

## 사용법

### 권장 워크플로우: A (다운로드) → B (후처리)

대규모 격자 API 호출은 시간이 오래 걸리고 신중한 재시도/로깅 관리가 필요합니다. 따라서 **다운로드 단계(A)**와 **캐시 기반 후처리 단계(B)**를 분리하는 것을 권장합니다.

### A 단계: 원시 데이터 다운로드/캐싱

**스크립트**: `run_download_fusion.py`

**목적**: `data/fusion_raw/YYYY/MM/{var}_{date}_parsed.parquet` 캐시 파일 생성

**특징**:
- 날짜 단위 병렬 처리 (기본 `--max-workers 4`)
- 지수 백오프를 사용한 자동 재시도
- `data/fusion_raw/_validation_logs/`에 검증 로그 기록

**예시**:

특정 날짜 범위 다운로드:
```bash
python run_download_fusion.py \
  --start-year 2024 \
  --end-year 2024 \
  --start-month 6 \
  --end-month 7 \
  --variables ta,rn_60m,sd_3hr \
  --max-workers 4
```

단일 날짜 테스트:
```bash
python run_download_fusion.py \
  --test-day 20241128 \
  --variables ta,rn_60m
```

**매개변수**:
- `--start-year`, `--end-year`: 연도 범위 (포함)
- `--start-month`, `--end-month`: 월 범위 (포함)
- `--variables`: 쉼표로 구분된 변수 목록 (ta, rn_60m, sd_3hr)
- `--test-day`: 단일 날짜 테스트 모드 (YYYYMMDD 형식)
- `--max-workers`: 날짜 단위 병렬 처리를 위한 워커 수

### B 단계: 캐시 기반 후처리

**스크립트**: `run_process_fusion.py`

**목적**: 캐시된 `*_parsed.parquet` 파일을 사용하여 `data/fusion_interim` 및 `data/fusion_output` 결과 생성

**정책**: 캐시 파일이 없는 날짜/변수는 건너뛰고(B 정책) 요약을 출력합니다

**예시**:

날짜 범위 처리:
```bash
python run_process_fusion.py \
  --start-year 2024 \
  --end-year 2024 \
  --start-month 6 \
  --end-month 7 \
  --variables ta,rn_60m,sd_3hr
```

단일 날짜 테스트:
```bash
python run_process_fusion.py \
  --test-day 20241128 \
  --variables ta,rn_60m
```

매핑 강제 재생성:
```bash
python run_process_fusion.py \
  --test-day 20241128 \
  --variables ta,rn_60m \
  --force-rebuild-mapping
```

**매개변수**:
- `--start-year`, `--end-year`: 연도 범위 (포함)
- `--start-month`, `--end-month`: 월 범위 (포함)
- `--variables`: 쉼표로 구분된 변수 목록
- `--test-day`: 단일 날짜 테스트 모드 (YYYYMMDD 형식)
- `--force-rebuild-mapping`: 격자-법정동 매핑 강제 재생성

---

## 프로젝트 구조

```
kma_api/
├── data/
│   ├── geodata/                      # 지리공간 참조 데이터
│   │   ├── sfc_grid_latlon.nc        # 격자 좌표 (필수)
│   │   └── grid_to_emd_umd.parquet   # 격자-법정동 매핑 (자동 생성)
│   ├── geodata_umd/                  # 법정동 경계 (필수)
│   │   ├── LSMD_ADM_SECT_UMD_11/     # 서울
│   │   ├── LSMD_ADM_SECT_UMD_26/     # 부산
│   │   ├── LSMD_ADM_SECT_UMD_41/     # 경기
│   │   └── ...                       # 기타 14개 시도
│   ├── fusion_raw/                   # 원시 API 캐시 (A 단계 출력)
│   │   ├── YYYY/MM/                  # 연도/월별 구성
│   │   │   ├── ta_YYYYMMDD_parsed.parquet
│   │   │   ├── rn_60m_YYYYMMDD_parsed.parquet
│   │   │   └── sd_3hr_YYYYMMDD_parsed.parquet
│   │   └── _validation_logs/         # 검증/오류 로그
│   ├── fusion_interim/               # 중간 결과 (B 단계 출력)
│   │   └── YYYY/
│   │       └── fusion_YYYYMMDD.parquet
│   └── fusion_output/                # 최종 CSV 출력 (B 단계 출력)
│       ├── YYYY/
│       │   └── fusion_YYYYMM.csv     # 월별 데이터
│       └── fusion_weather_YYYY.csv   # 연도별 데이터
├── fusion/                           # 핵심 모듈
│   ├── config.py                     # 설정
│   ├── geocode.py                    # 격자-법정동 매핑
│   ├── download.py                   # API 다운로더
│   ├── aggregate.py                  # 시간 및 공간 집계
│   └── pipeline.py                   # 메인 파이프라인 오케스트레이션
├── run_download_fusion.py            # A 단계: 다운로드 스크립트
├── run_process_fusion.py             # B 단계: 후처리 스크립트
├── environment.yml                   # Conda 환경 사양
├── .env                              # API 키 (직접 생성, git에 포함 안 됨)
└── README.md                         # 영문 README
```

---

## 설정

모든 설정은 `fusion/config.py` (`FusionConfig` 클래스)에 있습니다:

### 주요 경로

- **프로젝트 루트**: `FusionConfig.project_root`
- **데이터 루트**: `data/`
- **지오데이터**: `data/geodata/`, `data/geodata_umd/`
- **원시 캐시**: `data/fusion_raw/` (API 캐시)
- **중간 결과**: `data/fusion_interim/` (중간 결과)
- **출력**: `data/fusion_output/` (최종 CSV 파일)

### 핵심 파일

- **법정동 경계**: `data/geodata_umd/LSMD_ADM_SECT_UMD_*/*.shp` (17개 시도)
- **격자 좌표**: `data/geodata/sfc_grid_latlon.nc`
- **격자 매핑**: `data/geodata/grid_to_emd_umd.parquet`

---

## 데이터 변수

### 사용 가능한 변수

| 변수 | 설명 | 단위 | 시간 해상도 | 시작 연도 | 공간 집계 방법 |
|------|------|------|------------|-----------|---------------|
| `ta` | 기온 | ℃ | 1시간 | 1997 | 법정동 내 모든 격자 셀의 **평균** |
| `rn_60m` | 60분 강수량 | mm | 1시간 | 1997 | 법정동 내 모든 격자 셀의 **평균** |
| `sd_3hr` | 3시간 신적설 | cm | 3시간 | 2020 | 모든 격자 셀의 **평균** (10월-5월만) |

**공간 집계 상세 설명:**

하나의 법정동 경계 내에 여러 격자 셀이 있을 때, 파이프라인은 다음과 같이 집계합니다:

1. **기온 (`ta`)**:
   - 방법: `mean()` - 법정동 내 모든 격자 셀 값의 평균
   - NaN 처리: **보존됨** (평균 계산에서 제외)
   - 근거: 결측 기온 데이터가 "영도"를 의미하지 않습니다. NaN 값을 제외하면 평균 계산의 편향을 방지합니다. 격자 셀에 결측 데이터가 있는 경우, 유효한 측정값만으로 평균을 계산하는 것이 더 좋습니다.
   - 예시: 법정동 "청운효자동"에 5개 격자 셀의 기온이 [15.2, 15.5, NaN, 15.3, 15.4]℃인 경우, 결과는 (15.2+15.5+15.3+15.4)/4 = 15.35℃
   - 참고: NaN이 제외되므로 5(전체 셀)가 아닌 4(유효한 값)로 나눕니다

2. **강수량 (`rn_60m`)**:
   - 방법: `mean()` - 법정동 내 모든 격자 셀 값의 평균
   - NaN 처리: **0으로 변환** (가정: 데이터 없음 = 강수 없음)
   - 근거: 강수 이벤트의 경우, 결측 데이터는 일반적으로 해당 위치에 강수가 발생하지 않았음을 의미합니다. NaN을 0으로 변환하는 것은 기상학적으로 타당합니다 - 상당한 강우가 있었다면 측정/추정되었을 것입니다. 이는 건조 지역을 제외하여 강수량을 과대평가하는 것을 방지합니다.
   - 예시: 법정동에 5개 격자 셀의 강수량이 [0.5, 1.2, NaN, 0.0, 0.3]mm인 경우, 결과는 (0.5+1.2+0+0.0+0.3)/5 = 0.4mm
   - 참고: NaN을 0으로 처리하고 5(모든 셀)로 나누어 대표적인 면적 평균 강수량을 얻습니다

3. **적설량 (`sd_3hr`)**:
   - 방법: `mean()` - 법정동 내 모든 격자 셀 값의 평균
   - NaN 처리: **0으로 변환** (가정: 데이터 없음 = 적설 없음)
   - 근거: 강수량과 동일한 논리 - 결측 적설 데이터는 해당 위치에 적설 이벤트가 없었음을 의미합니다
   - 계절성: 10월-5월만 생산 (6월-9월은 자동 건너뛰기)

**주요 구현 세부사항:**
- 법정동에 매핑되지 않은 격자 셀(해양, 북한, 경계)은 집계 전에 제외됩니다
- 각 법정동의 최종 값 = 폴리곤 경계 내 모든 격자 셀의 산술 평균
- 결측 데이터(`NaN`) 처리는 집계의 편향을 피하기 위해 변수 유형에 따라 다릅니다

### 출력 컬럼 형식

**기온**: `t0001`, `t0102`, ..., `t2324` (24개 컬럼, 시간별)
**강수량**: `p0001`, `p0102`, ..., `p2324` (24개 컬럼, 시간별)
**적설량**: `s0003`, `s0306`, ..., `s2124` (8개 컬럼, 3시간별)

컬럼 명명 규칙: `{접두어}{시작시간:02d}{종료시간:02d}`

---

## 지리공간 매핑 로직

### 격자 구조

- **출처**: `data/geodata/sfc_grid_latlon.nc`
- **형식**: `(ny, nx)` 2D 격자 (현재 2049×2049)
- 각 격자 셀은 공간 조인에 사용되는 중심 좌표 `(lat, lon)`를 가집니다

### 법정동 경계

- **출처**: `data/geodata_umd/LSMD_ADM_SECT_UMD_*/*.shp` (17개 시도)
- **CRS**: 원본 CRS 유지, 필요 시 공간 조인을 위해 `EPSG:4326`으로 변환

### 매핑 방법: Point-in-Polygon (`within`)

**구현**: `fusion/geocode.py` → `GridToLawIdMapper.build_mapping()`

**프로세스**:
1. NetCDF에서 모든 격자 셀 좌표 로드 → `grid_df(grid_idx, lat, lon)`
2. 17개 시도 shapefile 로드 및 병합
3. `grid_df`를 `GeoDataFrame`으로 변환: `geometry = Point(lon, lat)`, CRS=`EPSG:4326`
4. 필요 시 법정동 `GeoDataFrame`을 `EPSG:4326`으로 변환
5. 공간 조인 수행: `geopandas.sjoin(grid_points, dong_gdf, how='left', predicate='within')`
   - **의미**: "이 격자 점을 완전히 포함하는 법정동 폴리곤 찾기"

### 매핑되지 않은 격자 셀

`EMD_CD`에 `NaN`이 있는 격자 셀은 "매핑되지 않음"이며 일반적으로 다음을 나타냅니다:
- **해양/바다 지역**: 법정동 폴리곤은 육지만 커버합니다
- **북한/경계 외부**: Shapefile로 커버되지 않는 지역
- **경계선 위의 점**: `predicate='within'`은 폴리곤 경계선 정확히 위의 점을 포함하지 않을 수 있습니다
- **(드물게) 좌표/정렬 문제**: 데이터 품질 문제

**저장**: 매핑되지 않은 셀은 `grid_to_emd_umd.parquet`에 `EMD_CD`/`EMD_NM` = `NaN`으로 보존됩니다

**처리**: 공간 집계 중(`fusion/aggregate.py`), `EMD_CD = NaN`인 행은 집계 전에 제거되므로 최종 법정동 결과에 기여하지 않습니다.

### 변수 유형별 결측 데이터 처리

공간 집계 중 변수에 따라 결측 데이터를 다르게 처리합니다:
- **강수/적설** (`p*`, `s*` 컬럼): `NaN` → `0` (가정: 관측 없음 = 현상 없음)
- **기온** (`t*` 컬럼): `NaN` 보존 (편향을 피하기 위해 평균 계산에서 제외)

---

## 파이프라인 워크플로우

### A 단계: 다운로드 (병렬)

```
run_download_fusion.py
    ↓
각 날짜마다 (병렬 워커):
    각 시간마다:
        API 호출 → 검증 → 필요 시 재시도
        격자 응답 파싱 → 엄격한 검증
        저장: fusion_raw/YYYY/MM/{var}_{date}_parsed.parquet
    실패 로그: fusion_raw/_validation_logs/YYYY/MM/{date}_{var}.txt
```

### B 단계: 후처리 (날짜별 순차)

```
run_process_fusion.py
    ↓
격자-법정동 매핑 로드/생성
    ↓
각 날짜마다:
    캐시된 parquet 파일 로드 (없으면 건너뛰기)
    시간 집계 (캐시에 이미 1시간/3시간 단위)
    피벗: 시간 → 컬럼 (t0001, t0102, ...)
    공간 집계: 격자 → 법정동 (각 동의 격자 셀 평균)
    매핑되지 않은 격자 셀 제거 (EMD_CD = NaN)
    변수 병합 (기온, 강수, 적설)
    저장:
        - 중간: fusion_interim/YYYY/fusion_YYYYMMDD.parquet
        - 월별: fusion_output/YYYY/fusion_YYYYMM.csv
        - 연도별: fusion_output/fusion_weather_YYYY.csv
```

---

## 운영 참고사항

### 격자 매핑 재생성 시기

**`grid_to_emd_umd.parquet` 재생성이 필요한 경우**:
- `sfc_grid_latlon.nc` 업데이트/교체 (격자 구조/차원 변경)
- 법정동 shapefile 업데이트 (경계 변경)
- 매핑되지 않은 격자 셀 비율이 높음 (데이터 품질 확인)

**명령어**:
```bash
python run_process_fusion.py --force-rebuild-mapping --test-day 20241128 --variables ta
```

### 검증 로그

모든 다운로드/파싱 실패는 다음에 로그됩니다:
```
data/fusion_raw/_validation_logs/YYYY/MM/{date}_{var}.txt
```

각 로그 항목에는 다음이 포함됩니다:
- 타임스탬프
- 심각도 수준 (INFO, WARN, ERROR)
- 시간 코드 (tm)
- 오류 메시지
- 응답 미리보기 (디버깅용)

### 캐시 파일 누락 (B 단계)

B 단계 실행 시 캐시 파일이 없는 경우:
- 날짜/변수를 건너뜁니다 (오류가 아님)
- 종료 시 건너뛴 모든 항목을 보여주는 요약 출력
- 누락된 날짜를 채우려면 A 단계를 다시 실행

---

## 문제 해결

### 문제: "ModuleNotFoundError: No module named 'geopandas'"

**해결책**: Conda 환경 생성:
```bash
conda env create -f environment.yml
conda activate kma-api
```

### 문제: "grid_to_emd_umd.parquet not found"

**해결책**: 첫 B 단계 실행 시 파일이 자동 생성됩니다 (10-30분). 또는 수동 생성:
```bash
python run_process_fusion.py --force-rebuild-mapping --test-day 20241128 --variables ta
```

### 문제: 다운로드 중 "HTTP 403 Forbidden"

**해결책**:
1. `.env` 파일에 올바른 `authKey`가 있는지 확인
2. [KMA API Hub 마이페이지](https://apihub.kma.go.kr/mypage)에서 API 권한 확인
3. 특정 변수(`ta`, `rn_60m`, `sd_3hr`)가 API 키에 대해 활성화되어 있는지 확인

### 문제: 매핑되지 않은 격자 셀이 많음

**해결책**:
1. 법정동 shapefile이 완전한지 확인 (17개 시도 모두)
2. Shapefile의 CRS 호환성 확인 (자동으로 EPSG:4326으로 변환되어야 함)
3. 격자 좌표 파일(`sfc_grid_latlon.nc`) 커버리지 검토
4. 해양/경계 셀에 대한 허용 오차 고려 (이는 예상됨)

### 문제: 여름철 적설 데이터 누락

**예상된 동작**: 적설(`sd_3hr`)은 10월-5월만 생산됩니다. 파이프라인은 6-9월에 `sd_3hr`을 자동으로 건너뜁니다.

### 문제: 다운로드 실패/재시도

**확인 사항**:
1. 검증 로그: `data/fusion_raw/_validation_logs/`
2. 네트워크 연결
3. API 속도 제한 (기본: 호출 간 0.5초)
4. `fusion/config.py`의 재시도 설정:
   - `download_retry_attempts` (기본: 3)
   - `download_retry_initial_sleep_seconds` (기본: 10.0)
   - `download_retry_backoff` (기본: 2.0)

---

## 기술 세부사항

### 격자 인덱스 (`grid_idx`)

- **정의**: `(ny, nx)` 2D 격자 좌표를 평탄화한 후 할당된 순차 인덱스 (0부터 N-1)
- **안정성**: `sfc_grid_latlon.nc` 차원/구조가 변경되지 않는 한 안정적
- **변경 영향**: NetCDF가 변경되면 (해상도/순서), grid_idx 의미가 변경됨 → 매핑 재생성 필요

### 법정동 코드

- **형식**: `EMD_CD` (예: "1111010100" - 10자리 코드)
- **계층**: 시도(2) + 시군구(3) + 법정동(5)
- **이름**: `EMD_NM` (한글 이름, 예: "청운효자동")

### 컬럼명 매핑 (내부)

- **소스 데이터**: `EMD_CD`, `EMD_NM` 사용
- **내부 파이프라인**: 호환성을 위해 `LAW_ID`, `LAW_NM`으로 이름 변경
- **출력 파일**: 다시 `EMD_CD`, `EMD_NM`으로 변환

### CRS 처리

- **격자 좌표**: `EPSG:4326` (WGS84) 가정
- **Shapefile**: 원본 CRS 유지, 공간 조인을 위해 `EPSG:4326`으로 자동 변환
- **공간 조인**: 모든 작업은 `EPSG:4326`에서 수행

---

## 데이터 소스 및 참고자료

### KMA API Hub
- **웹사이트**: https://apihub.kma.go.kr/
- **문서**: https://apihub.kma.go.kr/api/guide
- **변수**: 융합기상 데이터 (격자형 융합기상정보)

### 법정동 경계
- **출처**: 국가공간정보포털
- **웹사이트**: http://data.nsdi.go.kr/
- **데이터셋**: 법정경계(읍면동) / LSMD_ADM_SECT_UMD
- **업데이트**: 분기/연간 업데이트 확인

### 격자 좌표
- **출처**: KMA API Hub 문서 또는 지원팀
- **파일**: `sfc_grid_latlon.nc`
- **투영법**: Lambert Conformal Conic (LCC)

---

## 기여

이것은 데이터 처리 파이프라인 프로젝트입니다. 기여하려면:
1. A/B 단계 분리 유지
2. 프로덕션 신뢰성을 위한 검증/로깅 보존
3. 새로운 지리공간 소스 추가 시 매핑 생성 업데이트
4. `fusion/config.py`의 모든 설정 변경 문서화

---

## 라이선스

프로젝트 라이선스 및 약관 미정.

---

## 연락처

KMA API 접근 문제: https://apihub.kma.go.kr/support

파이프라인 문제: `data/fusion_raw/_validation_logs/`의 검증 로그 확인
