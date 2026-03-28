# 융합기상정보 시스템 (Fusion Weather)

기상청 융합기상관측 API를 통해 격자 단위 기상 데이터를 다운로드하고, **행정동(HJD)** 또는 **법정동(BJD)** 단위로 공간 집계하는 파이프라인입니다.

## 개요

- **데이터 소스**: [기상청 API허브](https://apihub.kma.go.kr/) - 융합기상 탭
- **API 엔드포인트**:
  | 유형 | 도메인 | 용도 |
  |------|--------|------|
  | `org` (기본) | `apihub-org.kma.go.kr` | 기관용 대용량 처리 |
  | `public` | `apihub.kma.go.kr` | 일반 개인 API키 |
- **공간 집계 기준**:
  | 유형 | 설명 | Shapefile | 매핑 캐시 파일 | 출력 컬럼 |
  |------|------|-----------|---------------|-----------|
  | `hjd` (기본) | 행정동 | `bnd_dong_00_2022_4Q` | `grid_to_hjd.parquet` | `HJD_CD`, `HJD_NM` |
  | `bjd` | 법정동(읍면동) | `LSMD_ADM_SECT_UMD_*` (17개 시도) | `grid_to_emd_umd.parquet` | `EMD_CD`, `EMD_NM` |
- **지원 변수**:
  | 변수 키 | 설명 | 단위 | 시간 간격 |
  |---------|------|------|-----------|
  | `ta` | 기온 | ℃ | 1시간 |
  | `rn_60m` | 60분 누적 강수량 | mm | 1시간 |
  | `sd_3hr` | 3시간 신적설 | cm | 3시간 |

## 폴더 구조

```
fusion_weather/
├── ../.env                 # 통합 인증키 (fusion_weather_authKey) [루트]
├── run_download.py         # [A 단계] raw 다운로드/캐시 생성
├── run_process.py          # [B 단계] 후처리 (피벗/공간집계/출력)
├── fusion/                 # 핵심 패키지
│   ├── __init__.py
│   ├── config.py           # 설정 (경로, API, 변수 정의)
│   ├── download.py         # API 다운로드/파싱
│   ├── geocode.py          # 격자-지역 매핑 (HJD + BJD)
│   ├── aggregate.py        # 시간/공간 집계
│   └── pipeline.py         # 전체 파이프라인 오케스트레이션
├── data/
│   ├── geodata/            # 격자 좌표 NetCDF, 매핑 캐시 파일
│   │   ├── sfc_grid_latlon.nc      # 격자 위경도 좌표 (2049x2049, ~4.2M 격자점)
│   │   ├── grid_to_hjd.parquet     # [자동생성] 행정동 매핑 캐시
│   │   └── grid_to_emd_umd.parquet # [자동생성] 법정동 매핑 캐시
│   ├── geodata_hjd/        # 행정동 Shapefile
│   ├── geodata_umd/        # 법정동(읍면동) Shapefile (17개 시도)
│   ├── fusion_raw/         # [A] raw 캐시 (parquet)
│   ├── fusion_interim/     # [B] 중간 결과 (parquet)
│   └── fusion_output/      # [B] 최종 출력 (csv)
└── README_KR.md            # 이 문서
```

## 설치 및 환경 구성

### 1. Conda 환경 생성

```bash
cd fusion_weather
conda env create -f environment.yml
conda activate kma_api
```

또는 pip:

```bash
pip install -r requirements.txt
```

### 2. API 인증키 설정

**프로젝트 루트**의 `.env` 파일에서 통합 관리합니다:

```env
# 프로젝트 루트/.env
asos_authKey=YOUR_ASOS_KEY
fusion_weather_authKey=YOUR_FUSION_KEY

# 동적 데이터 저장 경로 (raw/interim/output)
# 미설정 시 project_root/data 사용 (프로젝트 내 저장)
FUSION_DATA_ROOT=E:\kma
```

- `fusion_weather_authKey`: 기상청 API 인증키 ([API허브](https://apihub.kma.go.kr/)에서 발급)
- `FUSION_DATA_ROOT`: 다운로드/중간/출력 파일 저장 경로. 설정하면 `--output-path`를 매번 지정할 필요 없음

**경로 우선순위:** `--output-path` CLI 인자 > `.env`의 `FUSION_DATA_ROOT` > 기본값(`project_root/data`)

### 3. Shapefile 다운로드

사용하려는 `--region-type`에 해당하는 Shapefile만 준비하면 됩니다.

#### 행정동 (HJD) — `--region-type hjd` 사용 시 필요

행정동 경계 Shapefile(2022년 4분기)을 `data/geodata_hjd/`에 배치합니다.

- **다운로드**: [Google Drive](https://drive.google.com/file/d/1OHMMUa5lezsSURUztnVS4t1YJYeNKndZ/view?usp=drive_link)

```
data/geodata_hjd/
└── bnd_dong_00_2022_4Q/
    ├── bnd_dong_00_2022_4Q.shp
    ├── bnd_dong_00_2022_4Q.shx
    ├── bnd_dong_00_2022_4Q.dbf
    ├── bnd_dong_00_2022_4Q.cpg
    └── bnd_dong_00_2022_4Q.prj
```

#### 법정동 (BJD/UMD) — `--region-type bjd` 사용 시 필요

국가공간정보포털에서 "법정경계(읍면동)" Shapefile을 다운로드하여 `data/geodata_umd/`에 배치합니다.

```
data/geodata_umd/
├── LSMD_ADM_SECT_UMD_서울/LSMD_ADM_SECT_UMD_11_*.shp
├── LSMD_ADM_SECT_UMD_경기/LSMD_ADM_SECT_UMD_41_*.shp
└── ... (17개 시도)
```

## 실행 방법

### A 단계: Raw 데이터 다운로드

API에서 격자 데이터를 받아 `data/fusion_raw/`에 parquet 캐시로 저장합니다.
이 단계는 공간 집계를 수행하지 않으므로 Shapefile이나 `--region-type`이 필요 없습니다.

```bash
# 연/월 범위 다운로드
python fusion_weather/run_download.py \
    --start-year 2024 --end-year 2024 \
    --start-month 1 --end-month 12 \
    --variables ta,rn_60m \
    --max-workers 4

# 하루만 테스트
python fusion_weather/run_download.py --test-day 20241128 --variables ta,rn_60m,sd_3hr

# 별도 경로에 저장
python fusion_weather/run_download.py --output-path E:\kma --start-year 2024 --end-year 2024

# 일반(public) API키로 다운로드
python fusion_weather/run_download.py --api-type public --test-day 20241128 --variables ta
```

**A 단계 출력 구조:**
```
data/fusion_raw/
└── 2024/
    └── 01/
        ├── ta_20240101_parsed.parquet       # 기온 (grid_idx, date, hour, value)
        ├── rn_60m_20240101_parsed.parquet   # 강수량
        └── sd_3hr_20240101_parsed.parquet   # 적설
```

### B 단계: 후처리 (공간 집계)

A 단계에서 생성된 raw 캐시를 읽어 시간 피벗 → 공간 집계 → CSV 출력을 수행합니다.
**이 단계는 다운로드를 수행하지 않으며**, API 인증키가 없어도 실행 가능합니다.

```bash
# 행정동 기준 (기본)
python fusion_weather/run_process.py \
    --start-year 2024 --end-year 2024 \
    --start-month 1 --end-month 12 \
    --variables ta,rn_60m,sd_3hr

# 법정동 기준
python fusion_weather/run_process.py \
    --region-type bjd \
    --start-year 2024 --end-year 2024 \
    --variables ta,rn_60m

# 행정동 + 법정동 둘 다 (각각 별도 파일로 출력)
python fusion_weather/run_process.py \
    --region-type both \
    --test-day 20241128 \
    --variables ta,rn_60m,sd_3hr

# 매핑 테이블 강제 재생성
python fusion_weather/run_process.py --force-rebuild-mapping --test-day 20241128

# 커스텀 경로에서 처리 (A단계의 --output-path와 동일하게 지정)
python fusion_weather/run_process.py --output-path E:\kma --start-year 2024 --end-year 2024
```

## 처리 파이프라인 상세

### A 단계: 다운로드 → Raw 캐시

```
API 호출 (변수별, 시간별)
  → ASCII 응답 파싱 (격자값 추출, 결측 처리)
  → 검증 (기대 격자 수 일치 확인, 실패 시 재시도)
  → data/fusion_raw/{YYYY}/{MM}/{var}_{date}_parsed.parquet 저장
```

- 각 parquet 파일은 하루치 한 변수의 격자 데이터를 담고 있습니다.
- 컬럼: `grid_idx`(격자 번호 0~4.2M), `date`, `hour`, `value`
- 실패 시 최대 3회 재시도(exponential backoff), 로그는 `fusion_raw/_validation_logs/`에 저장

### B 단계: Raw 캐시 → 공간 집계 → CSV 출력

```
1. 매핑 테이블 생성/로드
   ├── --region-type hjd → grid_to_hjd.parquet    (격자→행정동)
   ├── --region-type bjd → grid_to_emd_umd.parquet (격자→법정동)
   └── --region-type both → 위 두 파일 모두 생성

2. 변수별 처리 (ta, rn_60m, sd_3hr 각각)
   ├── Raw parquet 로드 (grid_idx, date, hour, value)
   ├── 시간 피벗: 행=격자, 열=시간대
   │     기온: t0001, t0102, ..., t2324 (24개)
   │     강수: p0001, p0102, ..., p2324 (24개)
   │     적설: s0003, s0306, ..., s2124 (8개)
   └── 공간 집계: 격자→지역 매핑 조인 후 지역별 평균(mean)

3. 변수 병합 → CSV 출력
```

### 매핑 테이블 생성 과정

매핑 테이블은 **격자점(약 4.2M개)이 어느 행정구역에 속하는지**를 1:1로 대응시킨 테이블입니다.
행정동과 법정동은 **별도의 매핑 파일**로 각각 생성됩니다.

```
sfc_grid_latlon.nc (격자 위경도)
  → 격자점을 Point geometry로 변환 (EPSG:4326)
  → Shapefile의 폴리곤과 공간 조인 (Point-in-Polygon, geopandas.sjoin)
  → 매핑 결과 저장

  행정동: geodata_hjd/bnd_dong_00_2022_4Q.shp → grid_to_hjd.parquet
          컬럼: grid_idx, lat, lon, HJD_CD, HJD_NM

  법정동: geodata_umd/LSMD_ADM_SECT_UMD_*.shp (17개 시도 병합) → grid_to_emd_umd.parquet
          컬럼: grid_idx, lat, lon, EMD_CD, EMD_NM
```

- 최초 실행 시 자동 생성되며, 이후에는 캐시된 parquet를 재사용합니다.
- `--force-rebuild-mapping` 옵션으로 강제 재생성할 수 있습니다.
- 해양/북한/경계 밖 격자점은 매핑 실패(`NaN`)로 처리되며, 집계 시 자동 제외됩니다.

### 출력 파일 구조

모든 출력 파일에는 `--region-type`에 따라 접미사(`_hjd`, `_bjd`, `_both`)가 붙습니다.

```
data/fusion_output/
├── 2024/
│   ├── fusion_202401_hjd.csv       # 행정동 기준
│   ├── fusion_202401_bjd.csv       # 법정동 기준
│   ├── fusion_202401_both.csv      # 통합 (HJD+BJD)
│   └── ...
├── fusion_weather_2024_hjd.csv     # 연간 (행정동)
├── fusion_weather_2024_bjd.csv     # 연간 (법정동)
└── fusion_weather_2024_both.csv    # 연간 (통합)
```

**행정동(`_hjd`) 출력 CSV 컬럼:**
```
date     | HJD_CD     | t0001 | t0102 | ... | t2324 | p0001 | ... | s0003 | ...
20240101 | 1168064000 | -2.3  | -2.5  | ... | -1.8  | 0.0   | ... | 0.5   | ...
```

**법정동(`_bjd`) 출력 CSV 컬럼:**
```
date     | EMD_CD     | t0001 | t0102 | ... | t2324 | p0001 | ... | s0003 | ...
20240101 | 1168010100 | -2.1  | -2.4  | ... | -1.7  | 0.0   | ... | 0.3   | ...
```

**통합(`_both`) 출력 CSV 컬럼 — 하나의 파일에 HJD_CD와 EMD_CD 모두 포함:**
```
date     | HJD_CD     | EMD_CD     | t0001 | t0102 | ... | p0001 | ... | s0003 | ...
20240101 | 1168064000 | 1168010100 | -2.2  | -2.5  | ... | 0.0   | ... | 0.4   | ...
```

통합 모드는 격자점별 (HJD_CD, EMD_CD) 쌍을 기준으로 집계합니다.
동일 행정동 안에 여러 법정동이 있거나 그 반대인 경우, 각 조합별로 별도 행이 생성됩니다.

## 참고 사항

- 적설(`sd_3hr`)은 2020년부터 제공되며, 여름철(6~9월)에는 생산되지 않습니다.
- A 단계와 B 단계는 독립적으로 실행 가능합니다 (A만 먼저 실행하고, 나중에 B를 수행).
- 동일한 raw 캐시에 대해 `--region-type hjd`와 `bjd`를 각각 실행할 수 있습니다 (다운로드를 다시 할 필요 없음).
- 행정동과 법정동은 서로 다른 행정구역 체계이므로, 동일 격자점이 서로 다른 HJD_CD와 EMD_CD에 매핑될 수 있습니다.
