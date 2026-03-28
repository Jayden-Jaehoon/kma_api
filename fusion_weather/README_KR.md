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
  | 유형 | 설명 | Shapefile | 출력 컬럼 |
  |------|------|-----------|-----------|
  | `hjd` (기본) | 행정동 | `bnd_dong_00_2022_4Q` | `HJD_CD`, `HJD_NM` |
  | `bjd` | 법정동(읍면동) | `LSMD_ADM_SECT_UMD_*` (17개 시도) | `EMD_CD`, `EMD_NM` |
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
│   ├── geodata_hjd/        # 행정동 Shapefile
│   ├── geodata_umd/        # 법정동(읍면동) Shapefile
│   ├── fusion_raw/         # [A] raw 캐시 (parquet)
│   ├── fusion_interim/     # [B] 중간 결과
│   └── fusion_output/      # [B] 최종 출력
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
```

융합기상 시스템은 `fusion_weather_authKey`를 사용합니다.
인증키는 [기상청 API허브](https://apihub.kma.go.kr/)에서 발급받을 수 있습니다.

### 3. Shapefile 다운로드

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

### B 단계: 후처리 (공간 집계)

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

# 행정동 + 법정동 둘 다
python fusion_weather/run_process.py \
    --region-type both \
    --test-day 20241128 \
    --variables ta,rn_60m,sd_3hr

# 매핑 테이블 강제 재생성
python fusion_weather/run_process.py --force-rebuild-mapping --test-day 20241128

# 커스텀 경로 (A단계와 동일하게)
python fusion_weather/run_process.py --output-path E:\kma --start-year 2024 --end-year 2024
```

## 처리 파이프라인

```
[A 단계] API 호출 → 파싱 → data/fusion_raw/{YYYY}/{MM}/{var}_{date}_parsed.parquet
    ↓
[B 단계] 캐시 로드 → 시간 피벗 → 격자→지역 공간집계 → 변수 병합
    ↓
출력: data/fusion_output/{YYYY}/fusion_{YYYYMM}[_{region_type}].csv
      data/fusion_output/fusion_weather_{YYYY}[_{region_type}].csv
```

## 참고 사항

- 적설(`sd_3hr`)은 2020년부터 제공되며, 여름철(6~9월)에는 생산되지 않습니다.
- A 단계와 B 단계는 독립적으로 실행 가능합니다 (A만 먼저 실행하고, 나중에 B를 수행).
- 매핑 테이블은 최초 1회만 생성되며, 이후 캐시(`grid_to_hjd.parquet` / `grid_to_emd_umd.parquet`)를 재사용합니다.
- `--region-type both`로 실행하면 HJD/BJD 각각 별도 파일로 출력됩니다.
