# 융합기상정보 시스템 (Fusion Weather)

기상청 융합기상관측 API를 통해 격자 단위 기상 데이터를 다운로드하고, **행정동** 단위로 공간 집계하는 파이프라인입니다.

## 개요

- **데이터 소스**: [기상청 API허브](https://apihub.kma.go.kr/) - 융합기상 탭
- **API 엔드포인트**: `https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-sfc_obs_nc_api`
- **공간 집계 기준**: 행정동 경계 (2022년 4분기 Shapefile 기준)
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
│   ├── geocode.py          # 격자-행정동 매핑
│   ├── aggregate.py        # 시간/공간 집계
│   └── pipeline.py         # 전체 파이프라인 오케스트레이션
├── data/
│   ├── geodata/            # 격자 좌표 NetCDF, 매핑 파일
│   ├── geodata_hjd/        # 행정동 Shapefile (별도 다운로드 필요)
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

### 3. 행정동 Shapefile 다운로드

행정동 경계 Shapefile(2022년 4분기)을 다운로드하여 `data/geodata_hjd/` 폴더에 배치합니다.

- **다운로드 링크**: [Google Drive](https://drive.google.com/file/d/1OHMMUa5lezsSURUztnVS4t1YJYeNKndZ/view?usp=drive_link)

배치 후 구조:
```
data/geodata_hjd/
├── bnd_dong_00_2022_4Q/        ← 행정동 (사용)
│   ├── bnd_dong_00_2022_4Q.shp
│   ├── bnd_dong_00_2022_4Q.shx
│   ├── bnd_dong_00_2022_4Q.dbf
│   ├── bnd_dong_00_2022_4Q.cpg
│   └── bnd_dong_00_2022_4Q.prj
├── bnd_sigungu_00_2022_4Q/     (시군구 – 미사용)
└── bnd_sido_00_2022_4Q/        (시도 – 미사용)
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
```

### B 단계: 후처리 (행정동 집계)

```bash
# 연/월 범위 후처리
python fusion_weather/run_process.py \
    --start-year 2024 --end-year 2024 \
    --start-month 1 --end-month 12 \
    --variables ta,rn_60m,sd_3hr

# 행정동 매핑 강제 재생성
python fusion_weather/run_process.py --force-rebuild-mapping --test-day 20241128
```

## 처리 파이프라인

```
[A 단계] API 호출 → 파싱 → data/fusion_raw/{YYYY}/{MM}/{var}_{date}_parsed.parquet
    ↓
[B 단계] 캐시 로드 → 시간 피벗 → 격자→행정동 공간집계 → 변수 병합
    ↓
출력: data/fusion_output/{YYYY}/{MM}/fusion_{date}.csv
```

## 참고 사항

- 적설(`sd_3hr`)은 2020년부터 제공되며, 여름철(6~9월)에는 생산되지 않습니다.
- A 단계와 B 단계는 독립적으로 실행 가능합니다 (A만 먼저 실행하고, 나중에 B를 수행).
- 행정동 매핑 테이블은 최초 1회만 생성되며, 이후 캐시(`grid_to_hjd.parquet`)를 재사용합니다.
