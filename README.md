# KMA API Data Pipeline

기상청 API를 활용한 두 가지 독립적인 기상 데이터 수집/처리 파이프라인입니다.
각 시스템은 자체 폴더에서 독립적으로 동작하며, 인증키는 루트 `.env` 하나로 통합 관리합니다.

## 프로젝트 구조

```
kma_api/
├── asos/                   # 지상관측(ASOS) 시스템
│   ├── run.py              #   실행 스크립트
│   ├── process_data.py     #   데이터 파싱/전처리
│   ├── get_station_info.py #   관측소 정보 다운로드
│   └── data/               #   원본/가공 데이터
│
├── fusion_weather/         # 융합기상정보 시스템
│   ├── run_download.py     #   [A 단계] raw 다운로드
│   ├── run_process.py      #   [B 단계] 공간 집계/후처리
│   ├── fusion/             #   핵심 파이프라인 패키지
│   └── data/               #   격자 좌표, Shapefile 등 정적 데이터
│
├── .env                    # 통합 인증키 + 경로 설정
├── .env.example            # .env 템플릿
├── requirements.txt        # pip 의존성
├── environment.yml         # conda 환경
└── README_KR.md            # 이 문서
```

---

## ASOS — 지상관측 데이터

| 항목 | 내용 |
|------|------|
| **위치** | `asos/` |
| **API** | [기상청 API허브 — 지상관측](https://apihub.kma.go.kr/) |
| **기능** | 관측소(ASOS) 일자료 다운로드, 고정폭 텍스트 → CSV 변환, 관측소 정보 매핑 |

```bash
python asos/run.py
```

자세한 내용은 [`asos/README_KR.md`](asos/README_KR.md) 참고

---

## Fusion Weather — 융합기상관측 데이터

| 항목 | 내용 |
|------|------|
| **위치** | `fusion_weather/` |
| **API** | [기상청 API허브 — 융합기상](https://apihub.kma.go.kr/) |
| **기능** | 격자 기상 데이터 다운로드 → 시간 피벗 → 행정동/법정동 공간 집계 → CSV 출력 |
| **지원 변수** | 기온(`ta`), 강수량(`rn_60m`), 신적설(`sd_3hr`) |
| **공간 집계** | 행정동(`hjd`), 법정동(`bjd`), 통합(`both`) |

```bash
# A단계: Raw 다운로드
python fusion_weather/run_download.py \
    --start-year 2024 --end-year 2024 \
    --variables ta,rn_60m

# B단계: 공간 집계
python fusion_weather/run_process.py \
    --region-type hjd \
    --start-year 2024 --end-year 2024 \
    --variables ta,rn_60m,sd_3hr
```

자세한 내용은 [`fusion_weather/README_KR.md`](fusion_weather/README_KR.md) 참고

---

## 환경 설정

### Conda (권장)

```bash
conda env create -f environment.yml
conda activate kma-api
```

### pip

```bash
pip install -r requirements.txt
```

---

## 인증키 및 경로 설정

두 시스템 모두 [기상청 API허브](https://apihub.kma.go.kr/)에서 발급받은 인증키가 필요합니다.
프로젝트 루트의 `.env` 파일에서 통합 관리합니다. (템플릿: [`.env.example`](.env.example))

```env
# 인증키
asos_authKey=YOUR_ASOS_KEY
fusion_weather_authKey=YOUR_FUSION_KEY

# 동적 데이터 저장 경로 (미설정 시 프로젝트 내 data/ 사용)
# FUSION_DATA_ROOT=E:\kma
```

동일한 키를 사용해도 되고, 시스템별로 다른 키를 사용해도 됩니다.
