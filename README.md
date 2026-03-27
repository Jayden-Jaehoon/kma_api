# KMA API Data Pipeline

기상청 API를 활용한 두 가지 독립적인 기상 데이터 수집·처리 시스템을 포함하는 레포지토리입니다.
각 시스템은 자체 폴더 안에서 독립적인 데이터 디렉토리와 실행 파일을 관리하며, 인증키는 루트 `.env` 하나로 통합 관리합니다.

---

## 프로젝트 구조

```
kma_api/
├── asos/                   # 1. 지상관측(ASOS) 시스템
│   ├── run.py              #    실행 스크립트
│   ├── process_data.py     #    데이터 파싱/전처리
│   ├── get_station_info.py #    관측소 정보 다운로드
│   └── data/               #    원본/가공 데이터
│
├── fusion_weather/         # 2. 융합기상정보 시스템
│   ├── run_download.py     #    [A 단계] raw 다운로드
│   ├── run_process.py      #    [B 단계] 행정동 집계/후처리
│   ├── fusion/             #    핵심 파이프라인 패키지
│   └── data/               #    격자 좌표, Shapefile, 캐시, 출력
│
├── .env                    # 통합 인증키 (asos_authKey, fusion_weather_authKey)
├── requirements.txt        # pip 의존성
├── environment.yml         # conda 환경
└── README.md               # 이 문서
```

---

## 1. ASOS — 지상관측 데이터 시스템

| 항목 | 내용 |
|------|------|
| **위치** | `asos/` |
| **API** | [기상청 API허브 — 지상관측 탭](https://apihub.kma.go.kr/) |
| **엔드포인트** | `https://apihub.kma.go.kr/api/typ01/url/kma_sfctm2.php` |
| **기능** | 관측소(ASOS) 일자료 다운로드, 고정폭 텍스트 → CSV 변환, 관측소 정보 매핑 |

### 빠른 시작

```bash
# 1. 루트 .env에 인증키 설정 (아래 "인증키 발급" 섹션 참고)

# 2. 실행
python asos/run.py
```

자세한 내용은 [`asos/README_KR.md`](asos/README_KR.md) / [`asos/README_EN.md`](asos/README_EN.md) 참고

---

## 2. Fusion Weather — 지상융합기상관측 시스템

| 항목 | 내용 |
|------|------|
| **위치** | `fusion_weather/` |
| **API** | [기상청 API허브 — 융합기상 탭](https://apihub.kma.go.kr/) |
| **엔드포인트** | `https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-sfc_obs_nc_api` |
| **기능** | 격자 기상 데이터 다운로드, 시간 피벗, 격자→행정동 공간 집계, 변수 병합 |
| **공간 집계** | 행정동 경계 (2022년 4분기 Shapefile 기준) |

### 빠른 시작

```bash
# 1. 루트 .env에 인증키 설정 (아래 "인증키 발급" 섹션 참고)

# 2. 행정동 Shapefile 다운로드 후 fusion_weather/data/geodata_hjd/ 에 배치
#    다운로드: https://drive.google.com/file/d/1OHMMUa5lezsSURUztnVS4t1YJYeNKndZ/view

# 3. A단계 — Raw 다운로드
python fusion_weather/run_download.py \
    --start-year 2024 --end-year 2024 \
    --variables ta,rn_60m

# 4. B단계 — 행정동 집계
python fusion_weather/run_process.py \
    --start-year 2024 --end-year 2024 \
    --variables ta,rn_60m,sd_3hr
```

자세한 내용은 [`fusion_weather/README_KR.md`](fusion_weather/README_KR.md) / [`fusion_weather/README_EN.md`](fusion_weather/README_EN.md) 참고

---

## 환경 설정

루트의 `environment.yml` / `requirements.txt` 하나로 두 시스템(asos + fusion_weather)을 모두 실행할 수 있습니다.

### Conda

```bash
conda env create -f environment.yml
conda activate kma-api
```

### pip

```bash
pip install -r requirements.txt
```

---

## 인증키 발급

두 시스템 모두 [기상청 API허브](https://apihub.kma.go.kr/)에서 발급받은 `authKey`가 필요합니다.
**루트 `.env` 파일 하나**에서 통합 관리합니다:

```
# .env (프로젝트 루트)
asos_authKey=YOUR_ASOS_KEY
fusion_weather_authKey=YOUR_FUSION_KEY
```

동일한 키를 사용해도 되고, 시스템별로 다른 키를 사용해도 됩니다.
