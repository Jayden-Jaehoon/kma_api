# kma_api

기상청 API에서 연도별 일자료를 다운로드하고 전처리하여 CSV로 저장하는 간단한 파이프라인입니다. 또한 관측소(지점) 정보를 내려받는 도구도 포함합니다.

- 실행 진입점: `run.py`
- 데이터 전처리: `process_data.py`
- 관측소 정보 다운로드: `get_station_info.py`
- 데이터/메타 파일: `data/`


## 1) 설치 및 실행 환경

최근 NumPy 2.x로 인한 바이너리 호환성 문제(특히 pandas, matplotlib 등 C 확장 모듈)가 보고되어 있습니다. 본 프로젝트는 의존성 버전을 고정해 안정적으로 동작하도록 구성했습니다.

아래 중 하나의 방법으로 환경을 준비하세요.

### 방법 A) Conda 환경 사용(권장)

```bash
conda env create -f environment.yml
conda activate kma-api
```

### 방법 B) Python venv + pip 사용

```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -U pip
pip install -r requirements.txt
```


## 2) .env 파일 설정(중요)

KMA API 호출에는 인증키가 필요합니다. 프로젝트 루트에 `.env` 파일을 만들고 아래처럼 입력하세요.

```env
authKey=발급받은_인증키를_여기에
```

- 키 이름은 정확히 `authKey`여야 합니다. 코드에서 `os.getenv("authKey")`로 읽습니다.
- 인증키 발급/관리 방법은 기상청 API 포털 안내를 참고하세요.


## 3) 프로젝트 구조

```
kma_api/
├── run.py                        # 실행 진입점 (연도 범위 일자료 다운로드+전처리)
├── process_data.py               # 다운로드/파싱/전처리 로직
├── get_station_info.py           # 관측소(지점) 원본 정보 다운로드 스크립트
├── requirements.txt              # pip 의존성 고정 (numpy<2, pandas==1.5.3 등)
├── environment.yml               # conda 환경 정의
└── data/
    ├── raw_data/                 # 원본 텍스트 저장 위치(자동 생성)
    ├── post_process_data/        # 전처리된 CSV 저장 위치(자동 생성)
    ├── station_info_structured.csv  # 지점 메타(지점번호→행정코드 매핑용)
    ├── station_info_SFC.md       # 지점 정보 설명 문서(참고)
    └── weather_data_stn.md       # 원본 필드 설명(참고)
```


## 4) 빠른 시작: 연도 범위 처리(run.py)

`.env`가 준비되었다면 다음 한 줄로 예제를 실행할 수 있습니다.

```bash
python run.py
```

기본 설정(예시):
- 데이터 루트: `/Users/jaehoon/liminal_ego/git_clones/kma_api/data`
- 연도 범위: 1970 ~ 1972
- 지점: `stn="0"` (전체 지점)

실행 시
- `data/raw_data/`에 연도별 원본 텍스트가 저장되고,
- `data/post_process_data/`에 연도별 CSV가 생성됩니다.

출력 예시
- `data/post_process_data/weather_data_stn0_1970.csv`
- `data/post_process_data/weather_data_stn0_1971.csv`
- `data/post_process_data/weather_data_stn0_1972.csv`

실행 파라미터를 바꾸려면 `run.py` 내부의 `start_year`, `end_year`, `stn`, `BASE_DATA_DIR`를 수정하세요.


## 5) 세부 동작(process_data.py)

핵심 함수 요약:

- `download_year_txt(auth_key, base_data_dir, year, stn="0")`
  - 지정 연도의 원본 텍스트(도움말 포함)를 KMA API로부터 받아 `data/raw_data`에 저장합니다.

- `process_raw_txt_to_csv(input_txt_path, output_csv_path)`
  - 원본 텍스트에서 주석/도움말을 제거한 뒤 고정폭(FWF) 파싱으로 DataFrame을 만들고 CSV로 저장합니다.
  - 특수 처리:
    - 파일 끝에 종종 포함되는 `#7777END, ...` 라인을 제거합니다.
    - 결과 DF의 `STN`(지점번호)을 기준으로 `data/station_info_structured.csv`의 `STN_ID→LAW_ID`를 매핑해 `LAW_ID` 컬럼을 추가합니다. 매핑 실패 시 파이프라인은 계속 동작합니다.

- `run_year_range(auth_key, base_data_dir, start_year, end_year, stn="0")`
  - 지정 범위의 연도를 순회하며 다운로드→전처리를 수행합니다.

출력 CSV 인코딩은 `utf-8-sig`입니다(엑셀 호환성 고려).


## 6) 관측소(지점) 정보 다운로드(get_station_info.py)

관측소 메타 정보를 원본 그대로 내려받아 보관할 수 있습니다. 기본은 지상관측소(ASOS, `SFC`).

### 스크립트로 바로 실행

```bash
python get_station_info.py
```

기본 동작:
- `.env`의 `authKey`를 읽습니다.
- `data/` 폴더에 `station_info_SFC_YYYYMMDD.txt` 형식의 파일을 저장합니다.

### 코드에서 사용

```python
from _VER1.get_station_info import download_station_info, download_all_station_info

# 단일 종류(SFC) 다운로드
download_station_info(inf_type="SFC", auth_key=auth_key, save_dir="../data")

# 여러 종류를 확장하고 싶다면 download_all_station_info 내부의 station_types를 늘리세요.
```


## 7) 파라미터 참고(주요 인자)

- `tm1`, `tm2`: 조회 시작/종료일(예: `YYYYMMDD`) — 본 프로젝트에서는 연 단위로 자동 세팅합니다.
- `stn`: 지점번호. `"0"`이면 전체 지점, 특정 지점만 원하면 숫자 또는 콜론 구분자 사용(예: `"108"`, `"108:159"`).
- `help`: `1`이면 원본 끝에 도움말 섹션 포함(본 파이프라인은 도움말을 제거한 후 파싱합니다).
- `authKey`: 필수. `.env`에 저장합니다.


## 8) 자주 묻는 질문(FAQ) / 트러블슈팅

- NumPy/Pandas 관련 ImportError, `_ARRAY_API not found`, `numpy.dtype size changed` 등의 오류가 나요.
  - 의존성 버전을 고정하세요. 본 프로젝트의 `requirements.txt` 또는 `environment.yml`을 그대로 사용하면 해결됩니다.
  - 이미 만든 환경에서 해결하려면: `pip install "numpy<2" "pandas==1.5.3"`.

- `.env`가 없거나 `authKey`가 비어 있대요.
  - 프로젝트 루트에 `.env` 파일을 만들고 `authKey=...`를 채우세요. 키 이름은 대소문자 포함 동일해야 합니다.

- 출력 CSV에 `LAW_ID`가 비어있는 행이 있어요.
  - `STN`에 해당하는 매핑이 `data/station_info_structured.csv`에 없는 경우입니다. 해당 파일을 갱신하거나 필요한 지점을 추가하세요.

- 연도 범위/지점을 바꾸고 싶어요.
  - `run.py`에서 `start_year`, `end_year`, `stn` 값을 수정하세요. 또는 Python 셸에서 `run_year_range(...)`를 직접 호출하세요.


## 9) 라이선스 / 크레딧

- 데이터 출처: 기상청(APIHub)
- 본 저장소의 스크립트는 예제/연구용으로 제공됩니다.