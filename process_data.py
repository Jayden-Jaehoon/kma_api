import os
from itertools import dropwhile
from typing import Iterable, List, Optional

import pandas as pd


# 도움말에서 정의된 컬럼명 (56개)
WEATHER_DAILY_COLS: List[str] = [
    "TM", "STN", "WS_AVG", "WR_DAY", "WD_MAX", "WS_MAX", "WS_MAX_TM", "WD_INS", "WS_INS", "WS_INS_TM",
    "TA_AVG", "TA_MAX", "TA_MAX_TM", "TA_MIN", "TA_MIN_TM", "TD_AVG", "TS_AVG", "TG_MIN", "HM_AVG", "HM_MIN",
    "HM_MIN_TM", "PV_AVG", "EV_S", "EV_L", "FG_DUR", "PA_AVG", "PS_AVG", "PS_MAX", "PS_MAX_TM", "PS_MIN",
    "PS_MIN_TM", "CA_TOT", "SS_DAY", "SS_DUR", "SS_CMB", "SI_DAY", "SI_60M_MAX", "SI_60M_MAX_TM", "RN_DAY",
    "RN_D99", "RN_DUR", "RN_60M_MAX", "RN_60M_MAX_TM", "RN_10M_MAX", "RN_10M_MAX_TM", "RN_POW_MAX",
    "RN_POW_MAX_TM", "SD_NEW", "SD_NEW_TM", "SD_MAX", "SD_MAX_TM", "TE_05", "TE_10", "TE_15", "TE_30", "TE_50",
]


def ensure_dirs(base_data_dir: str) -> dict:
    """
    data 폴더 하위에 raw_data, post_process_data 폴더를 생성하고 경로를 반환합니다.
    """
    raw_dir = os.path.join(base_data_dir, "raw_data")
    proc_dir = os.path.join(base_data_dir, "post_process_data")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(proc_dir, exist_ok=True)
    return {"raw": raw_dir, "proc": proc_dir}


def _iter_data_lines(fp: Iterable[str]) -> Iterable[str]:
    """주석/빈 줄을 건너뛰고 실제 데이터 라인부터 반환합니다."""
    return dropwhile(lambda s: s.lstrip().startswith("#") or not s.strip(), fp)


def parse_weather_text_to_df(text: str, cols: Optional[List[str]] = None) -> pd.DataFrame:
    """
    도움말 섹션이 제거된 텍스트를 고정폭(FWF)으로 파싱하여 DataFrame으로 반환합니다.
    """
    if cols is None:
        cols = WEATHER_DAILY_COLS

    return pd.read_fwf(
        pd.io.common.StringIO(text),
        header=None,
        names=cols,
    )


def process_raw_txt_to_csv(input_txt_path: str, output_csv_path: str, cols: Optional[List[str]] = None) -> str:
    """
    원본 TXT에서 도움말(#) 구간을 제외한 데이터만 파싱하여 CSV로 저장합니다.
    """
    if cols is None:
        cols = WEATHER_DAILY_COLS

    with open(input_txt_path, "r", encoding="utf-8") as f:
        # 파일 시작 부분의 주석/빈줄은 건너뛰고, 이후 데이터 라인만 받는다
        data_lines = list(_iter_data_lines(f))

    # 일부 원본 파일의 맨 끝에 존재하는 "#7777END,...." 마커 라인을 제거한다
    # (중간에 등장하더라도 제거)
    data_lines = [ln for ln in data_lines if not ln.lstrip().startswith("#7777END")]

    raw = "".join(data_lines)

    df = parse_weather_text_to_df(raw, cols=cols)

    # 후처리: STN(지점번호) 기준으로 행정구역 코드(LAW_ID) 매핑하여 컬럼 추가
    # 참고 파일: data/station_info_structured.csv (컬럼: STN_ID, LAW_ID, ...)
    try:
        station_info_csv = os.path.join(os.path.dirname(__file__), "data", "station_info_structured.csv")
        if os.path.exists(station_info_csv):
            info_df = pd.read_csv(
                station_info_csv,
                dtype={"STN_ID": "Int64", "LAW_ID": str},
            )

            # STN을 안전하게 정수(Int64)로 변환 후 매핑
            stn_series = pd.to_numeric(df.get("STN"), errors="coerce").astype("Int64")
            # 매핑 딕셔너리 (키: int STN_ID, 값: str LAW_ID)
            mapping = {
                int(k): v
                for k, v in zip(
                    info_df["STN_ID"].dropna().astype(int),
                    info_df["LAW_ID"],
                )
            }
            df["LAW_ID"] = stn_series.map(lambda x: mapping.get(int(x)) if pd.notna(x) else None)
    except Exception:
        # 매핑 실패 시에는 조용히 넘어가고, 원본 데이터만 저장
        pass
    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")
    return output_csv_path


def yearly_processed_csv_paths(base_data_dir: str, stn: str, year: int) -> dict:
    """연도별 원본/가공 파일 경로를 생성합니다."""
    dirs = ensure_dirs(base_data_dir)
    tm1 = f"{year}0101"
    tm2 = f"{year}1231"
    raw_filename = f"weather_data_stn{stn}_{tm1}_{tm2}.txt"
    proc_filename = f"weather_data_stn{stn}_{year}.csv"
    return {
        "raw": os.path.join(dirs["raw"], raw_filename),
        "proc": os.path.join(dirs["proc"], proc_filename),
    }


def download_year_txt(auth_key: str, base_data_dir: str, year: int, stn: str = "0", timeout: int = 150) -> str:
    """
    특정 연도(YYYY)의 일자료를 도움말 포함 형태로 다운로드하여 raw_data 폴더에 저장합니다.
    """
    import requests

    tm1 = f"{year}0101"
    tm2 = f"{year}1231"
    BASE_URL = "https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd3.php"

    params = {
        "tm1": tm1,
        "tm2": tm2,
        "stn": stn,
        "help": "1",
        "authKey": auth_key,
    }

    paths = yearly_processed_csv_paths(base_data_dir, stn, year)
    raw_path = paths["raw"]
    os.makedirs(os.path.dirname(raw_path), exist_ok=True)

    resp = requests.get(BASE_URL, params=params, timeout=timeout)
    resp.raise_for_status()

    with open(raw_path, "w", encoding="utf-8") as f:
        f.write(resp.text)

    return raw_path


def process_year_file(base_data_dir: str, year: int, stn: str = "0") -> str:
    """해당 연도의 raw TXT를 가공하여 CSV로 저장합니다."""
    paths = yearly_processed_csv_paths(base_data_dir, stn, year)
    return process_raw_txt_to_csv(paths["raw"], paths["proc"])  # 반환: CSV 경로


def download_and_process_year(auth_key: str, base_data_dir: str, year: int, stn: str = "0") -> dict:
    """다운로드 + 가공을 한 번에 수행하고 경로들을 반환합니다."""
    raw = download_year_txt(auth_key=auth_key, base_data_dir=base_data_dir, year=year, stn=stn)
    proc = process_year_file(base_data_dir=base_data_dir, year=year, stn=stn)
    return {"raw": raw, "proc": proc}


def run_year_range(auth_key: str, base_data_dir: str, start_year: int, end_year: int, stn: str = "0") -> list:
    """
    start_year부터 end_year까지(포함) 연단위로 다운로드 후 post_process_data에 CSV로 저장합니다.
    """
    results = []
    for year in range(start_year, end_year + 1):
        print(f"[연도 처리] {year} (stn={stn})")
        try:
            paths = download_and_process_year(auth_key, base_data_dir, year, stn)
            print(f"  - RAW : {paths['raw']}")
            print(f"  - CSV : {paths['proc']}")
            results.append(paths)
        except Exception as e:
            print(f"  ✗ {year} 처리 실패: {e}")
    return results


if __name__ == "__main__":
    # 예시 실행: .env의 authKey 사용, 1970~1972년 처리
    import dotenv
    dotenv.load_dotenv()
    auth_key = os.getenv("authKey")
    if not auth_key:
        raise ValueError("authKey를 .env 파일에 설정해주세요")

    BASE_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
    run_year_range(auth_key, BASE_DATA_DIR, start_year=1970, end_year=1972, stn="0")