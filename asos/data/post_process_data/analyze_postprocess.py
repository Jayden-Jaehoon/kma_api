import os
import glob
from typing import Dict, List, Tuple

import pandas as pd


def _year_from_filename(path: str) -> int:
    """Extract year from filename like weather_data_stn0_1970.csv."""
    base = os.path.basename(path)
    for part in base.replace(".csv", "").split("_"):
        if part.isdigit() and len(part) == 4:
            return int(part)
    raise ValueError(f"연도를 파일명에서 찾을 수 없습니다: {path}")


def _load_station_info(repo_root: str) -> pd.DataFrame:
    info_path = os.path.join(repo_root, "data", "station_info_structured.csv")
    if not os.path.exists(info_path):
        return pd.DataFrame(columns=["STN_ID", "LAW_ID", "LAW_NM"])  # empty
    df = pd.read_csv(info_path, dtype={"STN_ID": "Int64", "LAW_ID": str, "LAW_NM": str})
    return df[["STN_ID", "LAW_ID", "LAW_NM"]]


def _expected_dates(year: int) -> pd.DatetimeIndex:
    return pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D")


def analyze_year(csv_path: str, station_info: pd.DataFrame) -> Dict:
    """Analyze a single year's CSV and return metrics and details."""
    year = _year_from_filename(csv_path)
    df = pd.read_csv(csv_path)

    # Basic columns we expect: TM (YYYYMMDD), STN
    # Parse TM safely
    tm = pd.to_datetime(pd.to_numeric(df.get("TM", pd.Series()), errors="coerce").astype("Int64"), format="%Y%m%d", errors="coerce")
    df["TM_dt"] = tm

    # Clean STN to Int64
    df["STN_clean"] = pd.to_numeric(df.get("STN", pd.Series()), errors="coerce").astype("Int64")

    # Duplicates on (STN, TM)
    dup_mask = df.duplicated(subset=["STN_clean", "TM_dt"], keep=False)
    dup_df = df.loc[dup_mask, ["STN_clean", "TM_dt"]].dropna().drop_duplicates()

    # Out-of-range dates
    exp_start, exp_end = pd.Timestamp(f"{year}-01-01"), pd.Timestamp(f"{year}-12-31")
    out_of_range_df = df.loc[df["TM_dt"].notna() & ((df["TM_dt"] < exp_start) | (df["TM_dt"] > exp_end)), ["STN_clean", "TM_dt"]]

    # Expected complete date set
    expected = _expected_dates(year)

    # Per-station completeness
    per_stn = (
        df.dropna(subset=["STN_clean", "TM_dt"])
        .groupby("STN_clean")["TM_dt"].agg(["nunique"]).rename(columns={"nunique": "days_present"})
    )
    per_stn["days_expected"] = len(expected)
    per_stn["days_missing"] = per_stn["days_expected"] - per_stn["days_present"]

    # Missing dates list per station (compact form: first 10 only for report)
    missing_examples: List[Tuple[int, int]] = []
    for stn_id, group in df.dropna(subset=["STN_clean", "TM_dt"]).groupby("STN_clean"):
        present = pd.Series(True, index=group["TM_dt"].unique())
        missing = expected.difference(present.index)
        if len(missing) > 0:
            # store count and first few examples
            per_stn.loc[stn_id, "missing_sample"] = ", ".join(missing.astype(str)[:10]) + (" ..." if len(missing) > 10 else "")
        else:
            per_stn.loc[stn_id, "missing_sample"] = ""

    # Merge station info
    per_stn = per_stn.reset_index().rename(columns={"STN_clean": "STN"})
    if not station_info.empty:
        per_stn = per_stn.merge(
            station_info.rename(columns={"STN_ID": "STN"}),
            on="STN",
            how="left",
        )

    # Station set
    stations = set(per_stn["STN"].dropna().astype(int).tolist())

    metrics = {
        "year": year,
        "station_count": len(stations),
        "full_coverage_station_count": int((per_stn["days_missing"] == 0).sum()),
        "any_missing_station_count": int((per_stn["days_missing"] > 0).sum()),
        "duplicate_records": int(len(dup_df)),
        "out_of_range_records": int(len(out_of_range_df)),
    }

    details = {
        "per_station": per_stn.sort_values(["days_missing", "STN"]).reset_index(drop=True),
        "duplicates": dup_df.sort_values(["STN_clean", "TM_dt"]).reset_index(drop=True),
        "out_of_range": out_of_range_df.sort_values(["STN_clean", "TM_dt"]).reset_index(drop=True),
        "stations": stations,
    }

    return {"metrics": metrics, "details": details}


def year_over_year_changes(analyses: List[Dict]) -> List[Dict]:
    analyses_sorted = sorted(analyses, key=lambda x: x["metrics"]["year"])
    prev_stations: set = set()
    yoy: List[Dict] = []
    for a in analyses_sorted:
        year = a["metrics"]["year"]
        stations = a["details"]["stations"]
        added = sorted(stations - prev_stations)
        removed = sorted(prev_stations - stations)
        yoy.append({"year": year, "added": added, "removed": removed})
        prev_stations = stations
    return yoy


def _md_table(df: pd.DataFrame, max_rows: int = 30) -> str:
    """Render a simple Markdown table without external deps (no tabulate)."""
    df_out = df.copy()
    if len(df_out) > max_rows:
        df_out = df_out.head(max_rows)
    cols = list(map(str, df_out.columns))
    # Header
    lines = ["| " + " | ".join(cols) + " |",
             "| " + " | ".join(["---"] * len(cols)) + " |"]
    # Rows
    for _, row in df_out.iterrows():
        vals = ["" if pd.isna(v) else str(v) for v in row.tolist()]
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


def write_reports(output_dir: str, analyses: List[Dict], station_info: pd.DataFrame) -> List[str]:
    os.makedirs(output_dir, exist_ok=True)
    written: List[str] = []

    # Summary report
    rows = []
    for a in sorted(analyses, key=lambda x: x["metrics"]["year"]):
        m = a["metrics"]
        rows.append({
            "연도": m["year"],
            "지점 수": m["station_count"],
            "전 기간 결측 없음": m["full_coverage_station_count"],
            "결측 존재 지점 수": m["any_missing_station_count"],
            "중복 레코드 수": m["duplicate_records"],
            "범위 밖 레코드 수": m["out_of_range_records"],
        })
    summary_df = pd.DataFrame(rows)

    yoy = year_over_year_changes(analyses)
    yoy_rows = []
    for item in yoy:
        yoy_rows.append({
            "연도": item["year"],
            "추가": len(item["added"]),
            "제거": len(item["removed"]),
        })
    yoy_df = pd.DataFrame(yoy_rows)

    summary_path = os.path.join(output_dir, "report_summary.md")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"# KMA 전처리 데이터 — 요약\n\n")
        f.write("## 연도별 지표\n\n")
        if not summary_df.empty:
            f.write(_md_table(summary_df, max_rows=1000))
        else:
            f.write("(데이터 없음)\n")
        f.write("\n\n## 연도 간 지점 변화\n\n")
        if not yoy_df.empty:
            f.write(_md_table(yoy_df, max_rows=1000))
        else:
            f.write("(데이터 없음)\n")

        # Detailed added/removed listing
        f.write("\n\n### 상세: 연도별 추가/제거 지점\n\n")
        for item in yoy:
            year = item["year"]
            added = item["added"]
            removed = item["removed"]
            f.write(f"#### {year}\n\n")
            if added:
                added_df = pd.DataFrame({"STN": added})
                if not station_info.empty:
                    added_df = added_df.merge(station_info.rename(columns={"STN_ID": "STN"}), on="STN", how="left")
                f.write("추가된 지점\n\n")
                f.write(_md_table(added_df, max_rows=200))
                f.write("\n\n")
            if removed:
                removed_df = pd.DataFrame({"STN": removed})
                if not station_info.empty:
                    removed_df = removed_df.merge(station_info.rename(columns={"STN_ID": "STN"}), on="STN", how="left")
                f.write("제거된 지점\n\n")
                f.write(_md_table(removed_df, max_rows=200))
                f.write("\n\n")
    written.append(summary_path)

    # Per-year reports
    for a in analyses:
        year = a["metrics"]["year"]
        per_stn = a["details"]["per_station"].copy()
        per_stn_cols = [
            "STN", "LAW_ID", "LAW_NM", "days_present", "days_expected", "days_missing", "missing_sample"
        ]
        for c in per_stn_cols:
            if c not in per_stn.columns:
                per_stn[c] = ""
        per_stn = per_stn[per_stn_cols]
        # 한국어 표기로 컬럼명 변경 (표시용)
        per_stn_display = per_stn.rename(columns={
            "STN": "지점(STN)",
            "LAW_ID": "법정동코드(LAW_ID)",
            "LAW_NM": "법정동명(LAW_NM)",
            "days_present": "관측일수",
            "days_expected": "기대일수",
            "days_missing": "결측일수",
            "missing_sample": "결측 예시",
        })
        duplicates = a["details"]["duplicates"].rename(columns={"STN_clean": "STN"})
        out_of_range = a["details"]["out_of_range"].rename(columns={"STN_clean": "STN"})

        year_path = os.path.join(output_dir, f"report_{year}.md")
        with open(year_path, "w", encoding="utf-8") as f:
            f.write(f"# {year}년 — 데이터 품질 및 커버리지\n\n")
            m = a["metrics"]
            f.write("## 요약\n\n")
            f.write(
                f"- 지점 수: {m['station_count']}\n\n"
                f"- 전 기간 커버리지 완전 지점 수: {m['full_coverage_station_count']}\n\n"
                f"- 결측 존재 지점 수: {m['any_missing_station_count']}\n\n"
                f"- 중복 (STN, TM) 레코드: {m['duplicate_records']}\n\n"
                f"- 범위 밖 날짜 레코드: {m['out_of_range_records']}\n\n"
            )

            f.write("## 지점별 커버리지\n\n")
            if not per_stn_display.empty:
                f.write(_md_table(per_stn_display, max_rows=2000))
            else:
                f.write("(데이터 없음)\n")

            f.write("\n\n## 중복 레코드 (STN, TM)\n\n")
            if not duplicates.empty:
                f.write(_md_table(duplicates.rename(columns={"TM_dt": "TM"}).rename(columns={"STN": "지점(STN)", "TM": "날짜(TM)"}), max_rows=100))
            else:
                f.write("(없음)\n")

            f.write("\n\n## 범위 밖 날짜\n\n")
            if not out_of_range.empty:
                f.write(_md_table(out_of_range.rename(columns={"TM_dt": "TM"}).rename(columns={"STN": "지점(STN)", "TM": "날짜(TM)"}), max_rows=100))
            else:
                f.write("(없음)\n")
        written.append(year_path)

    return written


def run_analysis(post_dir: str) -> List[str]:
    """Scan post_process_data folder, analyze all yearly CSVs, write markdown reports, return paths."""
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    station_info = _load_station_info(repo_root)

    csv_paths = sorted(glob.glob(os.path.join(post_dir, "weather_data_stn*_*.csv")))
    analyses: List[Dict] = []
    for p in csv_paths:
        try:
            analyses.append(analyze_year(p, station_info))
        except Exception as e:
            print(f"경고: {p} 분석 중 오류 발생: {e}")

    if not analyses:
        print("분석할 CSV가 없습니다.")
        return []

    # 출력 경로를 하위 report 폴더로 변경 (기존 영문 보고서는 유지)
    report_dir = os.path.join(post_dir, "report")
    written = write_reports(report_dir, analyses, station_info)
    print("다음 리포트를 생성했습니다:")
    for w in written:
        print(f" - {w}")
    return written


if __name__ == "__main__":
    # Default: this file is placed in data/post_process_data; analyze sibling CSVs
    here = os.path.dirname(os.path.abspath(__file__))
    run_analysis(here)
