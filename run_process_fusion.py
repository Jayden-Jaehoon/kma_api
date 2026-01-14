"""융합기상정보 후처리 전용 스크립트 (B 단계)

목표
----
- A 단계에서 생성된 raw 캐시(`data/fusion_raw/.../*_parsed.parquet`)만 사용해
  피벗/공간집계/변수 병합/출력을 수행합니다.
- 이 스크립트는 **다운로드를 절대 수행하지 않습니다.**

정책
----
- 캐시 누락 날짜/변수는 스킵(B 정책)하고, 스킵 목록을 요약 출력합니다.

예시
----
python run_process_fusion.py --start-year 2024 --end-year 2024 --start-month 1 --end-month 12 --variables ta,rn_60m,sd_3hr
python run_process_fusion.py --test-day 20241128 --variables ta,rn_60m
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from typing import List, Dict

import dotenv
import pandas as pd
from tqdm import tqdm


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="융합기상정보 후처리(B 단계) - 캐시 기반")
    p.add_argument("--start-year", type=int, default=2024)
    p.add_argument("--end-year", type=int, default=2024)
    p.add_argument("--start-month", type=int, default=1)
    p.add_argument("--end-month", type=int, default=12)
    p.add_argument("--variables", type=str, default="ta,rn_60m")
    p.add_argument("--test-day", type=str, default=None, help="테스트용 하루(YYYYMMDD)만 후처리")
    return p


def _iter_dates_for_month(year: int, month: int) -> List[str]:
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)

    first = datetime(year, month, 1)
    num_days = (next_month - first).days
    return [f"{year}{month:02d}{d:02d}" for d in range(1, num_days + 1)]


def main(argv: List[str] | None = None):
    args = _build_arg_parser().parse_args(argv)

    dotenv.load_dotenv()
    auth_key = os.getenv("authKey")
    if not auth_key:
        raise SystemExit("오류: .env에 authKey를 설정해주세요")

    variables = [v.strip() for v in args.variables.split(",") if v.strip()]
    if not variables:
        raise SystemExit("오류: variables가 비어있습니다")

    project_root = os.getcwd()

    from fusion.config import FusionConfig
    from fusion.pipeline import FusionPipeline

    config = FusionConfig(project_root=project_root)
    pipeline = FusionPipeline(auth_key=auth_key, config=config)
    pipeline.ensure_mapping()

    print("=" * 70)
    print("[B] 후처리(캐시 기반)")
    print("=" * 70)
    print("project_root:", project_root)
    print("variables:", variables)
    print("start:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print()

    skipped: Dict[str, List[str]] = {}  # date -> missing vars

    if args.test_day:
        date = args.test_day
        year = int(date[:4])
        month = int(date[4:6])

        raw_dir = os.path.join(config.fusion_raw_dir, f"{year}", f"{month:02d}")
        missing = [v for v in variables if not os.path.exists(os.path.join(raw_dir, f"{v}_{date}_parsed.parquet"))]
        if missing:
            skipped[date] = missing
            print(f"[SKIP] {date} missing_cache={missing}")
            return

        df = pipeline.process_day_from_cache(date, variables=variables, save_interim=True)
        print("rows:", len(df))
        print("columns:", list(df.columns))
        print(df.head(10).to_string(index=False))
        return

    results_year_paths: List[str] = []

    for year in range(args.start_year, args.end_year + 1):
        yearly_dfs: List[pd.DataFrame] = []

        m0 = args.start_month if year == args.start_year else 1
        m1 = args.end_month if year == args.end_year else 12

        for month in range(m0, m1 + 1):
            monthly_dfs: List[pd.DataFrame] = []
            dates = _iter_dates_for_month(year, month)

            for date in tqdm(dates, desc=f"{year}-{month:02d}"):
                raw_dir = os.path.join(config.fusion_raw_dir, f"{year}", f"{month:02d}")
                missing = [
                    v
                    for v in variables
                    if not os.path.exists(os.path.join(raw_dir, f"{v}_{date}_parsed.parquet"))
                ]
                if missing:
                    skipped[date] = missing
                    continue

                df_day = pipeline.process_day_from_cache(date, variables=variables, save_interim=True)
                if df_day is not None and len(df_day) > 0:
                    monthly_dfs.append(df_day)

            if monthly_dfs:
                month_df = pd.concat(monthly_dfs, ignore_index=True)
                output_dir = os.path.join(config.fusion_output_dir, str(year))
                os.makedirs(output_dir, exist_ok=True)
                output_path = os.path.join(output_dir, f"fusion_{year}{month:02d}.csv")
                month_df.to_csv(output_path, index=False, encoding="utf-8-sig")
                print(f"\n저장 완료: {output_path} (rows={len(month_df):,})")
                yearly_dfs.append(month_df)

        if yearly_dfs:
            year_df = pd.concat(yearly_dfs, ignore_index=True)
            output_path = os.path.join(config.fusion_output_dir, f"fusion_weather_{year}.csv")
            year_df.to_csv(output_path, index=False, encoding="utf-8-sig")
            results_year_paths.append(output_path)
            print(f"\n연도별 저장 완료: {output_path} (rows={len(year_df):,})")

    print("\n" + "=" * 70)
    print("완료")
    print("=" * 70)
    print("end:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    if results_year_paths:
        print("생성된 파일:")
        for p in results_year_paths:
            print("  -", p)

    if skipped:
        print("\n[스킵 요약(B 정책: 캐시 누락은 스킵)]")
        # 너무 길어질 수 있어 상위 30개만
        items = sorted(skipped.items())
        print(f"skipped_days: {len(items)}")
        for date, miss in items[:30]:
            print(f"- {date}: missing={miss}")
        if len(items) > 30:
            print(f"... (and {len(items) - 30} more)")


if __name__ == "__main__":
    # PyCharm/IDE 디버깅 편의를 위한 하드코딩 실행 옵션
    # - CLI로 실행할 때는 기본값(=USE_IDE_DEFAULTS=False)을 유지하세요.
    # - IDE에서 실행/디버깅할 때는 아래 값만 수정한 뒤 실행하면 됩니다.

    USE_IDE_DEFAULTS = True

    # IDE에서 Working Directory를 프로젝트 루트로 잡지 못할 때만 사용하세요.
    # (보통은 Run Configuration의 Working directory를 프로젝트 루트로 설정하는 것이 더 깔끔합니다.)
    IDE_PROJECT_ROOT = None  # 예: "/Users/jaehoon/liminal_ego/git_clones/kma_api"

    if USE_IDE_DEFAULTS:
        if IDE_PROJECT_ROOT:
            os.chdir(IDE_PROJECT_ROOT)

        # 예시 1) 하루만 후처리(캐시 기반)
        ide_argv = [
            "--test-day",
            "20241128",
            "--variables",
            "ta,rn_60m,sd_3hr",
        ]

        # 예시 2) 연/월 범위 후처리(필요하면 위 ide_argv와 교체)
        # ide_argv = [
        #     "--start-year", "2024",
        #     "--end-year", "2024",
        #     "--start-month", "6",
        #     "--end-month", "7",
        #     "--variables", "ta,rn_60m,sd_3hr",
        # ]

        main(argv=ide_argv)
    else:
        main()
