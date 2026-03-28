"""융합기상정보 후처리 전용 스크립트 (B 단계)

목표
----
- A 단계에서 생성된 raw 캐시(`data/fusion_raw/.../*_parsed.parquet`)만 사용해
  피벗/공간집계/변수 병합/출력을 수행합니다.
- 이 스크립트는 **다운로드를 절대 수행하지 않습니다.**
- 행정동(HJD) / 법정동(BJD) / 둘 다(both) 선택 가능

정책
----
- 캐시 누락 날짜/변수는 스킵(B 정책)하고, 스킵 목록을 요약 출력합니다.
- 매핑 테이블이 없으면 자동으로 생성합니다.

예시
----
python fusion_weather/run_process.py --start-year 2024 --end-year 2024 --variables ta,rn_60m,sd_3hr
python fusion_weather/run_process.py --test-day 20241128 --variables ta,rn_60m,sd_3hr --region-type both
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from typing import List, Dict

import dotenv
import pandas as pd
from tqdm import tqdm

# 실행 파일이 위치한 폴더를 기준으로 설정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="융합기상정보 후처리 (B 단계)")
    p.add_argument("--start-year", type=int, default=2024)
    p.add_argument("--end-year", type=int, default=2024)
    p.add_argument("--start-month", type=int, default=1)
    p.add_argument("--end-month", type=int, default=12)
    p.add_argument("--variables", type=str, default="ta,rn_60m")
    p.add_argument("--test-day", type=str, default=None, help="테스트용 하루(YYYYMMDD)만 후처리")
    p.add_argument("--force-rebuild-mapping", action="store_true", help="매핑 테이블 강제 재생성")
    p.add_argument("--output-path", type=str, default=None, help="데이터 경로 (A단계의 --output-path와 동일하게 지정)")
    p.add_argument("--region-type", type=str, default="hjd", choices=["hjd", "bjd", "both"],
                    help="집계 단위: hjd=행정동, bjd=법정동, both=둘 다 (기본: hjd)")
    return p


def _iter_dates_for_month(year: int, month: int) -> List[str]:
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)

    first = datetime(year, month, 1)
    num_days = (next_month - first).days
    return [f"{year}{month:02d}{d:02d}" for d in range(1, num_days + 1)]


def _run_single_region(
    pipeline,
    config,
    args,
    variables: List[str],
    region_type: str,
):
    """단일 region_type에 대해 후처리 실행."""
    label = "행정동(HJD)" if region_type == "hjd" else "법정동(BJD)"
    suffix = f"_{region_type}" if region_type != "hjd" else ""

    print("=" * 70)
    print(f"[B] {label} 후처리(캐시 기반)")
    print("=" * 70)

    # 매핑 준비
    pipeline.ensure_mapping(region_type, force_rebuild=args.force_rebuild_mapping)
    mapping_df, _ = pipeline._get_region(region_type)
    id_col = "HJD_CD" if region_type == "hjd" else "EMD_CD"
    print(f"매핑 완료: {len(mapping_df):,} 격자점, {mapping_df[id_col].nunique():,} 지역")
    print("variables:", variables)
    print("start:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print()

    skipped: Dict[str, List[str]] = {}

    if args.test_day:
        date = args.test_day
        year = int(date[:4])
        month = int(date[4:6])

        raw_dir = os.path.join(config.fusion_raw_dir, f"{year}", f"{month:02d}")
        missing = [v for v in variables if not os.path.exists(os.path.join(raw_dir, f"{v}_{date}_parsed.parquet"))]
        if missing:
            print(f"[SKIP] {date} missing_cache={missing}")
            return

        df = pipeline.process_day_from_cache(date, variables=variables, save_interim=True, region_type=region_type)
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

            for date in tqdm(dates, desc=f"{year}-{month:02d} [{region_type}]"):
                raw_dir = os.path.join(config.fusion_raw_dir, f"{year}", f"{month:02d}")
                missing = [
                    v
                    for v in variables
                    if not os.path.exists(os.path.join(raw_dir, f"{v}_{date}_parsed.parquet"))
                ]
                if missing:
                    skipped[date] = missing
                    continue

                df_day = pipeline.process_day_from_cache(
                    date, variables=variables, save_interim=True, region_type=region_type,
                )
                if df_day is not None and len(df_day) > 0:
                    monthly_dfs.append(df_day)

            if monthly_dfs:
                month_df = pd.concat(monthly_dfs, ignore_index=True)
                output_dir = os.path.join(config.fusion_output_dir, str(year))
                os.makedirs(output_dir, exist_ok=True)
                output_path = os.path.join(output_dir, f"fusion_{year}{month:02d}{suffix}.csv")
                month_df.to_csv(output_path, index=False, encoding="utf-8-sig")
                print(f"\n저장 완료: {output_path} (rows={len(month_df):,})")
                yearly_dfs.append(month_df)

        if yearly_dfs:
            year_df = pd.concat(yearly_dfs, ignore_index=True)
            output_path = os.path.join(config.fusion_output_dir, f"fusion_weather_{year}{suffix}.csv")
            year_df.to_csv(output_path, index=False, encoding="utf-8-sig")
            results_year_paths.append(output_path)
            print(f"\n연도별 저장 완료: {output_path} (rows={len(year_df):,})")

    print("\n" + "=" * 70)
    print(f"[{region_type.upper()}] 완료")
    print("=" * 70)
    print("end:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    if results_year_paths:
        print("생성된 파일:")
        for p in results_year_paths:
            print("  -", p)

    if skipped:
        print("\n[스킵 요약(B 정책: 캐시 누락은 스킵)]")
        items = sorted(skipped.items())
        print(f"skipped_days: {len(items)}")
        for date, miss in items[:30]:
            print(f"- {date}: missing={miss}")
        if len(items) > 30:
            print(f"... (and {len(items) - 30} more)")


def main(argv: List[str] | None = None):
    from fusion.config import FusionConfig
    from fusion.pipeline import FusionPipeline

    args = _build_arg_parser().parse_args(argv)

    # 루트 .env 파일 로드 (B단계는 다운로드하지 않으므로 auth_key는 선택사항)
    ROOT_DIR = os.path.dirname(BASE_DIR)
    dotenv.load_dotenv(os.path.join(ROOT_DIR, ".env"))
    auth_key = os.getenv("fusion_weather_authKey", "")

    variables = [v.strip() for v in args.variables.split(",") if v.strip()]
    if not variables:
        raise SystemExit("오류: variables가 비어있습니다")

    project_root = BASE_DIR
    config = FusionConfig(project_root=project_root, custom_data_root=args.output_path)
    pipeline = FusionPipeline(auth_key=auth_key, config=config)

    print("project_root:", project_root)
    if args.output_path:
        print("output_path:", args.output_path)
    print("region_type:", args.region_type)
    print()

    # region_type에 따라 실행
    if args.region_type == "both":
        for rt in ["hjd", "bjd"]:
            _run_single_region(pipeline, config, args, variables, rt)
            print()
    else:
        _run_single_region(pipeline, config, args, variables, args.region_type)


if __name__ == "__main__":
    # PyCharm/IDE 디버깅 편의를 위한 하드코딩 실행 옵션
    # - CLI로 실행할 때는 기본값(=USE_IDE_DEFAULTS=False)을 유지하세요.
    # - IDE에서 실행/디버깅할 때는 아래 값만 수정한 뒤 실행하면 됩니다.

    USE_IDE_DEFAULTS = True

    # IDE에서 Working Directory를 프로젝트 루트로 잡지 못할 때만 사용하세요.
    IDE_PROJECT_ROOT = None

    if USE_IDE_DEFAULTS:
        if IDE_PROJECT_ROOT:
            os.chdir(IDE_PROJECT_ROOT)

        ide_argv = [
            "--test-day",
            "20241128",
            "--variables",
            "ta,rn_60m,sd_3hr",
            "--region-type",
            "both",
        ]

        main(argv=ide_argv)
    else:
        main()
