"""융합기상정보 후처리 전용 스크립트 (B 단계) - 행정동 기반

목표
----
- A 단계에서 생성된 raw 캐시(`data/fusion_raw/.../*_parsed.parquet`)만 사용해
  피벗/공간집계/변수 병합/출력을 수행합니다.
- 이 스크립트는 **다운로드를 절대 수행하지 않습니다.**
- **행정동 경계**(2022년 4분기 기준)를 사용하여 공간 집계를 수행합니다.

정책
----
- 캐시 누락 날짜/변수는 스킵(B 정책)하고, 스킵 목록을 요약 출력합니다.
- 행정동 매핑 테이블(`grid_to_hjd.parquet`)이 없으면 자동으로 생성합니다.

예시
----
python fusion_weather/run_process.py --start-year 2024 --end-year 2024 --start-month 1 --end-month 12 --variables ta,rn_60m,sd_3hr
python fusion_weather/run_process.py --test-day 20241128 --variables ta,rn_60m,sd_3hr
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
    p = argparse.ArgumentParser(description="융합기상정보 후처리 (B 단계) - 행정동 기반")
    p.add_argument("--start-year", type=int, default=2024)
    p.add_argument("--end-year", type=int, default=2024)
    p.add_argument("--start-month", type=int, default=1)
    p.add_argument("--end-month", type=int, default=12)
    p.add_argument("--variables", type=str, default="ta,rn_60m")
    p.add_argument("--test-day", type=str, default=None, help="테스트용 하루(YYYYMMDD)만 후처리")
    p.add_argument("--force-rebuild-mapping", action="store_true", help="행정동 매핑 테이블 강제 재생성")
    p.add_argument("--output-path", type=str, default=None, help="데이터 경로 (A단계의 --output-path와 동일하게 지정)")
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

    # 루트 .env 파일 로드 (B단계는 다운로드하지 않으므로 auth_key는 선택사항)
    ROOT_DIR = os.path.dirname(BASE_DIR)
    dotenv.load_dotenv(os.path.join(ROOT_DIR, ".env"))
    auth_key = os.getenv("fusion_weather_authKey", "")

    variables = [v.strip() for v in args.variables.split(",") if v.strip()]
    if not variables:
        raise SystemExit("오류: variables가 비어있습니다")

    project_root = BASE_DIR

    # 행정동 매핑 사용
    from fusion.config import FusionConfig
    from fusion.geocode import GridToHjdMapper
    from fusion.pipeline import FusionPipeline
    from fusion.aggregate import SpatialAggregator

    config = FusionConfig(project_root=project_root, custom_data_root=args.output_path)

    # 행정동 매핑 테이블 생성/로드
    print("=" * 70)
    print("[B] 행정동 기반 후처리")
    print("=" * 70)
    print("project_root:", project_root)
    if args.output_path:
        print("output_path:", args.output_path)
    print("행정동 매핑 파일:", config.grid_hjd_mapping_file)
    print()

    mapper = GridToHjdMapper(config)

    # 매핑 테이블이 없거나 강제 재생성 옵션이 있으면 생성
    if args.force_rebuild_mapping or not os.path.exists(config.grid_hjd_mapping_file):
        print("행정동 매핑 테이블 생성 중... (처음 한 번만 수행되며 시간이 걸릴 수 있습니다)")
        grid_mapping = mapper.build_hjd_mapping(force_rebuild=args.force_rebuild_mapping)
    else:
        print("기존 행정동 매핑 테이블 로드 중...")
        grid_mapping = mapper.load_hjd_mapping()

    print(f"행정동 매핑 완료: {len(grid_mapping):,} 격자점")
    print(f"행정동 개수: {grid_mapping['HJD_CD'].nunique():,}")
    print()

    # FusionPipeline 생성 (기존 파이프라인 재사용)
    pipeline = FusionPipeline(auth_key=auth_key, config=config)

    # 행정동 매핑을 파이프라인에 주입
    pipeline._grid_mapping = grid_mapping
    pipeline._spatial_agg = SpatialAggregator(grid_mapping, config)

    print("=" * 70)
    print("[B] 후처리(캐시 기반)")
    print("=" * 70)
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
    IDE_PROJECT_ROOT = None  # 예: "/Users/jaehoon/alphatross/git_clones/kma_api"

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
