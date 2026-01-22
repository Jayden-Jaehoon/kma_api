"""융합기상정보 raw 다운로드/캐시 생성 전용 스크립트 (A 단계)

목표
----
- 대용량(전체 격자) API를 호출해 `data/fusion_raw/YYYY/MM/{var}_{date}_parsed.parquet`를 먼저 채웁니다.
- 이 단계에서는 후처리(피벗/공간집계/CSV 출력)를 하지 않습니다.

특징
----
- 날짜 단위 병렬 처리(기본 4 workers)
- 실패/재시도/검증 로그는 기존 로직대로 `data/fusion_raw/_validation_logs/...`에 남습니다.

예시
----
python run_download_fusion.py --start-year 2024 --end-year 2024 --start-month 1 --end-month 1 --variables ta,rn_60m
python run_download_fusion.py --test-day 20241128 --variables ta,rn_60m,sd_3hr
"""

from __future__ import annotations

import argparse
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple

import dotenv


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="융합기상정보 raw 다운로드/캐시 생성(A 단계)")
    p.add_argument("--start-year", type=int, default=2024)
    p.add_argument("--end-year", type=int, default=2024)
    p.add_argument("--start-month", type=int, default=1)
    p.add_argument("--end-month", type=int, default=12)
    p.add_argument("--variables", type=str, default="ta,rn_60m")
    p.add_argument("--test-day", type=str, default=None, help="테스트용 하루(YYYYMMDD)만 다운로드")
    p.add_argument("--max-workers", type=int, default=4, help="날짜 단위 병렬 worker 수 (기본 4)")
    return p


def _iter_dates(
    *,
    start_year: int,
    end_year: int,
    start_month: int,
    end_month: int,
) -> List[str]:
    """연/월 범위에 해당하는 YYYYMMDD 날짜 리스트를 생성."""
    out: List[str] = []

    for year in range(start_year, end_year + 1):
        m0 = start_month if year == start_year else 1
        m1 = end_month if year == end_year else 12

        for month in range(m0, m1 + 1):
            first = datetime(year, month, 1)
            if month == 12:
                next_month = datetime(year + 1, 1, 1)
            else:
                next_month = datetime(year, month + 1, 1)
            num_days = (next_month - first).days

            for d in range(1, num_days + 1):
                out.append(f"{year}{month:02d}{d:02d}")

    return out


@dataclass
class _DayResult:
    date: str
    ok_vars: List[str]
    failed_vars: List[Tuple[str, str]]  # (var, error)


def _download_one_day_worker(
    *,
    project_root: str,
    auth_key: str,
    date: str,
    variables: List[str],
) -> _DayResult:
    # 워커 프로세스에서 import/초기화
    from fusion.config import FusionConfig
    from fusion.pipeline import FusionPipeline

    config = FusionConfig(project_root=project_root)
    pipeline = FusionPipeline(auth_key=auth_key, config=config)

    summary = pipeline.ensure_day_cache(date=date, variables=variables)
    ok = sorted(summary.get("ok", {}).keys())
    failed = sorted(summary.get("failed", {}).items())
    return _DayResult(date=date, ok_vars=ok, failed_vars=failed)


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

    if args.test_day:
        dates = [args.test_day]
    else:
        dates = _iter_dates(
            start_year=args.start_year,
            end_year=args.end_year,
            start_month=args.start_month,
            end_month=args.end_month,
        )

    print("=" * 70)
    print("[A] raw 다운로드/캐시 생성")
    print("=" * 70)
    print("project_root:", project_root)
    print("dates:", len(dates), "(first/last:", dates[0], "~", dates[-1], ")")
    print("variables:", variables)
    print("max_workers:", args.max_workers)
    print("start:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print()

    ok_days = 0
    failed_days = 0
    failed_details: List[_DayResult] = []

    max_workers = max(1, int(args.max_workers))
    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futures = [
            ex.submit(
                _download_one_day_worker,
                project_root=project_root,
                auth_key=auth_key,
                date=date,
                variables=variables,
            )
            for date in dates
        ]

        for fut in as_completed(futures):
            res = fut.result()
            if res.failed_vars:
                failed_days += 1
                failed_details.append(res)
                print(f"[FAIL] {res.date} ok={res.ok_vars} failed={len(res.failed_vars)}")
            else:
                ok_days += 1
                print(f"[ OK ] {res.date} ok={res.ok_vars}")

    print("\n" + "=" * 70)
    print("완료")
    print("=" * 70)
    print("ok_days:", ok_days)
    print("failed_days:", failed_days)
    print("end:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    if failed_details:
        print("\n[실패 요약]")
        for r in sorted(failed_details, key=lambda x: x.date):
            msg = ", ".join([f"{v}({e})" for v, e in r.failed_vars])
            print(f"- {r.date}: {msg}")


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

        # 예시 1) 하루만 다운로드
        ide_argv = [
            "--test-day",
            "20260116",
            "--variables",
            "ta,rn_60m,sd_3hr",
            "--max-workers",
            "1",
        ]

        # 예시 2) 연/월 범위 다운로드 (필요하면 위 ide_argv와 교체)
        # ide_argv = [
        #     "--start-year", "2000",
        #     "--end-year", "2025",
        #     "--start-month", "1",
        #     "--end-month", "12",
        #     "--variables", "ta,rn_60m,sd_3hr",
        #     "--max-workers", "4",
        # ]

        main(argv=ide_argv)
    else:
        main()
