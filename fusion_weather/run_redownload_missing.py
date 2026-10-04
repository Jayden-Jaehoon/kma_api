"""누락/불완전한 융합기상 raw 파일 재다운로드 스크립트.

작업 대상은 점검 결과 누락/손상/격자 부족으로 확인된 124개 파일입니다.
정상적으로 다시 받은 파일은 다음 실행에서 건너뛰고, 손상되었거나 일부 시간대가
부족한 파일은 임시 백업 후 해당 날짜/변수의 하루 전체를 다시 다운로드합니다.

실행 예:
    python fusion_weather/run_redownload_missing.py
    python fusion_weather/run_redownload_missing.py --dry-run
    python fusion_weather/run_redownload_missing.py --max-workers 2 --output-path E:\\kma
"""

from __future__ import annotations

import argparse
import os
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple

import dotenv


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# (날짜, 변수). 기존 파일이 있어도 목록에 포함된 파일은 무결성을 확인하고,
# 정상 파일만 건너뜁니다. 2017년 12월 10일 00시 문제도 파일 전체 재다운로드로 처리합니다.
TARGETS: Tuple[Tuple[str, str], ...] = (
    ("20211221", "ta"),
    ("20211222", "ta"),
    ("20211201", "rn_60m"),
    ("20211205", "rn_60m"),
    ("20211206", "rn_60m"),
    ("20211207", "rn_60m"),
    ("20211208", "rn_60m"),
    ("20211209", "rn_60m"),
    ("20211210", "rn_60m"),
    ("20211213", "rn_60m"),
    ("20211214", "rn_60m"),
    ("20211215", "rn_60m"),
    ("20211216", "rn_60m"),
    ("20211217", "rn_60m"),
    ("20211218", "rn_60m"),
    ("20211219", "rn_60m"),
    ("20211220", "rn_60m"),
    ("20231019", "rn_60m"),
    ("20231020", "ta"),
    ("20231020", "rn_60m"),
    ("20231021", "ta"),
    ("20231022", "ta"),
    ("20240531", "rn_60m"),
    ("20240502", "rn_60m"),
    ("20240718", "rn_60m"),
    ("20230321", "ta"),
    ("20230513", "ta"),
    ("20230518", "ta"),
    ("20230926", "rn_60m"),
    ("20230927", "ta"),
    ("20230927", "rn_60m"),
    ("20231002", "rn_60m"),
    ("20240607", "rn_60m"),
    ("20240613", "ta"),
    ("20240618", "rn_60m"),
    ("20240619", "rn_60m"),
    ("20240621", "rn_60m"),
    ("20240622", "ta"),
    ("20240623", "rn_60m"),
    ("20240705", "rn_60m"),
    ("20240716", "ta"),
    ("20240816", "ta"),
    ("20240817", "ta"),
    ("20240818", "rn_60m"),
    ("20240914", "ta"),
    ("20240915", "ta"),
    ("20240923", "rn_60m"),
    ("20240924", "rn_60m"),
    ("20240925", "ta"),
    ("20240925", "rn_60m"),
    ("20240926", "ta"),
    ("20240927", "ta"),
    ("20240930", "ta"),
    ("20241016", "rn_60m"),
    ("20241017", "ta"),
    ("20241022", "rn_60m"),
    ("20241024", "rn_60m"),
    ("20241025", "ta"),
    ("20241026", "ta"),
)

EXPECTED_GRID_N = 4198401
EXPECTED_HOURS = {"ta": 24, "rn_60m": 24}


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="융합기상 누락/불완전 파일 재다운로드")
    parser.add_argument("--max-workers", type=int, default=1, help="동시 날짜/파일 수 (기본 1)")
    parser.add_argument("--output-path", default=None, help="데이터 저장 경로 (기본: .env 또는 project_root/data)")
    parser.add_argument("--api-type", choices=("org", "public"), default="org")
    parser.add_argument("--dry-run", action="store_true", help="대상과 경로만 확인하고 다운로드하지 않음")
    return parser


@dataclass
class RedownloadResult:
    date: str
    variable: str
    status: str
    detail: str = ""


def _is_complete_cache(cache_path: str, variable: str) -> bool:
    """이미 정상적으로 받은 parquet인지 빠르게 판정합니다.

    Parquet 메타데이터만 읽어 파일 전체를 메모리에 올리지 않습니다.
    격자 수는 현재 geodata의 NetCDF 기준(4,198,401개)을 사용합니다.
    """
    if not os.path.isfile(cache_path):
        return False
    try:
        import pyarrow.parquet as pq

        metadata = pq.ParquetFile(cache_path).metadata
        expected_rows = EXPECTED_GRID_N * EXPECTED_HOURS[variable]
        return (
            metadata is not None
            and metadata.num_rows == expected_rows
            and {"grid_idx", "date", "hour", "value"}.issubset(
                set(pq.ParquetFile(cache_path).schema.names)
            )
        )
    except Exception:
        return False


def _redownload_one(
    *,
    project_root: str,
    auth_key: str,
    date: str,
    variable: str,
    output_path: str | None,
    api_type: str,
    backup_root: str,
) -> RedownloadResult:
    from fusion.config import FusionConfig
    from fusion.pipeline import FusionPipeline

    config = FusionConfig(project_root=project_root, custom_data_root=output_path, api_type=api_type)
    raw_dir = os.path.join(config.fusion_raw_dir, date[:4], date[4:6])
    cache_path = os.path.join(raw_dir, f"{variable}_{date}_parsed.parquet")
    os.makedirs(raw_dir, exist_ok=True)

    if _is_complete_cache(cache_path, variable):
        return RedownloadResult(date, variable, "SKIP", "이미 정상 파일이 존재합니다")

    backup_path = None
    if os.path.exists(cache_path):
        os.makedirs(backup_root, exist_ok=True)
        backup_path = os.path.join(backup_root, os.path.basename(cache_path))
        # 같은 이름의 백업이 남아 있어도 덮어쓰지 않도록 고유 경로를 사용합니다.
        backup_path = f"{backup_path}.{os.getpid()}"
        shutil.move(cache_path, backup_path)

    try:
        pipeline = FusionPipeline(auth_key=auth_key, config=config)
        summary = pipeline.ensure_day_cache(date=date, variables=[variable])
        failed = summary.get("failed", {})
        if failed:
            raise RuntimeError(failed.get(variable, "알 수 없는 다운로드 실패"))

        if not os.path.exists(cache_path):
            raise RuntimeError("다운로드 성공으로 보고되었지만 parquet 파일이 생성되지 않았습니다")

        if backup_path and os.path.exists(backup_path):
            os.remove(backup_path)
        return RedownloadResult(date, variable, "OK", cache_path)
    except Exception as exc:
        # 새 파일이 일부만 만들어진 경우 제거하고, 원본을 복원합니다.
        if os.path.exists(cache_path):
            os.remove(cache_path)
        if backup_path and os.path.exists(backup_path):
            shutil.move(backup_path, cache_path)
        return RedownloadResult(date, variable, "FAIL", f"{type(exc).__name__}: {exc}")


def main(argv: List[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    root_dir = os.path.dirname(BASE_DIR)
    dotenv.load_dotenv(os.path.join(root_dir, ".env"))
    auth_key = os.getenv("fusion_weather_authKey")
    if not auth_key and not args.dry_run:
        raise SystemExit("오류: 루트 .env에 fusion_weather_authKey를 설정해주세요")

    # dry-run은 대용량 지리공간 의존성 없이 대상과 경로만 점검할 수 있어야 합니다.
    if args.output_path:
        data_root = args.output_path
    else:
        data_root = os.getenv("FUSION_DATA_ROOT") or os.path.join(BASE_DIR, "data")
    api_base_url = {
        "org": "https://apihub-org.kma.go.kr/api/typ01",
        "public": "https://apihub.kma.go.kr/api/typ01",
    }[args.api_type]
    raw_dir = os.path.join(data_root, "fusion_raw")
    backup_root = os.path.join(raw_dir, "_redownload_backups", datetime.now().strftime("%Y%m%d_%H%M%S"))

    print(f"대상 파일: {len(TARGETS)}개")
    print(f"API: {args.api_type} ({api_base_url})")
    print(f"저장 경로: {raw_dir}")
    for date, variable in TARGETS:
        print(f"- {variable}_{date}_parsed.parquet")

    if args.dry_run:
        return 0

    from fusion.config import FusionConfig

    config = FusionConfig(project_root=BASE_DIR, custom_data_root=args.output_path, api_type=args.api_type)

    results: List[RedownloadResult] = []
    workers = max(1, int(args.max_workers))
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                _redownload_one,
                project_root=BASE_DIR,
                auth_key=auth_key,
                date=date,
                variable=variable,
                output_path=args.output_path,
                api_type=args.api_type,
                backup_root=backup_root,
            )
            for date, variable in TARGETS
        ]
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            print(f"[{result.status}] {result.variable}_{result.date}: {result.detail}")

    failed = [result for result in results if result.status == "FAIL"]
    skipped = [result for result in results if result.status == "SKIP"]
    succeeded = [result for result in results if result.status == "OK"]
    print(f"완료: 새로 다운로드 {len(succeeded)}개, 건너뜀 {len(skipped)}개, 실패 {len(failed)}개")
    if failed:
        print("실패한 대상은 기존 파일을 복원했습니다.")
        return 1
    if os.path.isdir(backup_root) and not os.listdir(backup_root):
        os.rmdir(backup_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
