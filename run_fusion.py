"""
융합기상정보 처리 실행 스크립트

사용법:
    python run_fusion.py

또는 특정 옵션:
    python run_fusion.py --start-year 2020 --end-year 2024 --variables ta,rn_60m,sd_3hr
"""

import os
import sys
import argparse
from datetime import datetime

import dotenv


def main():
    parser = argparse.ArgumentParser(description='융합기상정보 처리 파이프라인')
    
    parser.add_argument('--start-year', type=int, default=2024,
                        help='시작 연도 (기본: 2024)')
    parser.add_argument('--end-year', type=int, default=2024,
                        help='종료 연도 (기본: 2024)')
    parser.add_argument('--start-month', type=int, default=1,
                        help='시작 월 (기본: 1)')
    parser.add_argument('--end-month', type=int, default=12,
                        help='종료 월 (기본: 12)')
    parser.add_argument('--variables', type=str, default='ta,rn_60m',
                        help='변수 목록, 콤마 구분 (기본: ta,rn_60m)')
    parser.add_argument('--build-mapping', action='store_true',
                        help='격자-법정동 매핑만 생성')
    parser.add_argument('--rebuild-mapping', action='store_true',
                        help='격자-법정동 매핑 재생성')
    parser.add_argument('--test-day', type=str, default=None,
                        help='테스트용 하루 처리 (YYYYMMDD)')
    
    args = parser.parse_args()
    
    # 환경 변수 로드
    dotenv.load_dotenv()
    auth_key = os.getenv("authKey")
    
    if not auth_key:
        print("오류: authKey를 .env 파일에 설정해주세요")
        print("예시: authKey=발급받은_인증키")
        sys.exit(1)
    
    # 변수 파싱
    variables = [v.strip() for v in args.variables.split(',')]
    
    # 모듈 임포트 (여기서 하는 이유: 에러 메시지를 먼저 보여주기 위해)
    try:
        from fusion import FusionPipeline, GridToLawIdMapper
        from fusion.config import FusionConfig
    except ImportError as e:
        print(f"모듈 임포트 오류: {e}")
        print("필요한 패키지를 설치해주세요:")
        print("  pip install geopandas shapely pyproj xarray netCDF4 pyarrow tqdm")
        sys.exit(1)
    
    config = FusionConfig()
    
    print("="*60)
    print("융합기상정보 처리 파이프라인")
    print("="*60)
    print(f"시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"프로젝트 경로: {config.project_root}")
    print()
    
    # 매핑만 생성
    if args.build_mapping or args.rebuild_mapping:
        print("격자-법정동 매핑 테이블 생성 중...")
        mapper = GridToLawIdMapper(config)
        mapping = mapper.build_mapping(force_rebuild=args.rebuild_mapping)
        
        print(f"\n매핑 완료!")
        print(f"  총 격자점: {len(mapping):,}")
        print(f"  매핑된 법정동: {mapping['LAW_ID'].nunique():,}")
        print(f"  저장 위치: {config.grid_mapping_file}")
        
        if args.build_mapping:
            return
    
    # 파이프라인 생성
    pipeline = FusionPipeline(auth_key, config)
    
    # 매핑 확보
    pipeline.ensure_mapping()
    
    # 테스트 모드
    if args.test_day:
        print(f"\n테스트 모드: {args.test_day} 처리")
        print(f"변수: {variables}")
        
        result = pipeline.process_day(args.test_day, variables)
        
        print(f"\n결과:")
        print(f"  레코드 수: {len(result):,}")
        print(f"  컬럼: {list(result.columns)}")
        print(f"\n샘플 데이터:")
        print(result.head(10))
        return
    
    # 전체 처리
    print(f"\n처리 범위: {args.start_year}년 ~ {args.end_year}년")
    print(f"변수: {variables}")
    print()
    
    results = []
    
    for year in range(args.start_year, args.end_year + 1):
        try:
            # 연도별 변수 조정 (적설은 2020년부터)
            year_vars = variables.copy()
            if 'sd_3hr' in year_vars and year < config.snow_start_year:
                year_vars.remove('sd_3hr')
                print(f"\n참고: {year}년은 적설 데이터 미제공 (2020년부터)")
            
            output_path = pipeline.process_year(
                year,
                variables=year_vars,
                start_month=args.start_month if year == args.start_year else 1,
                end_month=args.end_month if year == args.end_year else 12,
            )
            
            if output_path:
                results.append(output_path)
                
        except KeyboardInterrupt:
            print("\n\n사용자에 의해 중단되었습니다.")
            break
        except Exception as e:
            print(f"\n{year}년 처리 중 오류: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # 완료 메시지
    print("\n" + "="*60)
    print("처리 완료!")
    print("="*60)
    print(f"종료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"생성된 파일:")
    for path in results:
        print(f"  - {path}")


if __name__ == "__main__":
    main()
