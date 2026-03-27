
if __name__ == "__main__" :
    import os
    from pathlib import Path
    import dotenv

    # 실행 파일이 위치한 폴더를 기준으로 설정
    BASE_DIR = Path(__file__).resolve().parent
    
    # 해당 폴더 내의 .env 파일 로드
    dotenv.load_dotenv(BASE_DIR / ".env")
    auth_key = os.getenv("authKey")

    if not auth_key:
        raise ValueError("authKey를 asos/.env 파일에 설정해주세요")

    # 데이터 루트 디렉토리 (asos/data 하위에 raw_data / post_process_data 자동 생성)
    BASE_DATA_DIR = str(BASE_DIR / "data")

    # 연단위 다운로드 + 전처리 실행 예시
    try:
        from process_data import run_year_range

        results = run_year_range(
            auth_key=auth_key,
            base_data_dir=BASE_DATA_DIR,
            start_year=1970,
            end_year=2024,
            stn="0",
        )

        print("\n" + "=" * 80)
        print("✓ 연단위 다운로드 및 전처리 완료!")
        print("결과 요약:")
        for r in results:
            print(f"  - RAW : {r['raw']}")
            print(f"    CSV : {r['proc']}")
        print("=" * 80)

    except Exception as e:
        print(f"\n✗ 오류 발생: {e}")
        import traceback

        traceback.print_exc()
