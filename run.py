
if __name__ == "__main__" :
    import os
    import dotenv
    # 환경 변수에서 인증키 로드
    dotenv.load_dotenv()
    auth_key = os.getenv("authKey")

    if not auth_key:
        raise ValueError("authKey를 .env 파일에 설정해주세요")

    # 데이터 루트 디렉토리 (data 하위에 raw_data / post_process_data 자동 생성)
    BASE_DATA_DIR = "/Users/jaehoon/liminal_ego/git_clones/kma_api/data"

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
