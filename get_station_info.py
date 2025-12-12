import requests
import os
import dotenv
from datetime import datetime


def download_station_info(inf_type="SFC", auth_key=None, save_dir=None):
    """
    기상청 지점 정보를 원본 그대로 다운로드

    Parameters:
    - inf_type: 지점 종류 (SFC, AWS, BUOY, RAWS)
    - auth_key: API 인증키
    - save_dir: 저장 디렉토리

    Returns:
    - str: 저장된 파일 경로
    """
    BASE_URL = "https://apihub.kma.go.kr/api/typ01/url/stn_inf.php"

    params = {
        "inf": inf_type,  # 지점 종류
        "stn": "",  # 빈 문자열: 전체 지점
        "help":"1",
        "authKey": auth_key
    }

    print(f"지점 정보 다운로드 중... (종류: {inf_type})")
    response = requests.get(BASE_URL, params=params, timeout=30)

    if response.status_code != 200:
        raise Exception(f"API 요청 실패: {response.status_code}")

    # 저장 디렉토리 설정
    if save_dir is None:
        save_dir = os.getcwd()

    os.makedirs(save_dir, exist_ok=True)

    # 파일명 생성
    timestamp = datetime.now().strftime("%Y%m%d")
    filename = f"station_info_{inf_type}_{timestamp}.txt"
    save_path = os.path.join(save_dir, filename)

    # 원본 그대로 저장
    with open(save_path, 'w', encoding='utf-8') as f:
        f.write(response.text)

    print(f"✓ 파일 저장 완료: {save_path}")
    print(f"  파일 크기: {os.path.getsize(save_path):,} bytes")

    # 처음 몇 줄만 미리보기
    lines = response.text.split('\n')
    print(f"  총 라인 수: {len(lines)}")
    print(f"\n  처음 5줄 미리보기:")
    for i, line in enumerate(lines[:5]):
        print(f"    {i + 1}: {line[:100]}...")

    return save_path


def download_all_station_info(save_dir=None, auth_key=None):
    """
    모든 종류의 지점 정보를 다운로드
    """
    if save_dir is None:
        save_dir = os.getcwd()

    station_types = {
        "SFC": "지상관측소(ASOS)",
    }

    saved_files = []

    for inf_type, description in station_types.items():
        try:
            print(f"\n{'=' * 80}")
            print(f"{description} [{inf_type}]")
            print('=' * 80)

            file_path = download_station_info(
                inf_type=inf_type,
                auth_key=auth_key,
                save_dir=save_dir
            )

            saved_files.append(file_path)

        except Exception as e:
            print(f"✗ 오류 발생: {e}")

    return saved_files


if __name__ == "__main__":
    # 환경 변수에서 인증키 로드
    dotenv.load_dotenv()
    auth_key = os.getenv("authKey")

    if not auth_key:
        raise ValueError("authKey를 .env 파일에 설정해주세요")

    # 저장 디렉토리
    SAVE_DIR = "/Users/jaehoon/liminal_ego/git_clones/kma_api/data"

    print("=" * 80)
    print("기상청 지점 정보 다운로드")
    print("=" * 80)
    print(f"저장 경로: {SAVE_DIR}\n")

    try:
        # 모든 지점 정보 다운로드
        saved_files = download_all_station_info(
            save_dir=SAVE_DIR,
            auth_key=auth_key
        )

        print("\n" + "=" * 80)
        print("다운로드 완료!")
        print("=" * 80)
        print(f"총 {len(saved_files)}개 파일 저장:")
        for file in saved_files:
            print(f"  ✓ {os.path.basename(file)}")

    except Exception as e:
        print(f"\n✗ 오류 발생: {e}")
        import traceback

        traceback.print_exc()
