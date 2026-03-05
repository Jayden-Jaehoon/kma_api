"""
융합기상정보 설정 v2 (법정동 기반)

기존 FusionConfig를 상속하여 법정동(읍면동) 관련 경로만 추가/오버라이드합니다.
"""

import os
from dataclasses import dataclass

from .config import FusionConfig


@dataclass
class FusionConfigV2(FusionConfig):
    """융합기상정보 처리 설정 v2 (법정동 기반)

    기존 FusionConfig를 상속하여 법정동(읍면동) 데이터 경로를 추가합니다.
    """

    # 법정동(읍면동) 데이터 경로
    @property
    def geodata_umd_dir(self) -> str:
        """법정동(읍면동) shapefile 디렉토리 (시도별로 분리)"""
        return os.path.join(self.data_dir, "geodata_umd")

    @property
    def grid_mapping_file_umd(self) -> str:
        """격자-법정동(읍면동) 매핑 파일 경로"""
        return os.path.join(self.geodata_dir, "grid_to_emd_umd.parquet")


# 기본 v2 설정 인스턴스
DEFAULT_CONFIG_V2 = FusionConfigV2()
