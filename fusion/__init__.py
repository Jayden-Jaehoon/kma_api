"""
융합기상정보 처리 모듈

고해상도 격자자료(500m)를 다운로드하고 법정동 단위로 집계하는 파이프라인
"""

from .config import FusionConfig
from .geocode import GridToLawIdMapper
from .download import FusionDataDownloader
from .aggregate import TimeAggregator, SpatialAggregator
from .pipeline import FusionPipeline

__all__ = [
    'FusionConfig',
    'GridToLawIdMapper',
    'FusionDataDownloader',
    'TimeAggregator',
    'SpatialAggregator',
    'FusionPipeline',
]
