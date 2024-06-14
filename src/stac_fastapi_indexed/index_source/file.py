from glob import glob
from os import path
from re import match
from typing import Dict, Optional

from stac_fastapi_indexed.index_source.index_source import IndexSource
from stac_index_common.data_stores.file import url_prefix


class FileIndexSource(IndexSource):
    @classmethod
    def create_index_source(cls, url: str) -> Optional[IndexSource]:
        if match(url_prefix, url):
            return cls(url)
        return None

    def __init__(self, base_url):
        super().__init__()
        self._base_url = base_url

    def get_parquet_urls(self) -> Dict[str, str]:
        return {
            path.basename(".".join(parquet_path.split(".")[:-1])): parquet_path
            for parquet_path in glob(path.join(self._base_url, "*.parquet"))
        }
