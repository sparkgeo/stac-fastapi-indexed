from asyncio import Semaphore
from typing import Any, Dict, Final

from stac_index.io.readers import get_reader_for_uri

from stac_fastapi.indexed.settings import get_settings

# If we ever support multi-catalog indexes, or if we see catalogs
# whose STAC items come from different domains, it might be worth
# maintaining one semaphore per domain as a single semaphore may
# unnecessarily limit concurrency when hosts could handle more.
_semaphore: Final[Semaphore] = Semaphore(value=get_settings().max_concurrency)


async def fetch_dict(uri: str) -> Dict[str, Any]:
    async with _semaphore:
        return await get_reader_for_uri(uri=uri).load_json_from_uri(uri)
