from stac_fastapi_indexed.fetchers.fetcher import Fetcher


class FileFetcher(Fetcher):
    @staticmethod
    def compatibility_regex() -> str:
        return r"^(?!.*://).*"

    async def fetch(self, url: str) -> str:
        with open(url, "r") as f:
            return f.read()
