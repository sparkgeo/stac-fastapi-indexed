import os
from os import getenv

from fastapi import FastAPI
from fastapi.middleware import Middleware
from fastapi.responses import ORJSONResponse
from stac_fastapi.api.app import StacApi
from stac_fastapi.api.middleware import CORSMiddleware, ProxyHeaderMiddleware
from stac_fastapi.api.models import (
    ItemCollectionUri,
    create_get_request_model,
    create_post_request_model,
    create_request_model,
)
from stac_fastapi.extensions.core import FilterExtension, TokenPaginationExtension

from stac_fastapi.indexed.core import CoreCrudClient
from stac_fastapi.indexed.db import connect_to_db, disconnect_from_db
from stac_fastapi.indexed.search.filter.filter_client import FiltersClient
from stac_fastapi.indexed.search.search_get_request import SearchGetRequest
from stac_fastapi.indexed.settings import get_settings

extensions_map = {
    "pagination": TokenPaginationExtension(),
    "filter": FilterExtension(client=FiltersClient()),
}

extensions = list(extensions_map.values())
post_request_model = create_post_request_model(extensions)

stage = os.environ.get("API_STAGE", None)
if stage:
    fast_api_app = FastAPI(root_path=f"/{stage}", docs_url="/api.html")
else:
    fast_api_app = FastAPI(docs_url="/api.html")
api = StacApi(
    app=fast_api_app,
    settings=get_settings(),
    extensions=extensions,
    client=CoreCrudClient(post_request_model=post_request_model),  # type: ignore
    response_class=ORJSONResponse,
    items_get_request_model=create_request_model(
        "ItemCollectionURI",
        base_model=ItemCollectionUri,
        mixins=[TokenPaginationExtension().GET],
    ),
    search_get_request_model=create_get_request_model(
        extensions, base_model=SearchGetRequest
    ),
    search_post_request_model=post_request_model,
    middlewares=[Middleware(CORSMiddleware), Middleware(ProxyHeaderMiddleware)],
)
app = api.app


@app.on_event(
    "startup"
)  # deprecated event handlers because of stac-fastapi, not yet able to use lifespan approach
async def startup_event():
    await connect_to_db()


@app.on_event("shutdown")
async def shutdown_event():
    await disconnect_from_db()


def run():
    """Run app from command line using uvicorn if available."""
    try:
        import uvicorn

        settings = get_settings()
        uvicorn.run(
            "stac_fastapi.indexed.app:app",
            host=settings.app_host,
            port=settings.app_port,
            log_level="debug" if settings.log_level.upper() == "DEBUG" else "info",
            reload=settings.reload,
            root_path=getenv("UVICORN_ROOT_PATH", ""),
        )
    except ImportError as e:
        raise RuntimeError("Uvicorn must be installed in order to use command") from e


if __name__ == "__main__":
    run()


def create_handler(app):
    """Create a handler to use with AWS Lambda if mangum available."""
    try:
        from mangum import Mangum

        return Mangum(
            app,
            text_mime_types=[
                "text/",
                "application/json",
                "application/geo+json",
                "application/vnd.oai.openapi",
            ],
        )
    except ImportError:
        return None


handler = create_handler(app)
