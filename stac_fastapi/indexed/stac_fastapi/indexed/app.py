from logging import Logger, getLogger
from os import getenv
from typing import Final

from asgi_correlation_id import CorrelationIdMiddleware
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
from stac_fastapi.extensions.core import (
    FilterExtension,
    SortExtension,
    TokenPaginationExtension,
)

from stac_fastapi.indexed.core import CoreCrudClient
from stac_fastapi.indexed.db import connect_to_db, disconnect_from_db
from stac_fastapi.indexed.errors import get_all_errors
from stac_fastapi.indexed.middleware.request_log_middleware import RequestLogMiddleware
from stac_fastapi.indexed.search.filter.filter_client import FiltersClient
from stac_fastapi.indexed.search.search_get_request import SearchGetRequest
from stac_fastapi.indexed.settings import get_settings
from stac_fastapi.indexed.sortables.routes import add_routes as add_sortables_routes
from stac_index.common.indexing_error import IndexingError

_logger: Final[Logger] = getLogger(__file__)

extensions_map = {
    "sort": SortExtension(),
    "pagination": TokenPaginationExtension(),
    "filter": FilterExtension(client=FiltersClient()),
}

extensions = list(extensions_map.values())
post_request_model = create_post_request_model(extensions)


def fastapi_factory() -> FastAPI:
    fapi_args = {
        "docs_url": "/api.html",
    }
    api_stage = get_settings().deployment_stage
    if api_stage is not None:
        fapi_args["root_path"] = f"/{api_stage}"
    _logger.info(f"configuring FastAPI with {fapi_args}")
    return FastAPI(**fapi_args)


api = StacApi(
    app=fastapi_factory(),
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
    middlewares=[
        Middleware(CORSMiddleware),
        Middleware(ProxyHeaderMiddleware),
        Middleware(RequestLogMiddleware),
        Middleware(CorrelationIdMiddleware),
    ],
)
app = api.app
add_sortables_routes(app)


@app.on_event(
    "startup"
)  # deprecated event handlers because of stac-fastapi, not yet able to use lifespan approach
async def startup_event():
    await connect_to_db()


@app.on_event("shutdown")
async def shutdown_event():
    await disconnect_from_db()


@app.get("/status/errors")
async def get_status_errors() -> list[IndexingError]:
    return get_all_errors()


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
