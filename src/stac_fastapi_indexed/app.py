from os import getenv

from fastapi.middleware import Middleware
from fastapi.responses import ORJSONResponse
from stac_fastapi.api.app import StacApi
from stac_fastapi.api.middleware import CORSMiddleware, ProxyHeaderMiddleware
from stac_fastapi.api.models import create_get_request_model, create_post_request_model
from stac_fastapi.extensions.core import (  # FilterExtension,; FieldsExtension,
    SortExtension,
    TokenPaginationExtension,
)

from stac_fastapi_indexed.core import CoreCrudClient
from stac_fastapi_indexed.db import connect_to_db, disconnect_from_db
from stac_fastapi_indexed.settings import get_settings

# from stac_fastapi.pgstac.config import Settings
# from stac_fastapi.pgstac.core import CoreCrudClient
# from stac_fastapi.pgstac.db import close_db_connection, connect_to_db
# from stac_fastapi.pgstac.extensions import QueryExtension
# from stac_fastapi.pgstac.extensions.filter import FiltersClient
# from stac_fastapi.pgstac.transactions import BulkTransactionsClient, TransactionsClient
# from stac_fastapi.pgstac.types.search import PgstacSearch

extensions_map = {
    # "query": QueryExtension(),
    "sort": SortExtension(),
    # "fields": FieldsExtension(),
    "pagination": TokenPaginationExtension(),
    # "filter": FilterExtension(client=FiltersClient()),
}

extensions = list(extensions_map.values())
post_request_model = create_post_request_model(extensions)

api = StacApi(
    settings=get_settings(),
    extensions=extensions,
    client=CoreCrudClient(post_request_model=post_request_model),  # type: ignore
    response_class=ORJSONResponse,
    search_get_request_model=create_get_request_model(extensions),
    search_post_request_model=post_request_model,
    middlewares=[Middleware(CORSMiddleware), Middleware(ProxyHeaderMiddleware)],
)
app = api.app


@app.on_event(
    "startup"
)  # deprecated event handlers because of stac-fastapi, not yet able to use lifespan approach
async def startup_event():
    connect_to_db(app)


@app.on_event("shutdown")
async def shutdown_event():
    disconnect_from_db(app)


def run():
    """Run app from command line using uvicorn if available."""
    try:
        import uvicorn

        settings = get_settings()
        uvicorn.run(
            "stac_fastapi_indexed.app:app",
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

        return Mangum(app)
    except ImportError:
        return None


handler = create_handler(app)
