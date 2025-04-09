FROM python:3.13-slim AS build

SHELL ["/bin/sh", "-exc"]

### Start build prep.

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=python3.13 \
    UV_PROJECT_ENVIRONMENT=/opt/app

### End build prep -- this is where your app Dockerfile should start.

WORKDIR /src
COPY ./ /src


RUN uv sync \
    --directory=/src \
    --locked \
    --no-dev \
    --no-install-project

RUN uv sync \
    --extra server \
    --locked \
    --no-dev \
    --no-editable


# ##########################################################################

FROM python:3.13-slim
SHELL ["/bin/sh", "-exc"]

ENV PATH=/opt/app/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgeos3.11.1 \
    && rm -rf /var/lib/apt/lists/*

STOPSIGNAL SIGINT

COPY --from=build /opt/app /opt/app
COPY packages/stac-index/src/ /opt/app/lib/python3.13/site-packages/
COPY src/stac_fastapi/indexed /opt/app/lib/python3.13/site-packages/stac_fastapi/indexed

RUN python -c "import duckdb; duckdb.execute('install spatial'); duckdb.execute('install httpfs')"
ENV stac_api_indexed_install_duckdb_extensions=false

WORKDIR /opt/app

CMD /opt/app/bin/uvicorn stac_fastapi.indexed.app:app --host=0.0.0.0 --port=80
