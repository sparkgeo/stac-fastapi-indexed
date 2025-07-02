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
COPY pyproject.toml .
COPY uv.lock .

# File is required to successfully parse root package.toml, even though not installed.
COPY packages/stac-index/pyproject.toml ./packages/stac-index/

# Install dependencies.
RUN uv sync \
    --extra lambda \
    --locked \
    --no-dev \
    --no-install-workspace

# ##########################################################################

FROM public.ecr.aws/lambda/python:3.13
SHELL ["/bin/sh", "-exc"]

ENV PATH=/opt/app/bin:$PATH
ENV TZ=UTC

STOPSIGNAL SIGINT

COPY --from=build /opt/app /opt/app
COPY packages/stac-index/src/ /opt/app/lib/python3.13/site-packages/
COPY src/stac_fastapi/indexed /opt/app/lib/python3.13/site-packages/stac_fastapi/indexed

ENV PYTHONPATH=/opt/app/lib/python3.13/site-packages
RUN python -c "import duckdb; duckdb.execute('INSTALL spatial'); duckdb.execute('INSTALL httpfs'); duckdb.execute('INSTALL aws')"
ENV stac_api_indexed_install_duckdb_extensions=false

ARG DUCKDB_VERSION=1.2.2
# Store installed extensions at an alternate location so that the Lambda's least-privileged user can access them.
WORKDIR /datastore/.duckdb/extensions/v${DUCKDB_VERSION}/linux_amd64_gcc4
RUN  mv /root/.duckdb/extensions/v${DUCKDB_VERSION}/linux_amd64_gcc4/* . \
  && chmod -R 755 /datastore
# Tell DuckDB to look here for its installed extensions.
ENV HOME=/datastore

CMD ["stac_fastapi.indexed.app.handler"]
