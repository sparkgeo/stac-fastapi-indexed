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
    --extra lambda \
    --locked \
    --no-dev \
    --no-editable


# ##########################################################################

FROM public.ecr.aws/lambda/python:3.13
SHELL ["/bin/sh", "-exc"]

ENV PATH=/opt/app/bin:$PATH

STOPSIGNAL SIGINT

COPY --from=build /opt/app /opt/app
COPY packages/stac-index/src/ /opt/app/lib/python3.13/site-packages/
COPY src/stac_fastapi/indexed /opt/app/lib/python3.13/site-packages/stac_fastapi/indexed

ARG DUCKDB_VERSION=1.2.2
WORKDIR /tmp
RUN  curl -o spatial.gz http://extensions.duckdb.org/v${DUCKDB_VERSION}/linux_amd64_gcc4/spatial.duckdb_extension.gz \
  && gunzip spatial.gz \
  && curl -o httpfs.gz http://extensions.duckdb.org/v${DUCKDB_VERSION}/linux_amd64_gcc4/httpfs.duckdb_extension.gz \
  && gunzip httpfs.gz \
  && curl -o aws.gz http://extensions.duckdb.org/v${DUCKDB_VERSION}/linux_amd64_gcc4/aws.duckdb_extension.gz \
  && gunzip aws.gz

ENV PYTHONPATH=/opt/app/lib/python3.13/site-packages
RUN python -c "import duckdb; duckdb.execute(\"INSTALL '/tmp/spatial'\"); duckdb.execute(\"INSTALL '/tmp/httpfs'\"); duckdb.execute(\"INSTALL '/tmp/aws'\")"
ENV stac_api_indexed_install_duckdb_extensions=false

ENV HOME=/datastore
RUN mkdir -p /datastore/.duckdb/extensions/v${DUCKDB_VERSION}/linux_amd64_gcc4 \
  && mv spatial /datastore/.duckdb/extensions/v${DUCKDB_VERSION}/linux_amd64_gcc4/spatial.duckdb_extension \
  && mv httpfs /datastore/.duckdb/extensions/v${DUCKDB_VERSION}/linux_amd64_gcc4/httpfs.duckdb_extension \
  && mv aws /datastore/.duckdb/extensions/v${DUCKDB_VERSION}/linux_amd64_gcc4/aws.duckdb_extension \
  && chmod 777 /datastore

WORKDIR /opt/app

CMD ["stac_fastapi.indexed.app.handler"]
