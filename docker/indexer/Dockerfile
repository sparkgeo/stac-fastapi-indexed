FROM python:3.13-slim AS build

SHELL ["sh", "-exc"]

### Start build prep.

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=python3.13 \
    UV_PROJECT_ENVIRONMENT=/opt/app

### End build prep -- this is where your app Dockerfile should start.
COPY ./ /src
WORKDIR /src

RUN uv sync \
    --package=stac-index \
    --directory=/src \
    --locked \
    --no-dev \
    --no-install-project

RUN uv sync \
    --package=stac-index \
    --directory=/src \
    --locked \
    --no-dev \
    --no-editable

# ##########################################################################

FROM python:3.13-slim
SHELL ["sh", "-exc"]

ARG api_port=8000
ARG num_workers=2

ENV PORT=${api_port}
ENV WORKERS=${num_workers}
ENV PATH=/opt/app/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgeos3.11.1 \
    jq \
    && rm -rf /var/lib/apt/lists/*

STOPSIGNAL SIGINT

COPY --from=build /opt/app /opt/app
COPY docker/indexer/command.sh /command.sh

WORKDIR /opt/app

CMD ["/command.sh"]