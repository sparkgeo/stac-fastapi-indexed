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

# Files are required to successfully parse package.toml, even though the project is not installed at this stage.
COPY packages/stac-index/pyproject.toml ./packages/stac-index/
COPY packages/stac-index/README.md ./packages/stac-index/

# Install dependencies first, then application code, as dependencies change less frequently.
RUN uv sync \
    --package=stac-index \
    --locked \
    --no-dev \
    --no-install-project

# Copy and install application code.
COPY packages/stac-index/* ./packages/stac-index/
RUN uv sync \
    --package=stac-index \
    --locked \
    --no-dev \
    --no-editable

# ##########################################################################

FROM python:3.13-slim
SHELL ["/bin/sh", "-exc"]

ENV PATH=/opt/app/bin:$PATH

STOPSIGNAL SIGINT

COPY --from=build /opt/app /opt/app
COPY docker/indexer/command.sh /command.sh

WORKDIR /opt/app

CMD ["/command.sh"]
