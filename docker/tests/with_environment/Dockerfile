FROM python:3.12-slim-bookworm

RUN apt-get update --fix-missing \
  && apt-get install -y --no-install-recommends \
  geos-bin \
  && rm -rf /var/lib/apt/lists/*

RUN pip install \
    pytest~=8.2.2 \
    requests~=2.32.3 \
    shapely~=2.0.4

COPY data/STAC/sample /STAC
ENV STAC_JSON_ROOT_DIR=/STAC/data

WORKDIR /src
COPY tests/with_environment ./with_environment
