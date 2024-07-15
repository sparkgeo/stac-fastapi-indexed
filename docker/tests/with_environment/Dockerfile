FROM python:3.12-alpine

RUN pip install \
    pytest~=8.2.2 \
    requests~=2.32.3

COPY data/STAC/sample /STAC
ENV STAC_JSON_ROOT_DIR=/STAC/data

WORKDIR /src
COPY tests/with_environment ./with_environment
