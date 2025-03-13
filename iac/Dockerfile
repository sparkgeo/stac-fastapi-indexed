FROM public.ecr.aws/lambda/python:3.12

ENV HOME=/tmp

WORKDIR /app

# see scripts/update-requirements.sh for detail on generated requirements
COPY docker/requirements-index-common-generated.txt /requirements-index-common-generated.txt
COPY docker/requirements-indexer-generated.txt /requirements-indexer-generated.txt
COPY docker/requirements-reader-s3-generated.txt /requirements-reader-s3-generated.txt
COPY docker/requirements-api-generated.txt /requirements-api-generated.txt
RUN pip install -r /requirements-index-common-generated.txt
RUN pip install -r /requirements-indexer-generated.txt
RUN pip install -r /requirements-reader-s3-generated.txt
RUN pip install -r /requirements-api-generated.txt

# Install DuckDB extensions in image rather than waiting for FastAPI init to install them.
# This should result in shorter cold-boot times.
RUN python -c "import duckdb; duckdb.execute('install spatial'); duckdb.execute('install httpfs')"

COPY stac_index/common ./stac_index/common
COPY stac_index/reader ./stac_index/reader
COPY stac_fastapi/indexed ./stac_fastapi/indexed

RUN pip install -e stac_index/common
RUN pip install -e stac_index/reader/s3
RUN pip install -e stac_fastapi/indexed[lambda,local-deps]

CMD ["stac_fastapi.indexed.app.handler"]
