FROM python:3.12-slim

WORKDIR /app
COPY stac_index_common ./stac_index_common
COPY stac_indexer ./stac_indexer

RUN pip install -e stac_index_common
RUN pip install -e stac_indexer[s3_source]

CMD ["python", "-m", "stac_indexer.index", "--help"]
