FROM python:3.12-slim

WORKDIR /app
COPY stac_index/common ./stac_index/common
COPY stac_index/reader/s3 ./stac_index/reader/s3
COPY stac_index/indexer ./stac_index/indexer

RUN pip install -e stac_index/common
RUN pip install -e stac_index/reader/s3
RUN pip install -e stac_index/indexer

CMD ["python", "-m", "stac-indexer.index", "--help"]
