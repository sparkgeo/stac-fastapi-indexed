FROM python:3.12-slim

RUN apt-get update --fix-missing \
  && apt-get install -y --no-install-recommends \
  jq \
  && rm -rf /var/lib/apt/lists/*

COPY docker/indexer/command.sh /command.sh

WORKDIR /app
COPY stac_index/common ./stac_index/common
COPY stac_index/reader ./stac_index/reader
COPY stac_index/indexer ./stac_index/indexer

RUN pip install -e stac_index/common
RUN pip install -e stac_index/reader/filesystem
RUN pip install -e stac_index/reader/s3
RUN pip install -e stac_index/indexer

CMD ["/command.sh"]
