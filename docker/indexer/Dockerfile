FROM python:3.12-slim

RUN apt-get update --fix-missing \
  && apt-get install -y --no-install-recommends \
  jq \
  && rm -rf /var/lib/apt/lists/*

COPY docker/indexer/command.sh /command.sh

# see scripts/update-requirements.sh for detail on generated requirements
COPY docker/requirements-index-common-generated.txt /requirements-index-common-generated.txt
COPY docker/requirements-indexer-generated.txt /requirements-indexer-generated.txt
COPY docker/requirements-reader-filesystem-generated.txt /requirements-reader-filesystem-generated.txt
COPY docker/requirements-reader-filesystem-generated.txt /requirements-reader-https-generated.txt
COPY docker/requirements-reader-s3-generated.txt /requirements-reader-s3-generated.txt
RUN pip install -r /requirements-index-common-generated.txt
RUN pip install -r /requirements-indexer-generated.txt
RUN pip install -r /requirements-reader-filesystem-generated.txt
RUN pip install -r /requirements-reader-https-generated.txt
RUN pip install -r /requirements-reader-s3-generated.txt

WORKDIR /app
COPY stac-index.common ./stac-index.common
COPY stac_index/reader ./stac_index/reader
COPY stac_index/indexer ./stac_index/indexer

RUN pip install -e stac-index.common
RUN pip install -e stac_index/reader/filesystem
RUN pip install -e stac_index/reader/https
RUN pip install -e stac_index/reader/s3
RUN pip install -e stac_index/indexer[local-deps]

CMD ["/command.sh"]
