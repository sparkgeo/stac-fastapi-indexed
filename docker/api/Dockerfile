FROM python:3.12-slim

RUN apt-get update --fix-missing \
  && apt-get install -y --no-install-recommends \
  curl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY stac_index/common ./stac_index/common
COPY stac_index/reader ./stac_index/reader
COPY stac_fastapi/indexed ./stac_fastapi/indexed

RUN pip install -e stac_index/common
RUN pip install -e stac_index/reader/s3
RUN pip install -e stac_fastapi/indexed[server]

CMD ["uvicorn", "stac_fastapi.indexed.app:app", "--host", "0.0.0.0", "--port", "80"]
