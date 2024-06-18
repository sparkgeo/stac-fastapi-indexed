FROM python:3.12-slim

WORKDIR /app
COPY stac_index_common ./stac_index_common
COPY stac_fastapi_indexed ./stac_fastapi_indexed

RUN pip install -e stac_index_common
RUN pip install -e stac_fastapi_indexed[server,s3_source]

CMD ["uvicorn", "stac_fastapi_indexed.app:app", "--host", "0.0.0.0", "--port", "80"]
