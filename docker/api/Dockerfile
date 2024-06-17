FROM python:3.12-slim

WORKDIR /app/src
COPY src/stac_fastapi_indexed ./stac_fastapi_indexed
COPY src/stac_index_common ./stac_index_common

WORKDIR /app
COPY pyproject.toml .
COPY setup.py .

RUN pip install .[s3_source,server]

CMD ["uvicorn", "stac_fastapi_indexed.app:app", "--host", "0.0.0.0", "--port", "8000"]
