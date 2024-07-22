FROM public.ecr.aws/lambda/python:3.12

ENV HOME=/tmp

WORKDIR /app

COPY stac_index/common ./stac_index/common
COPY stac_index/reader ./stac_index/reader
COPY stac_fastapi/indexed ./stac_fastapi/indexed

RUN pip install -e stac_index/common
RUN pip install -e stac_index/reader/s3
RUN pip install -e stac_fastapi/indexed[server]
RUN pip install -e stac_fastapi/indexed[lambda]

CMD ["stac_fastapi.indexed.app.handler"]