FROM public.ecr.aws/lambda/python:3.12

ENV HOME=/tmp
COPY stac_index_common ./stac_index_common
COPY stac_fastapi_indexed ./stac_fastapi_indexed

RUN pip install -e stac_index/common
RUN pip install -e stac_index/reader/s3
RUN pip install -e stac_fastapi/indexed[server]
RUN pip install -e stac_fastapi/indexed[lambda]

CMD ["stac_fastapi.indexed.app.handler"]