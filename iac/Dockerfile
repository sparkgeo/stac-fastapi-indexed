FROM public.ecr.aws/lambda/python:3.12

ENV HOME=/tmp
COPY stac_index_common ./stac_index_common
COPY stac_fastapi_indexed ./stac_fastapi_indexed

RUN pip install uvicorn mangum
RUN pip install -e stac_index_common
RUN pip install -e stac_fastapi_indexed[server,s3_source]

CMD ["stac_fastapi_indexed.app.handler"]

