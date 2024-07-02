FROM python:3.12-alpine

RUN pip install \
    pytest \
    requests

WORKDIR /src
COPY tests/smoke_tests/test_api_endpoints_ok.py .

CMD ["python", "-m", "pytest"]
