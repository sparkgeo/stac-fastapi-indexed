FROM python:3.10-slim-bookworm

RUN apt-get update --fix-missing \
  && apt-get install -y --no-install-recommends \
  curl \
  && rm -rf /var/lib/apt/lists/*

RUN pip install \
    stac-api-validator~=0.6.2

WORKDIR /scripts
COPY docker/tests/wait_for_api.sh .
COPY docker/tests/validator/command.sh .

CMD [ "/scripts/command.sh" ]
