FROM openjdk:11-slim-buster

ARG DEBIAN_FRONTEND=noninteractive

# ca-certificates-java must be installed prior to maven
RUN apt-get update --fix-missing \
  && apt-get install -y --no-install-recommends \
  ca-certificates-java \
  && rm -rf /var/lib/apt/lists/*

RUN apt-get update --fix-missing \
  && apt-get install -y --no-install-recommends \
  git \
  maven \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /src
RUN git clone https://github.com/opengeospatial/ets-ogcapi-features10.git

WORKDIR /src/ets-ogcapi-features10
RUN mvn clean package -Dsource=8

RUN mkdir /result
COPY docker/cite/test-run-props.xml /config/test-run-props.xml

WORKDIR /src/ets-ogcapi-features10/target
