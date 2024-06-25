FROM debian:trixie-slim

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update --fix-missing \
  && apt-get install -y --no-install-recommends \
  ca-certificates \
  curl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /src
RUN curl -L -o gpq.tgz https://github.com/planetlabs/gpq/releases/download/v0.22.0/gpq-linux-amd64.tar.gz \
  && tar -xzf gpq.tgz \
  && rm -rf gpq.tgz

WORKDIR /usr/local/bin
RUN ln -s /src/gpq

WORKDIR /

CMD ["gpq", "version"]
