FROM ubuntu:24.04 AS build
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential git curl ca-certificates cmake pkg-config python3 \
      libssl-dev libsasl2-dev zlib1g-dev libzstd-dev liblz4-dev \
      libcurl4-openssl-dev libjansson-dev
RUN git clone --depth 1 https://github.com/edenhill/kcat.git /src
WORKDIR /src
RUN ./bootstrap.sh && ./kcat -V | grep -q Avro

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates libssl3t64 libsasl2-2 zlib1g libzstd1 liblz4-1 \
      libcurl4t64 libjansson4 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/kcat /usr/local/bin/kcat
ENTRYPOINT ["kcat"]
CMD ["-V"]
