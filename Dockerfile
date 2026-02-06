FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build


RUN git clone --recursive https://github.com/saturnines/lygus.git .

RUN mkdir -p build && cd build && \
    cmake .. -DCMAKE_BUILD_TYPE=Release && \
    make -j$(nproc)


FROM ubuntu:24.04

RUN apt-get update && apt-get install -y iptables && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/build/lygus-server /app/lygus-server

RUN mkdir -p /data

EXPOSE 8080 5000 5001

ENTRYPOINT ["stdbuf", "-oL", "-eL", "/app/lygus-server", "-v"]