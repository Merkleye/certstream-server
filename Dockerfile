# syntax=docker/dockerfile:1.7
FROM rust:1.95-alpine AS builder
ARG TARGETPLATFORM

# build-base + make: tikv-jemalloc-sys compiles jemalloc from source.
RUN apk add --no-cache build-base make musl-dev pkgconf openssl-dev openssl-libs-static

WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src

ENV OPENSSL_STATIC=1
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    case "$TARGETPLATFORM" in \
      "linux/amd64"|"") export RUSTFLAGS="-C target-cpu=x86-64-v3" ;; \
    esac && \
    cargo build --release && \
    cp target/release/certstream-server-rust /usr/local/bin/certstream-server-rust

FROM alpine:3.21

ARG OCI_VERSION=0.0.0
ARG OCI_REVISION=unknown
ARG OCI_CREATED=unknown
ARG OCI_REF_NAME=dev
ARG OCI_SOURCE=https://github.com/merkleye/certstream-server

LABEL org.opencontainers.image.title="Merkleye certstream-server" \
      org.opencontainers.image.description="Unfiltered Certificate Transparency firehose (certstream-server-rust)" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.source="${OCI_SOURCE}" \
      org.opencontainers.image.url="${OCI_SOURCE}" \
      org.opencontainers.image.version="${OCI_VERSION}" \
      org.opencontainers.image.revision="${OCI_REVISION}" \
      org.opencontainers.image.created="${OCI_CREATED}" \
      org.opencontainers.image.ref.name="${OCI_REF_NAME}"

RUN apk add --no-cache ca-certificates curl

COPY --from=builder /usr/local/bin/certstream-server-rust /usr/local/bin/

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:8080/health/deep | grep -q '"status":"healthy"' || exit 1

CMD ["certstream-server-rust"]
