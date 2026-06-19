FROM rust:1-slim-bookworm AS builder

# Limit codegen/linker parallelism to keep peak CPU+RAM low (avoids
# stressing the Snapdragon/Hyper-V hypervisor into a HYPERVISOR_ERROR).
ARG CARGO_BUILD_JOBS=1
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS} \
    CARGO_HTTP_TIMEOUT=600 \
    CARGO_NET_RETRY=10 \
    CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies: build with a stub main first.
COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs && cargo build --release && rm -rf src

COPY src ./src
RUN find src -name "*.rs" | xargs touch && cargo build --release

FROM gcr.io/distroless/cc-debian12

WORKDIR /app
COPY --from=builder /app/target/release/tintflow /app/tintflow

EXPOSE 8090
ENV PORT=8090 \
    RUST_LOG=info

ENTRYPOINT ["/app/tintflow"]
