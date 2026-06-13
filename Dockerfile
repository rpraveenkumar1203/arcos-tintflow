FROM rust:1-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies: build with a stub main first.
COPY Cargo.toml ./
COPY migrations ./migrations
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs && cargo build --release && rm -rf src

COPY src ./src
RUN find src -name "*.rs" | xargs touch && cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/tintflow /app/tintflow

EXPOSE 8090
ENV PORT=8090 \
    RUST_LOG=info

ENTRYPOINT ["/app/tintflow"]
