# Image for building the project
FROM rust:bookworm
# 1. create empty shell project
RUN USER=root cargo new --bin app
WORKDIR /app
COPY Cargo.toml Cargo.lock .
# 2. build only dependencies to cache them
RUN cargo build --release
# 3. build the source code of the project
RUN rm -r ./src/* ./target/release/deps/sop*
COPY ./src ./src
RUN cargo build --release
ENTRYPOINT ["/bin/sh"]

## inspired from https://dev.to/rogertorres/first-steps-with-docker-rust-30oi
