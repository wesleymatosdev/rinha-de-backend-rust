FROM rust:1.88-slim-bookworm as builder
WORKDIR /app
COPY . .
RUN cargo install --path .
CMD ["rinha-de-backend-rust"]