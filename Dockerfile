FROM rust:alpine AS build

RUN apk add --no-cache build-base musl-dev pkgconf openssl-dev openssl-libs-static
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
ENV OPENSSL_STATIC=1
RUN cargo build --release --locked -p workflow-api -p workflow-runner

FROM alpine:3.22 AS runtime-base
RUN apk add --no-cache ca-certificates curl tzdata chromium openssl libstdc++
RUN addgroup -S -g 10001 workflow && adduser -S -D -H -u 10001 -G workflow workflow
WORKDIR /app
RUN mkdir -p /app/workflow-data && chown -R 10001:10001 /app/workflow-data
USER 10001:10001

FROM runtime-base AS api
COPY --from=build /src/target/release/drission-workflow-api /usr/local/bin/drission-workflow-api
COPY --from=build /src/target/release/drission-workflow-runner /usr/local/bin/drission-workflow-runner
EXPOSE 8787
ENTRYPOINT ["/usr/local/bin/drission-workflow-api"]
CMD ["--listen", "0.0.0.0:8787", "--database", "/app/workflow-data/workflows.db", "--artifacts", "/app/workflow-data/artifacts", "--runner", "/usr/local/bin/drission-workflow-runner"]

