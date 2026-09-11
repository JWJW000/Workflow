FROM rust:alpine AS build

RUN apk add --no-cache build-base musl-dev pkgconf openssl-dev openssl-libs-static
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
ENV OPENSSL_STATIC=1
RUN cargo build --release --locked -p workflow-api -p workflow-runner -p workflow-cli

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


FROM runtime-base AS academic
USER root
RUN apk add --no-cache python3 py3-pypdf procps
COPY --from=build /src/target/release/drission-workflow /usr/local/bin/drission-workflow
COPY examples/academic /app/examples/academic
COPY examples/templates /app/examples/templates
RUN mkdir -p /app/academic-data /app/.drission-workflow && chown -R 10001:10001 /app/academic-data /app/.drission-workflow
ENV XDG_CONFIG_HOME=/app/.drission-workflow/config XDG_CACHE_HOME=/app/.drission-workflow/cache DRISSION_ROOT=/app DRISSION_CHROME_BIN=/usr/bin/chromium DRISSION_CHROME_HEADLESS=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 10001:10001
EXPOSE 8899
ENTRYPOINT ["python3", "/app/examples/academic/status_server.py"]
CMD ["--serve", "8899", "--host", "0.0.0.0", "--doi-target", "/app/academic-data", "--export", "/app/.drission-workflow/doi_status.json", "--max-parallel", "1"]
