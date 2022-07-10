FROM rust:1.61.0-bullseye as build-env

WORKDIR /app

COPY Cargo.toml /app/Cargo.toml
COPY Cargo.lock /app/Cargo.lock
COPY springql /app/springql
COPY springql-core /app/springql-core
COPY foreign-service /app/foreign-service
COPY test-logger /app/test-logger

RUN cargo build --release
RUN cargo build --release --examples

FROM gcr.io/distroless/cc as target
COPY --from=build-env /app/target/release/examples/doc_app1 doc_app1
COPY --from=build-env /app/target/release/examples/doc_app2 doc_app2
COPY --from=build-env /app/target/release/examples/http_client_sink_writer http_client_sink_writer
COPY --from=build-env /app/target/release/examples/in_vehicle_pipeline in_vehicle_pipeline

CMD ["/doc_app1"]
