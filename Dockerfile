FROM alpine:latest
COPY target/x86_64-unknown-linux-musl/release/mktdarchive target/x86_64-unknown-linux-musl/release/td-archive-server ./
