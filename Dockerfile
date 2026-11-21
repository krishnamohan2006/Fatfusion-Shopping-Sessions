FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
COPY --chmod=0555 bin/engine-linux-amd64 /app/engine
USER 1000:1000
ENTRYPOINT ["/app/engine"]
