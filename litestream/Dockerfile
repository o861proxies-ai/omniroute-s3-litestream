##############################################################
# litestream/Dockerfile
#
# Base: node:20-alpine (có Node.js để chạy startup.js)
# + litestream binary tải từ GitHub releases
#
# Lý do không dùng image litestream/litestream:latest:
#   image đó dùng scratch/alpine không có Node.js.
#   Chúng ta cần Node.js để chạy startup.js.
##############################################################

FROM node:20-alpine

# Cài litestream binary
ARG LITESTREAM_VERSION=0.3.13
RUN apk add --no-cache wget ca-certificates && \
    wget -qO /tmp/litestream.tar.gz \
      "https://github.com/benbjohnson/litestream/releases/download/v${LITESTREAM_VERSION}/litestream-v${LITESTREAM_VERSION}-linux-amd64.tar.gz" && \
    tar -xzf /tmp/litestream.tar.gz -C /usr/local/bin litestream && \
    chmod +x /usr/local/bin/litestream && \
    rm /tmp/litestream.tar.gz

WORKDIR /app

COPY startup.js /startup.js

ENTRYPOINT ["node", "/startup.js"]