#!/usr/bin/env bash
# Install a nats-server binary on Linux (used by CI). Usage: install-nats-server.sh [version] [dir]
set -euo pipefail
VERSION=${1:-v2.15.0}
DEST=${2:-/usr/local/bin}
ARCH=$(uname -m); case $ARCH in x86_64) ARCH=amd64;; aarch64) ARCH=arm64;; esac
NAME=nats-server-$VERSION-linux-$ARCH
curl -fsSL "https://github.com/nats-io/nats-server/releases/download/$VERSION/$NAME.tar.gz" | tar xz -C /tmp
install "/tmp/$NAME/nats-server" "$DEST/nats-server"
nats-server --version
