#!/usr/bin/env bash
# Regenerate Go bindings for the Sparkplug B protobuf.
#
# The .proto under proto/sparkplug_b.proto is vendored byte-for-byte from
# eclipse-tahu/tahu so re-vendoring is a clean diff. The Go package path is
# overridden via --go_opt so we don't have to patch the .proto.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

PATH="${PATH}:${HOME}/go/bin"

mkdir -p internal/spbpb
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

protoc \
  --proto_path=proto \
  --go_out="${TMP}" \
  --go_opt=Msparkplug_b.proto=github.com/joyautomation/sparkplug-tck-go/internal/spbpb \
  proto/sparkplug_b.proto

# Without paths=source_relative, protoc-gen-go writes to a path derived from
# the resolved go_package option. We just want the file flat in internal/spbpb.
mv "${TMP}/github.com/joyautomation/sparkplug-tck-go/internal/spbpb/sparkplug_b.pb.go" \
   internal/spbpb/sparkplug_b.pb.go

echo "wrote internal/spbpb/sparkplug_b.pb.go" >&2
