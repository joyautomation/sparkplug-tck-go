#!/usr/bin/env bash
# Pull the latest Sparkplug spec asciidoc from upstream and regenerate
# assertions.json. Run this when you suspect spec drift; CI runs it on a
# schedule and opens a PR if the diff is non-empty.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CACHE="${ROOT}/.spec-cache"
UPSTREAM="${SPARKPLUG_SPEC_REF:-master}"
BASE="https://raw.githubusercontent.com/eclipse-sparkplug/sparkplug/${UPSTREAM}/specification/src/main/asciidoc/chapters"

CHAPTERS=(
  Sparkplug_4_Topics
  Sparkplug_5_Operational_Behavior
  Sparkplug_6_Payloads
  Sparkplug_8_HA
)

mkdir -p "${CACHE}"
for ch in "${CHAPTERS[@]}"; do
  curl -fsSL "${BASE}/${ch}.adoc" -o "${CACHE}/${ch}.adoc"
done

cd "${ROOT}"
go run ./cmd/extract-assertions \
  "${CACHE}"/Sparkplug_4_Topics.adoc \
  "${CACHE}"/Sparkplug_5_Operational_Behavior.adoc \
  "${CACHE}"/Sparkplug_6_Payloads.adoc \
  "${CACHE}"/Sparkplug_8_HA.adoc \
  > assertions.json

echo "wrote assertions.json (ref: ${UPSTREAM})" >&2
