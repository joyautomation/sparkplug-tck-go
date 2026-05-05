#!/usr/bin/env bash
# Pull the upstream Eclipse Sparkplug TCK Java sources and inventory
# which spec assertion IDs each upstream test class references in its
# `testIds = List.of(...)` declaration. Output is checked into
# upstream_tests.json so the parity bench can score per-upstream-test
# coverage without a network fetch.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CACHE="${ROOT}/.upstream-tck-cache"
UPSTREAM="${SPARKPLUG_TCK_REF:-master}"
TARBALL_URL="https://github.com/eclipse-sparkplug/sparkplug/archive/${UPSTREAM}.tar.gz"

mkdir -p "${CACHE}"
TARBALL="${CACHE}/sparkplug-${UPSTREAM}.tar.gz"
if [[ ! -f "${TARBALL}" ]]; then
  curl -fsSL "${TARBALL_URL}" -o "${TARBALL}"
fi
SRC_DIR="${CACHE}/sparkplug-${UPSTREAM}"
if [[ ! -d "${SRC_DIR}" ]]; then
  tar -xzf "${TARBALL}" -C "${CACHE}"
fi

TEST_DIR="${SRC_DIR}/tck/src/main/java/org/eclipse/sparkplug/tck/test"

python3 - "$TEST_DIR" "${ROOT}/upstream_tests.json" <<'PYEOF'
"""Map each upstream test file to the list of tck-id-* it references via
testIds = List.of(...). Java constants ID_FOO_BAR_311 map to
tck-id-foo-bar-311 (lowercase, dashes, drop leading ID_)."""
import json, os, re, sys

test_dir, out_path = sys.argv[1], sys.argv[2]
inventory = []
for sub in ("broker", "edge", "host"):
    d = os.path.join(test_dir, sub)
    if not os.path.isdir(d):
        continue
    for fname in sorted(os.listdir(d)):
        if not fname.endswith(".java"):
            continue
        text = open(os.path.join(d, fname)).read()
        m = re.search(r"testIds\s*=\s*List\.of\((.*?)\);", text, re.DOTALL)
        if not m:
            continue
        body = m.group(1)
        body = re.sub(r"/\*.*?\*/", "", body, flags=re.DOTALL)
        body = re.sub(r"//.*", "", body)
        ids = re.findall(r"\bID_[A-Z0-9_]+\b", body)
        spids = sorted({"tck-id-" + c[3:].lower().replace("_", "-") for c in ids})
        inventory.append({"file": f"{sub}/{fname}", "ids": spids})

inventory.sort(key=lambda t: t["file"])
with open(out_path, "w") as f:
    json.dump(inventory, f, indent=2)
    f.write("\n")
total = sorted({i for t in inventory for i in t["ids"]})
print(f"wrote {out_path}: {len(inventory)} tests, {len(total)} unique IDs",
      file=sys.stderr)
PYEOF
