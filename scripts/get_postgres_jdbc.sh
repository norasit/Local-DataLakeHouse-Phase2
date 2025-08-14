#!/usr/bin/env bash
set -euo pipefail

# ===== Config =====
POSTGRES_VER="${POSTGRES_VER:-42.7.3}"
OUT_DIR="${OUT_DIR:-./jars}"

JAR_NAME="postgresql-${POSTGRES_VER}.jar"
URL="https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_VER}/${JAR_NAME}"

mkdir -p "${OUT_DIR}"
DEST="${OUT_DIR}/${JAR_NAME}"

# Guard against a directory named like the target file
if [[ -d "${DEST}" ]]; then
  echo "✖ Destination is a directory (should be a file): ${DEST}" >&2
  exit 1
fi

if [[ -f "${DEST}" ]]; then
  echo "✔ ${DEST} already exists — skipping download"
  exit 0
fi

echo "↓ Downloading ${JAR_NAME} ..."
curl -fL --retry 3 --connect-timeout 10 --progress-bar -o "${DEST}.part" "${URL}"
mv "${DEST}.part" "${DEST}"
echo "✔ Saved to ${DEST}"
