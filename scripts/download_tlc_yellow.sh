#!/usr/bin/env bash
# scripts/download_tlc_yellow.sh
# Download NYC TLC Yellow Taxi Parquet files: Jan 2024 -> Jun 2025
# Output layout: datasets/nyc_tlc/yellow/<year>/yellow_tripdata_<year>-<mm>.parquet

set -euo pipefail

# Resolve repo root (parent of this script directory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

OUT_ROOT="${REPO_ROOT}/datasets/nyc_tlc/yellow"
BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"

have_cmd() { command -v "$1" >/dev/null 2>&1; }

download() {
  local url="$1"
  local dest="$2"
  local dest_dir
  dest_dir="$(dirname "$dest")"
  mkdir -p "$dest_dir"

  if [[ -s "$dest" ]]; then
    echo "✅ Exists, skip: $dest"
    return 0
  fi

  echo "⬇️  Downloading: $url"
  if have_cmd wget; then
    # -c resume, -O write to path
    wget -c -O "$dest" "$url"
  elif have_cmd curl; then
    # -C - resume, -L follow redirects, -o output
    curl -C - -L "$url" -o "$dest"
  else
    echo "❌ Need either 'wget' or 'curl' installed." >&2
    exit 1
  fi
}

main() {
  echo "Output root: $OUT_ROOT"
  local years=("2024" "2025")

  for y in "${years[@]}"; do
    local months
    if [[ "$y" == "2024" ]]; then
      months=(01 02 03 04 05 06 07 08 09 10 11 12)
    else
      months=(01 02 03 04 05 06)   # up to June 2025
    fi

    for m in "${months[@]}"; do
      local fname="yellow_tripdata_${y}-${m}.parquet"
      local url="${BASE_URL}/${fname}"
      local dest="${OUT_ROOT}/${y}/${fname}"
      download "$url" "$dest" || echo "⚠️  Failed: $url"
    done
  done

  echo
  echo "📦 Summary under datasets/nyc_tlc/yellow:"
  du -sh "${OUT_ROOT}"/* 2>/dev/null || true
  echo "✅ Done."
}

main "$@"
