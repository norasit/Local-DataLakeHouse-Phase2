#!/usr/bin/env bash
set -euo pipefail

# ===== Versions (override ได้ตอนรัน) =====
ICEBERG_VER="${ICEBERG_VER:-1.9.2}"
SPARK_VER="${SPARK_VER:-3.5}"      # 3.5 หรือ 3.4 ก็ได้ (ขึ้นกับ image Spark ที่ใช้)
SCALA_VER="${SCALA_VER:-2.12}"     # ส่วนใหญ่ Spark 3.x ใช้ 2.12

# ===== Targets =====
OUT_DIR="${OUT_DIR:-./jars}"

# ชื่อไฟล์ตาม Maven Central ของ Iceberg
SRC_ICEBERG_SPARK_JAR="iceberg-spark-runtime-${SPARK_VER}_${SCALA_VER}-${ICEBERG_VER}.jar"

# ชื่อ "คงที่" สำหรับ mount ใน Docker (ลดการแก้ compose เมื่ออัปเวอร์ชัน)
DST_ICEBERG_SPARK_JAR="iceberg-spark-runtime.jar"

ICEBERG_SPARK_URL="https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VER}_${SCALA_VER}/${ICEBERG_VER}/${SRC_ICEBERG_SPARK_JAR}"

mkdir -p "${OUT_DIR}"

dl() {
  local url="$1" dest="$2"
  if [[ -f "${dest}" ]]; then
    echo "✔ ${dest} already exists — skipping download"
    return
  fi
  echo "↓ Downloading $(basename "${dest}") ..."
  curl -fL --retry 3 --connect-timeout 10 --progress-bar -o "${dest}.part" "${url}"
  mv "${dest}.part" "${dest}"
  echo "✔ Saved to ${dest}"
}

copy_as() {
  local src="$1" dst="$2"
  if [[ ! -f "${src}" ]]; then
    echo "✖ Source not found: ${src}" >&2
    exit 1
  fi
  cp -f "${src}" "${dst}"
  echo "✔ Prepared ${dst}"
}

# 1) Download
dl "${ICEBERG_SPARK_URL}" "${OUT_DIR}/${SRC_ICEBERG_SPARK_JAR}"

# 2) ทำไฟล์ชื่อคงที่สำหรับการ mount (ไม่ใช้ symlink เพื่อให้ Docker bind ง่าย)
copy_as "${OUT_DIR}/${SRC_ICEBERG_SPARK_JAR}" "${OUT_DIR}/${DST_ICEBERG_SPARK_JAR}"

# 3) แสดงผล
echo
echo "Downloaded & prepared:"
ls -lh "${OUT_DIR}/${SRC_ICEBERG_SPARK_JAR}" "${OUT_DIR}/${DST_ICEBERG_SPARK_JAR}"

cat <<'EOF'

Next steps (Spark container):

# ตัวอย่าง volumes ที่จะใส่ใน docker-compose service "spark"
#   - ./spark/conf:/opt/spark/conf:ro
#   - ./jars/iceberg-spark-runtime.jar:/opt/spark/jars/iceberg-spark-runtime.jar:ro
#   - ./jars/hadoop-aws.jar:/opt/spark/jars/hadoop-aws.jar:ro
#   - ./jars/aws-sdk-bundle.jar:/opt/spark/jars/aws-sdk-bundle.jar:ro
#
# ตรวจสอบเวอร์ชัน Scala/Spark ของ image ที่ใช้ให้ตรงกับค่าในสคริปต์นี้:
#   docker exec <spark-container> spark-submit --version | grep -E 'version|Scala'
#
# หาก image เป็น Spark 3.4 ให้รันแบบนี้:
#   SPARK_VER=3.4 ./scripts/get_iceberg_spark_jar.sh
#
# หาก image รายงาน Scala 2.13 (พบน้อย) ให้:
#   SCALA_VER=2.13 ./scripts/get_iceberg_spark_jar.sh

EOF
