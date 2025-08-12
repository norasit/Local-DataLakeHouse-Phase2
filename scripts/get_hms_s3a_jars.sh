#!/usr/bin/env bash
set -euo pipefail

# ===== Versions (override ได้ตอนรัน) =====
HADOOP_VER="${HADOOP_VER:-3.4.1}"
AWS_BUNDLE_VER="${AWS_BUNDLE_VER:-2.24.6}"

# ===== Targets =====
OUT_DIR="${OUT_DIR:-./jars}"
SRC_HADOOP_AWS_JAR="hadoop-aws-${HADOOP_VER}.jar"
SRC_AWS_BUNDLE_JAR="bundle-${AWS_BUNDLE_VER}.jar"

# ชื่อ "คงที่" สำหรับใช้กับ docker-compose
DST_HADOOP_AWS_JAR="hadoop-aws.jar"
DST_AWS_BUNDLE_JAR="aws-sdk-bundle.jar"

HADOOP_AWS_URL="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VER}/${SRC_HADOOP_AWS_JAR}"
AWS_BUNDLE_URL="https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_BUNDLE_VER}/${SRC_AWS_BUNDLE_JAR}"

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
dl "${HADOOP_AWS_URL}"  "${OUT_DIR}/${SRC_HADOOP_AWS_JAR}"
dl "${AWS_BUNDLE_URL}"  "${OUT_DIR}/${SRC_AWS_BUNDLE_JAR}"

# 2) ทำไฟล์ชื่อคงที่สำหรับการ mount (ไม่ใช้ symlink เพื่อให้ Docker bind ง่าย)
copy_as "${OUT_DIR}/${SRC_HADOOP_AWS_JAR}" "${OUT_DIR}/${DST_HADOOP_AWS_JAR}"
copy_as "${OUT_DIR}/${SRC_AWS_BUNDLE_JAR}"  "${OUT_DIR}/${DST_AWS_BUNDLE_JAR}"

# 3) แสดงผล
echo
echo "Downloaded & prepared:"
ls -lh "${OUT_DIR}/${SRC_HADOOP_AWS_JAR}" "${OUT_DIR}/${SRC_AWS_BUNDLE_JAR}" \
       "${OUT_DIR}/${DST_HADOOP_AWS_JAR}" "${OUT_DIR}/${DST_AWS_BUNDLE_JAR}"

cat <<'EOF'

Next steps (docker-compose -> hive-metastore):

environment:
  SERVICE_NAME: metastore
  DB_DRIVER: postgres
  HIVE_AUX_JARS_PATH: /opt/hive/lib/postgres.jar:/opt/hive/lib/hadoop-aws.jar:/opt/hive/lib/aws-sdk-bundle.jar
  HADOOP_CLASSPATH:   /opt/hive/lib/hadoop-aws.jar:/opt/hive/lib/aws-sdk-bundle.jar
  METASTORE_AUX_JARS_PATH: /opt/hive/lib/hadoop-aws.jar:/opt/hive/lib/aws-sdk-bundle.jar

volumes:
  - ./metastore_conf/metastore-site.xml:/opt/hive/conf/metastore-site.xml:ro
  - ./jars/postgresql-42.7.3.jar:/opt/hive/lib/postgres.jar:ro
  - ./jars/hadoop-aws.jar:/opt/hive/lib/hadoop-aws.jar:ro
  - ./jars/aws-sdk-bundle.jar:/opt/hive/lib/aws-sdk-bundle.jar:ro

Then:
  docker compose restart hive-metastore

In Trino (Native S3):
  CREATE SCHEMA iceberg.lh;
# or
# CREATE SCHEMA iceberg.lh WITH (location='s3://lakehouse-data/iceberg');

EOF
