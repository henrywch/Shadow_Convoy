#!/usr/bin/env bash
# Install JDK 17 (Temurin) + Apache Spark into a container image.
# Idempotent: rerunning is a no-op once both are present at $PREFIX.
#
# Intended usage:
#   - As a RUN step in a Dockerfile that derives from your Slurm base image.
#   - Or run manually inside an interactive container of that image.
#
# Override knobs (env vars):
#   PREFIX           install root                 (default /opt)
#   SPARK_VERSION    Apache Spark version         (default 3.5.3)
#   HADOOP_PROFILE   bundled Hadoop client major  (default 3)
#   JDK_VERSION      Temurin tag, e.g. 17.0.12+7  (default 17.0.12+7)
set -euo pipefail

PREFIX="${PREFIX:-/opt}"
SPARK_VERSION="${SPARK_VERSION:-3.5.3}"
HADOOP_PROFILE="${HADOOP_PROFILE:-3}"
JDK_VERSION="${JDK_VERSION:-17.0.12+7}"

export JAVA_HOME="$PREFIX/jdk"
export SPARK_HOME="$PREFIX/spark"

need() { command -v "$1" >/dev/null 2>&1 || { echo "[install] missing required tool: $1" >&2; exit 1; }; }
need curl; need tar; need python3

# Considered "installed" only if the binary exists AND actually runs — guards
# against half-extracted tarballs or ABI-incompatible leftovers from a prior run.
healthy() { [ -x "$1" ] && "$1" $2 >/dev/null 2>&1; }

mkdir -p "$PREFIX"

if ! healthy "$JAVA_HOME/bin/java" -version; then
  echo "[install] Temurin JDK $JDK_VERSION -> $JAVA_HOME"
  v="${JDK_VERSION//+/_}"
  curl -fsSL -o /tmp/jdk.tgz \
    "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-${JDK_VERSION}/OpenJDK17U-jdk_x64_linux_hotspot_${v}.tar.gz"
  rm -rf "$JAVA_HOME" && mkdir -p "$JAVA_HOME"
  tar -xzf /tmp/jdk.tgz -C "$JAVA_HOME" --strip-components=1
  rm /tmp/jdk.tgz
fi

if ! healthy "$SPARK_HOME/bin/spark-submit" --version; then
  echo "[install] Apache Spark $SPARK_VERSION (hadoop$HADOOP_PROFILE) -> $SPARK_HOME"
  curl -fsSL -o /tmp/spark.tgz \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_PROFILE}.tgz"
  rm -rf "$SPARK_HOME" && mkdir -p "$SPARK_HOME"
  tar -xzf /tmp/spark.tgz -C "$SPARK_HOME" --strip-components=1
  rm /tmp/spark.tgz
fi

# Python deps that pyspark needs at runtime (numpy for ml/mllib, etc.).
REQS="$(dirname "${BASH_SOURCE[0]}")/requirements.txt"
if [ -f "$REQS" ]; then
  echo "[install] pip install -r $REQS"
  python3 -m pip install --no-cache-dir --upgrade pip
  python3 -m pip install --no-cache-dir -r "$REQS"
fi

# Make Spark visible to login shells inside the image.
install -d /etc/profile.d
cat > /etc/profile.d/spark.sh <<EOF
export JAVA_HOME=$JAVA_HOME
export SPARK_HOME=$SPARK_HOME
export PATH=\$JAVA_HOME/bin:\$SPARK_HOME/bin:\$SPARK_HOME/sbin:\$PATH
EOF

echo "[install] done."
"$JAVA_HOME/bin/java" -version
"$SPARK_HOME/bin/spark-submit" --version 2>&1 | tail -3
