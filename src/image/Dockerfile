# Derive a Spark-capable image from your existing Slurm base image.
#
# Build (from repo root):
#   docker build \
#     --build-arg BASE=<your-slurm-image>:<tag> \
#     -f src/image/Dockerfile -t spark-on-slurm:3.5.3 .
ARG BASE
FROM ${BASE}

ARG SPARK_VERSION=3.5.3
ARG HADOOP_PROFILE=3
ARG JDK_VERSION=17.0.12+7

COPY src/image/install-spark.sh src/image/requirements.txt /tmp/
RUN chmod +x /tmp/install-spark.sh \
 && SPARK_VERSION=${SPARK_VERSION} \
    HADOOP_PROFILE=${HADOOP_PROFILE} \
    JDK_VERSION=${JDK_VERSION} \
    /tmp/install-spark.sh \
 && rm /tmp/install-spark.sh /tmp/requirements.txt

ENV JAVA_HOME=/opt/jdk \
    SPARK_HOME=/opt/spark \
    PATH=/opt/jdk/bin:/opt/spark/bin:/opt/spark/sbin:$PATH
