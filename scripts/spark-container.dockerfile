# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Build the test and build container for presto_cpp
#
FROM ghcr.io/facebookincubator/velox-dev:centos8 

ARG SPARK_VERSION=3.5.1

ADD scripts /velox/scripts/
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/${SPARK_VERSION}/spark-connect_2.12-${SPARK_VERSION}.jar

ARG SPARK_PKG=spark-${SPARK_VERSION}-bin-hadoop3.tgz
ARG SPARK_CONNECT_JAR=spark-connect_2.12-${SPARK_VERSION}.jar

ENV SPARK_HOME="/opt/spark-server"

RUN dnf install -y java-11-openjdk less procps python3 tzdata \
    && ln -s $(which python3) /usr/bin/python \
    && tar -zxf $SPARK_PKG \
    && mv ./spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME \
    && mv ./$SPARK_CONNECT_JAR ${SPARK_HOME}/jars/

# We set the timezone to America/Los_Angeles due to issue
# detailed here : https://github.com/facebookincubator/velox/issues/8127
ENV TZ=America/Los_Angeles

COPY scripts/spark/conf/spark-defaults.conf.example $SPARK_HOME/conf/spark-defaults.conf
COPY scripts/spark/conf/spark-env.sh.example $SPARK_HOME/conf/spark-env.sh
COPY scripts/spark/conf/workers.example $SPARK_HOME/conf/workers
COPY scripts/spark/start-spark.sh /opt

WORKDIR /velox
