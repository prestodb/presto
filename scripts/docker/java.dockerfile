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

# Global arg default to share across stages
ARG SPARK_VERSION=3.5.1
ARG PRESTO_VERSION=0.293

#########################
# Stage: Spark Download #
#########################
# This allows us to cache the (slow) download until we change Spark version
FROM alpine:3.22 AS spark-download
ARG SPARK_VERSION

RUN wget -O spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz
RUN wget -O spark-connect.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/${SPARK_VERSION}/spark-connect_2.12-${SPARK_VERSION}.jar

RUN tar -zxf spark.tgz

##########################
# Stage: Presto Download #
##########################
# This allows us to cache the (slow) download until we change version
FROM alpine:3.22 AS presto-download
ARG PRESTO_VERSION

RUN wget -O presto-server.tar.gz \
    https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz
RUN wget -O presto-cli \
    https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar

RUN tar -xzf presto-server.tar.gz

#########################
# Stage: Java Base      #
#########################
FROM ghcr.io/facebookincubator/velox-dev:centos9 AS java-base

RUN dnf install -y -q --setopt=install_weak_deps=False java-11-openjdk less procps tzdata

# We set the timezone to America/Los_Angeles due to issue
# detailed here : https://github.com/facebookincubator/velox/issues/8127
ENV TZ=America/Los_Angeles

#########################
# Stage: Spark Server   #
#########################
FROM java-base AS spark-server
ARG SPARK_VERSION

ENV SPARK_HOME="/opt/spark-server"

COPY scripts/ci/spark/conf/spark-defaults.conf.example $SPARK_HOME/conf/spark-defaults.conf
COPY scripts/ci/spark/conf/spark-env.sh.example $SPARK_HOME/conf/spark-env.sh
COPY scripts/ci/spark/conf/workers.example $SPARK_HOME/conf/workers
COPY scripts/ci/spark/start-spark.sh /opt/

COPY --from=spark-download /spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME/
COPY --from=spark-download /spark-connect.jar $SPARK_HOME/misc/

WORKDIR /velox

#########################
# Stage: Presto Java    #
#########################
FROM java-base AS presto-java
ARG PRESTO_VERSION

ENV PRESTO_HOME="/opt/presto-server"

COPY scripts/ci/presto/etc/config.properties.example $PRESTO_HOME/etc/config.properties
COPY scripts/ci/presto/etc/jvm.config.example $PRESTO_HOME/etc/jvm.config
COPY scripts/ci/presto/etc/node.properties $PRESTO_HOME/etc/node.properties
COPY scripts/ci/presto/etc/hive.properties $PRESTO_HOME/etc/catalog/
COPY scripts/ci/presto/start-prestojava.sh /opt/

COPY --from=presto-download /presto-server-$PRESTO_VERSION  $PRESTO_HOME/
COPY --from=presto-download --chmod=755 /presto-cli  /opt/

RUN ln -s /opt/presto-cli /usr/local/bin/ && \
      mkdir -p $PRESTO_HOME/etc/data && \
      mkdir -p /usr/lib/presto/utils


WORKDIR /velox
