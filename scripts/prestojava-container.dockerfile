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
FROM ghcr.io/facebookincubator/velox-dev:centos9 

ARG PRESTO_VERSION=0.288

ADD scripts /velox/scripts/
RUN wget https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz
RUN wget https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar

ARG PRESTO_PKG=presto-server-$PRESTO_VERSION.tar.gz
ARG PRESTO_CLI_JAR=presto-cli-$PRESTO_VERSION-executable.jar

ENV PRESTO_HOME="/opt/presto-server"
RUN cp $PRESTO_CLI_JAR /opt/presto-cli

RUN dnf install -y java-11-openjdk less procps python3 tzdata \
    && ln -s $(which python3) /usr/bin/python \
    && tar -zxf $PRESTO_PKG \
    && mv ./presto-server-$PRESTO_VERSION $PRESTO_HOME \
    && chmod +x /opt/presto-cli \
    && ln -s /opt/presto-cli /usr/local/bin/ \
    && mkdir -p $PRESTO_HOME/etc \
    && mkdir -p $PRESTO_HOME/etc/catalog \
    && mkdir -p $PRESTO_HOME/etc/data \
    && mkdir -p /usr/lib/presto/utils

# We set the timezone to America/Los_Angeles due to issue
# detailed here : https://github.com/facebookincubator/velox/issues/8127
ENV TZ=America/Los_Angeles

COPY scripts/presto/etc/config.properties.example $PRESTO_HOME/etc/config.properties
COPY scripts/presto/etc/jvm.config.example $PRESTO_HOME/etc/jvm.config
COPY scripts/presto/etc/node.properties $PRESTO_HOME/etc/node.properties
COPY scripts/presto/etc/hive.properties $PRESTO_HOME/etc/catalog
COPY scripts/presto/start-prestojava.sh /opt

WORKDIR /velox
