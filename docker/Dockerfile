FROM quay.io/centos/centos:stream9

ARG PRESTO_VERSION
ARG PRESTO_PKG=presto-server-$PRESTO_VERSION.tar.gz
ARG PRESTO_CLI_JAR=presto-cli-$PRESTO_VERSION-executable.jar
ARG JMX_PROMETHEUS_JAVAAGENT_VERSION=0.20.0

ENV PRESTO_HOME="/opt/presto-server"

COPY $PRESTO_PKG .
COPY $PRESTO_CLI_JAR /opt/presto-cli

RUN dnf install -y java-11-openjdk less procps python3
RUN ln -s $(which python3) /usr/bin/python
RUN tar -zxf $PRESTO_PKG
RUN mv ./presto-server-$PRESTO_VERSION $PRESTO_HOME
RUN rm -rf $PRESTO_PKG ./presto-server-$PRESTO_VERSION
RUN chmod +x /opt/presto-cli
RUN ln -s /opt/presto-cli /usr/local/bin/
RUN mv /etc/yum/protected.d/systemd.conf /etc/yum/protected.d/systemd.conf.bak || true
RUN dnf clean all
RUN mkdir -p $PRESTO_HOME/etc $PRESTO_HOME/etc/catalog /var/lib/presto/data /usr/lib/presto/utils
RUN curl -fLo /usr/lib/presto/utils/jmx_prometheus_javaagent-$JMX_PROMETHEUS_JAVAAGENT_VERSION.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/$JMX_PROMETHEUS_JAVAAGENT_VERSION/jmx_prometheus_javaagent-$JMX_PROMETHEUS_JAVAAGENT_VERSION.jar
RUN ln -s /usr/lib/presto/utils/jmx_prometheus_javaagent-$JMX_PROMETHEUS_JAVAAGENT_VERSION.jar /usr/lib/presto/utils/jmx_prometheus_javaagent.jar

COPY etc/config.properties.example $PRESTO_HOME/etc/config.properties
COPY etc/jvm.config.example $PRESTO_HOME/etc/jvm.config
COPY etc/node.properties $PRESTO_HOME/etc/node.properties
COPY etc/catalog $PRESTO_HOME/etc/catalog
COPY entrypoint.sh /opt
RUN df


EXPOSE 8080

ENTRYPOINT ["/opt/entrypoint.sh"]
