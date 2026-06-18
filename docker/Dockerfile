FROM quay.io/centos/centos:stream9

ARG PRESTO_VERSION
ARG PRESTO_PKG=presto-server-$PRESTO_VERSION.tar.gz
ARG PRESTO_CLI_JAR=presto-cli-$PRESTO_VERSION-executable.jar
ARG PRESTO_FUNCTION_SERVER_JAR=presto-function-server-$PRESTO_VERSION-executable.jar
ARG JMX_PROMETHEUS_JAVAAGENT_VERSION=0.20.0

ENV PRESTO_HOME="/opt/presto-server"

RUN --mount=type=cache,target=/var/cache/dnf,sharing=locked \
    dnf install -y java-17-openjdk less procps python3 \
    && ln -s $(which python3) /usr/bin/python \
    # clean cache jobs
    && mv /etc/yum/protected.d/systemd.conf /etc/yum/protected.d/systemd.conf.bak

COPY --chmod=755 $PRESTO_CLI_JAR              /opt/presto-cli
COPY --chmod=755 $PRESTO_FUNCTION_SERVER_JAR  /opt/presto-remote-function-server

RUN --mount=type=bind,source=$PRESTO_PKG,target=/$PRESTO_PKG \
    # Download Presto and move \
    mkdir -p "$PRESTO_HOME" && \
    tar --strip-components=1 -C "$PRESTO_HOME" -zxf /$PRESTO_PKG \
    && ln -s /opt/presto-cli /usr/local/bin/ \
    #  mkdir for config
    && mkdir -p $PRESTO_HOME/etc/catalog \
    && mkdir -p /var/lib/presto/data \
        && mkdir -p /usr/lib/presto/utils \
        && curl -o /usr/lib/presto/utils/jmx_prometheus_javaagent-$JMX_PROMETHEUS_JAVAAGENT_VERSION.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/$JMX_PROMETHEUS_JAVAAGENT_VERSION/jmx_prometheus_javaagent-$JMX_PROMETHEUS_JAVAAGENT_VERSION.jar \
        && ln -s /usr/lib/presto/utils/jmx_prometheus_javaagent-$JMX_PROMETHEUS_JAVAAGENT_VERSION.jar /usr/lib/presto/utils/jmx_prometheus_javaagent.jar

COPY etc/config.properties.example $PRESTO_HOME/etc/config.properties
COPY etc/jvm.config.example $PRESTO_HOME/etc/jvm.config
COPY etc/node.properties $PRESTO_HOME/etc/node.properties
COPY etc/catalog $PRESTO_HOME/etc/catalog
COPY entrypoint.sh /opt

EXPOSE 8080

VOLUME ["/var/lib/presto/data"]

ENTRYPOINT ["/opt/entrypoint.sh"]
