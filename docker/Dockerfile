FROM quay.io/centos/centos:stream8

ARG PRESTO_VERSION
ARG PRESTO_PKG=presto-server-$PRESTO_VERSION.tar.gz
ARG PRESTO_CLI_JAR=presto-cli-$PRESTO_VERSION-executable.jar

ENV PRESTO_HOME="/opt/presto-server"

COPY $PRESTO_PKG .
COPY $PRESTO_CLI_JAR /opt/presto-cli

RUN dnf install -y java-11-openjdk less procps python3 \
    && ln -s $(which python3) /usr/bin/python \
    # Download Presto and move \
    && tar -zxf $PRESTO_PKG \
    && mv ./presto-server-$PRESTO_VERSION $PRESTO_HOME \
    && rm -rf $PRESTO_PKG \
    && rm -rf ./presto-server-$PRESTO_VERSION \
    && chmod +x /opt/presto-cli \
    && ln -s /opt/presto-cli /usr/local/bin/ \
    # clean cache jobs
    && mv /etc/yum/protected.d/systemd.conf /etc/yum/protected.d/systemd.conf.bak \
    && dnf clean all \
    #  mkdir for config
    && mkdir -p $PRESTO_HOME/etc \
    && mkdir -p $PRESTO_HOME/etc/catalog \
    && mkdir -p /var/lib/presto/data \
        && mkdir -p /usr/lib/presto/utils \
        # Download jmx prometheus exporter \
        && curl -o /usr/lib/presto/utils/jmx_prometheus_javaagent-0.16.1.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.16.1/jmx_prometheus_javaagent-0.16.1.jar

COPY etc $PRESTO_HOME/etc
COPY entrypoint.sh /opt

ENTRYPOINT ["/opt/entrypoint.sh"]
