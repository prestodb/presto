package com.facebook.presto.hdfs;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;
import static java.util.Objects.requireNonNull;

/**
 * presto-hdfs
 *
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSConnectorFactory
implements ConnectorFactory {
    private final String name = "hdfs-connector";
    private final ClassLoader classLoader;

    public HDFSConnectorFactory(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new HDFSHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context) {
        requireNonNull(config, "config is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new HDFSModule(connectorId),
                    new MetaServerModule()
            );

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(HDFSConnector.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
