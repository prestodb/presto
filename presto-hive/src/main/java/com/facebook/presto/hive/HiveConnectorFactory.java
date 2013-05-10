package com.facebook.presto.hive;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorHandleResolver;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorMetadata;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorRecordSetProvider;
import com.facebook.presto.spi.classloader.ClassLoaderSafeConnectorSplitManager;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableClassToInstanceMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeModule;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class
        HiveConnectorFactory
        implements ConnectorFactory
{
    private final Map<String, String> optionalConfig;
    private final ClassLoader classLoader;
    private final SlowDatanodeSystemTable slowDatanodeSystemTable;

    public HiveConnectorFactory(Map<String, String> optionalConfig)
    {
        this(optionalConfig, HiveConnectorFactory.class.getClassLoader(), null);
    }

    public HiveConnectorFactory(Map<String, String> optionalConfig, ClassLoader classLoader, SlowDatanodeSystemTable slowDatanodeSystemTable)
    {
        this.optionalConfig = checkNotNull(optionalConfig, "optionalConfig is null");
        this.classLoader = checkNotNull(classLoader, "classLoader is null");
        this.slowDatanodeSystemTable = slowDatanodeSystemTable;
    }

    @Override
    public String getName()
    {
        return "hive";
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> config)
    {
        checkNotNull(config, "config is null");

        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new NodeModule(),
                    new DiscoveryModule(),
                    new MBeanModule(),
                    new JsonModule(),
                    new HiveClientModule(connectorId),
                    new Module()
                    {
                        @Override
                        public void configure(Binder binder)
                        {
                            MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
                            binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(platformMBeanServer));
                        }
                    }
            );

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            HiveClient hiveClient = injector.getInstance(HiveClient.class);

            ImmutableClassToInstanceMap.Builder<Object> builder = ImmutableClassToInstanceMap.builder();
            builder.put(ConnectorMetadata.class, new ClassLoaderSafeConnectorMetadata(hiveClient, classLoader));
            builder.put(ConnectorSplitManager.class, new ClassLoaderSafeConnectorSplitManager(hiveClient, classLoader));
            builder.put(ConnectorRecordSetProvider.class, new ClassLoaderSafeConnectorRecordSetProvider(hiveClient, classLoader));
            builder.put(ConnectorHandleResolver.class, new ClassLoaderSafeConnectorHandleResolver(hiveClient, classLoader));

            if (slowDatanodeSystemTable != null) {
                SlowDatanodeSwitcher slowDatanodeSwitcher = injector.getInstance(SlowDatanodeSwitcher.class);
                slowDatanodeSystemTable.addConnectorSlowDatanodeSwitcher(connectorId, slowDatanodeSwitcher);
            }
            return new HiveConnector(builder.build());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
