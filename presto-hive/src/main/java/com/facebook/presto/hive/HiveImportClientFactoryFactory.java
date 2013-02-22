/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.hive;

import com.facebook.presto.spi.ImportClientFactory;
import com.facebook.presto.spi.ImportClientFactoryFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
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

public class HiveImportClientFactoryFactory
        implements ImportClientFactoryFactory
{
    public String getConfigName() {
        return "hive";
    }

    @Override
    public ImportClientFactory createImportClientFactory(Map<String, String> requiredConfig, Map<String, String> optionalConfig)
    {
        Preconditions.checkNotNull(requiredConfig, "requiredConfig is null");
        Preconditions.checkNotNull(optionalConfig, "optionalConfig is null");

        try {
            Bootstrap app = new Bootstrap(
                    new NodeModule(),
                    new DiscoveryModule(),
                    new MBeanModule(),
                    new JsonModule(),
                    new HiveClientModule(),
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
                    .setRequiredConfigurationProperties(requiredConfig)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(HiveImportClientFactory.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

}
