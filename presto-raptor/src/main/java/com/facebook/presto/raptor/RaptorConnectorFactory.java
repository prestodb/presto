/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.backup.BackupModule;
import com.facebook.presto.raptor.storage.StorageModule;
import com.facebook.presto.raptor.util.CurrentNodeId;
import com.facebook.presto.raptor.util.RebindSafeMBeanServer;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

public class RaptorConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final Module metadataModule;
    private final Map<String, Module> backupProviders;
    private final Map<String, String> optionalConfig;
    private final NodeManager nodeManager;
    private final BlockEncodingSerde blockEncodingSerde;
    private final TypeManager typeManager;
    private final PageSorter pageSorter;

    public RaptorConnectorFactory(
            String name,
            Module metadataModule,
            Map<String, Module> backupProviders,
            Map<String, String> optionalConfig,
            NodeManager nodeManager,
            PageSorter pageSorter,
            BlockEncodingSerde blockEncodingSerde,
            TypeManager typeManager)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.metadataModule = checkNotNull(metadataModule, "metadataModule is null");
        this.backupProviders = ImmutableMap.copyOf(checkNotNull(backupProviders, "backupProviders is null"));
        this.optionalConfig = checkNotNull(optionalConfig, "optionalConfig is null");
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.pageSorter = checkNotNull(pageSorter, "pageSorter is null");
        this.blockEncodingSerde = checkNotNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config)
    {
        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new MBeanModule(),
                    binder -> {
                        CurrentNodeId currentNodeId = new CurrentNodeId(nodeManager.getCurrentNode().getNodeIdentifier());
                        MBeanServer mbeanServer = new RebindSafeMBeanServer(getPlatformMBeanServer());

                        binder.bind(MBeanServer.class).toInstance(mbeanServer);
                        binder.bind(CurrentNodeId.class).toInstance(currentNodeId);
                        binder.bind(NodeManager.class).toInstance(nodeManager);
                        binder.bind(PageSorter.class).toInstance(pageSorter);
                        binder.bind(BlockEncodingSerde.class).toInstance(blockEncodingSerde);
                        binder.bind(TypeManager.class).toInstance(typeManager);
                    },
                    metadataModule,
                    new BackupModule(backupProviders),
                    new StorageModule(connectorId),
                    new RaptorModule(connectorId));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(RaptorConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
