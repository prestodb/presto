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
package com.facebook.presto.flightshim;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.StaticCatalogStore;
import com.facebook.presto.metadata.StaticCatalogStoreConfig;
import com.facebook.presto.server.PluginInstaller;
import com.facebook.presto.server.PluginManagerConfig;
import com.facebook.presto.server.PluginManagerUtil;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.CoordinatorPlugin;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.type.TypeDeserializer;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.resolver.ArtifactResolver;
import jakarta.annotation.PreDestroy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.server.PluginManagerUtil.SPI_PACKAGES;
import static java.util.Objects.requireNonNull;

public class FlightShimPluginManager
        implements PluginInstaller
{
    private static final Logger log = Logger.get(FlightShimPluginManager.class);
    private static final String SERVICES_FILE = "META-INF/services/" + Plugin.class.getName();
    private final ConnectorManager connectorManager;
    private final Map<String, ConnectorCodecs> connectorCodecMap = new ConcurrentHashMap<>();
    private final File installedPluginsDir;
    private final List<String> plugins;
    private final ArtifactResolver resolver;
    private final AtomicBoolean pluginsLoading = new AtomicBoolean();
    private final AtomicBoolean pluginsLoaded = new AtomicBoolean();
    private final StaticCatalogStore staticCatalogStore;
    private final TypeDeserializer typeDeserializer;
    private final BlockEncodingManager blockEncodingManager;

    @Inject
    public FlightShimPluginManager(
            ConnectorManager connectorManager,
            StaticCatalogStore staticCatalogStore,
            PluginManagerConfig pluginManagerConfig,
            StaticCatalogStoreConfig catalogStoreConfig,
            TypeDeserializer typeDeserializer,
            BlockEncodingManager blockEncodingManager)
    {
        this.connectorManager = requireNonNull(connectorManager, "connectorManager is null");
        this.staticCatalogStore = requireNonNull(staticCatalogStore, "staticCatalogStore is null");
        requireNonNull(pluginManagerConfig, "pluginManagerConfig is null");
        requireNonNull(catalogStoreConfig, "catalogStoreConfig is null");
        this.typeDeserializer = requireNonNull(typeDeserializer, "typeDeserializer is null");
        this.blockEncodingManager = requireNonNull(blockEncodingManager, "blockEncodingManager is null");
        this.installedPluginsDir = pluginManagerConfig.getInstalledPluginsDir();
        if (pluginManagerConfig.getPlugins() == null) {
            this.plugins = ImmutableList.of();
        }
        else {
            this.plugins = ImmutableList.copyOf(pluginManagerConfig.getPlugins());
        }
        this.resolver = new ArtifactResolver(pluginManagerConfig.getMavenLocalRepository(), pluginManagerConfig.getMavenRemoteRepository());
    }

    @PreDestroy
    public synchronized void stop()
    {
        connectorManager.stop();
    }

    public void loadPlugins()
            throws Exception
    {
        PluginManagerUtil.loadPlugins(
                pluginsLoading,
                pluginsLoaded,
                installedPluginsDir,
                plugins,
                null,
                resolver,
                SPI_PACKAGES,
                null,
                SERVICES_FILE,
                this,
                getClass().getClassLoader());
    }

    public void loadCatalogs(Map<String, Map<String, String>> additionalCatalogs)
            throws Exception
    {
        staticCatalogStore.loadCatalogs(additionalCatalogs);
    }

    public ConnectorCodecs getConnectorCodecs(String connectorId)
    {
        return connectorCodecMap.get(connectorId);
    }

    @Override
    public void installPlugin(Plugin plugin)
    {
        for (ConnectorFactory factory : plugin.getConnectorFactories()) {
            log.info("Registering connector %s", factory.getName());
            connectorManager.addConnectorFactory(factory);
            connectorCodecMap.computeIfAbsent(factory.getName(), name -> {
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
                    ConnectorCodecs holder = new ConnectorCodecs(factory.getHandleResolver(), typeDeserializer, blockEncodingManager);
                    log.debug("Finished loading connector: %s", name);
                    return holder;
                }
            });
        }
    }

    @Override
    public void installCoordinatorPlugin(CoordinatorPlugin plugin) {}

    public static class ConnectorCodecs
    {
        private final JsonCodec<? extends ConnectorSplit> codecSplit;
        private final JsonCodec<? extends ColumnHandle> codecColumnHandle;
        private final JsonCodec<? extends ConnectorTableHandle> codecTableHandle;
        private final JsonCodec<? extends ConnectorTableLayoutHandle> codecTableLayoutHandle;
        private final JsonCodec<? extends ConnectorTransactionHandle> codecTransactionHandle;

        ConnectorCodecs(ConnectorHandleResolver resolver, TypeDeserializer typeDeserializer, BlockEncodingManager blockEncodingManager)
        {
            JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
            JsonDeserializer<?> columnDeserializer = new JsonDeserializer<ColumnHandle>()
            {
                @Override
                public ColumnHandle deserialize(JsonParser p, DeserializationContext ctxt)
                        throws IOException
                {
                    return p.readValueAs(resolver.getColumnHandleClass());
                }
            };
            BlockJsonSerde.Deserializer blockDeserializer = new BlockJsonSerde.Deserializer(blockEncodingManager);
            provider.setJsonDeserializers(ImmutableMap.of(
                    Type.class, typeDeserializer,
                    ColumnHandle.class, columnDeserializer,
                    Block.class, blockDeserializer));
            JsonCodecFactory jsonCodecFactory = new JsonCodecFactory(provider);

            this.codecSplit = jsonCodecFactory.jsonCodec(resolver.getSplitClass());
            this.codecColumnHandle = jsonCodecFactory.jsonCodec(resolver.getColumnHandleClass());
            this.codecTableHandle = jsonCodecFactory.jsonCodec(resolver.getTableHandleClass());
            this.codecTableLayoutHandle = jsonCodecFactory.jsonCodec(resolver.getTableLayoutHandleClass());
            this.codecTransactionHandle = jsonCodecFactory.jsonCodec(resolver.getTransactionHandleClass());
        }

        JsonCodec<? extends ConnectorSplit> getCodecSplit()
        {
            return codecSplit;
        }

        JsonCodec<? extends ColumnHandle> getCodecColumnHandle()
        {
            return codecColumnHandle;
        }

        JsonCodec<? extends ConnectorTableHandle> getCodecTableHandle()
        {
            return codecTableHandle;
        }

        JsonCodec<? extends ConnectorTableLayoutHandle> getCodecTableLayoutHandle()
        {
            return codecTableLayoutHandle;
        }

        JsonCodec<? extends ConnectorTransactionHandle> getCodecTransactionHandle()
        {
            return codecTransactionHandle;
        }
    }
}
