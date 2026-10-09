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
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.resolver.ArtifactResolver;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.StaticCatalogStore;
import com.facebook.presto.metadata.StaticCatalogStoreConfig;
import com.facebook.presto.server.PluginInstaller;
import com.facebook.presto.server.PluginManagerConfig;
import com.facebook.presto.server.PluginManagerUtil;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.CoordinatorPlugin;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import jakarta.annotation.PreDestroy;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.server.PluginManagerUtil.SPI_PACKAGES;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FlightShimPluginManager
        implements PluginInstaller
{
    private static final Logger log = Logger.get(FlightShimPluginManager.class);
    private static final String SERVICES_FILE = "META-INF/services/" + Plugin.class.getName();
    private final ConnectorManager connectorManager;
    private final CatalogManager catalogManager;
    private final File installedPluginsDir;
    private final List<String> plugins;
    private final ArtifactResolver resolver;
    private final AtomicBoolean pluginsLoading = new AtomicBoolean();
    private final AtomicBoolean pluginsLoaded = new AtomicBoolean();
    private final StaticCatalogStore staticCatalogStore;
    private final Supplier<List<PluginManagerUtil.PluginClassLoaderHandle>> cachedPluginClassLoaders;
    private final HandleResolver handleResolver;
    private final JsonCodec<ConnectorSplit> splitCodec;
    private final JsonCodec<ColumnHandle> columnHandleCodec;
    private final JsonCodec<ConnectorTableHandle> tableHandleCodec;
    private final JsonCodec<ConnectorTableLayoutHandle> tableLayoutHandleCodec;
    private final JsonCodec<ConnectorTransactionHandle> transactionHandleCodec;

    @Inject
    public FlightShimPluginManager(
            ConnectorManager connectorManager,
            CatalogManager catalogManager,
            StaticCatalogStore staticCatalogStore,
            PluginManagerConfig pluginManagerConfig,
            StaticCatalogStoreConfig catalogStoreConfig,
            HandleResolver handleResolver,
            JsonCodec<ConnectorSplit> splitCodec,
            JsonCodec<ColumnHandle> columnHandleCodec,
            JsonCodec<ConnectorTableHandle> tableHandleCodec,
            JsonCodec<ConnectorTableLayoutHandle> tableLayoutHandleCodec,
            JsonCodec<ConnectorTransactionHandle> transactionHandleCodec)
    {
        this.connectorManager = requireNonNull(connectorManager, "connectorManager is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.staticCatalogStore = requireNonNull(staticCatalogStore, "staticCatalogStore is null");
        requireNonNull(pluginManagerConfig, "pluginManagerConfig is null");
        requireNonNull(catalogStoreConfig, "catalogStoreConfig is null");
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        this.splitCodec = requireNonNull(splitCodec, "splitCodec is null");
        this.columnHandleCodec = requireNonNull(columnHandleCodec, "columnHandleCodec is null");
        this.tableHandleCodec = requireNonNull(tableHandleCodec, "tableHandleCodec is null");
        this.tableLayoutHandleCodec = requireNonNull(tableLayoutHandleCodec, "tableLayoutHandleCodec is null");
        this.transactionHandleCodec = requireNonNull(transactionHandleCodec, "transactionHandleCodec is null");
        this.installedPluginsDir = pluginManagerConfig.getInstalledPluginsDir();
        if (pluginManagerConfig.getPlugins() == null) {
            this.plugins = ImmutableList.of();
        }
        else {
            this.plugins = ImmutableList.copyOf(pluginManagerConfig.getPlugins());
        }
        this.resolver = new ArtifactResolver(pluginManagerConfig.getMavenLocalRepository(), pluginManagerConfig.getMavenRemoteRepository());
        this.cachedPluginClassLoaders = Suppliers.memoize(this::createPluginClassLoaders);
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
                null,
                this,
                cachedPluginClassLoaders.get());
    }

    public void loadCatalogs(Map<String, Map<String, String>> additionalCatalogs)
            throws Exception
    {
        staticCatalogStore.loadCatalogs(additionalCatalogs);
    }

    public String getConnectorName(String catalogName)
    {
        Catalog catalog = catalogManager.getCatalog(catalogName).orElseThrow(() -> new PrestoException(NOT_FOUND, "Federation catalog does not exist: " + catalogName));
        return catalog.getCatalogContext().getConnectorName();
    }

    public ConnectorSplit decodeSplit(String connectorName, byte[] splitBytes)
    {
        return decodeHandle(splitCodec, splitBytes, handleResolver.getSplitClass(connectorName), connectorName);
    }

    public ConnectorTableHandle decodeTableHandle(String connectorName, byte[] tableHandleBytes)
    {
        return decodeHandle(tableHandleCodec, tableHandleBytes, handleResolver.getTableHandleClass(connectorName), connectorName);
    }

    public ConnectorTableLayoutHandle decodeTableLayoutHandle(String connectorName, byte[] tableLayoutHandleBytes)
    {
        return decodeHandle(tableLayoutHandleCodec, tableLayoutHandleBytes, handleResolver.getTableLayoutHandleClass(connectorName), connectorName);
    }

    public ColumnHandle decodeColumnHandle(String connectorName, byte[] columnHandleBytes)
    {
        return decodeHandle(columnHandleCodec, columnHandleBytes, handleResolver.getColumnHandleClass(connectorName), connectorName);
    }

    public ConnectorTransactionHandle decodeTransactionHandle(String connectorName, byte[] transactionHandleBytes)
    {
        return decodeHandle(transactionHandleCodec, transactionHandleBytes, handleResolver.getTransactionHandleClass(connectorName), connectorName);
    }

    @Override
    public void installPlugin(Plugin plugin)
    {
        for (ConnectorFactory factory : plugin.getConnectorFactories()) {
            log.info("Registering connector %s", factory.getName());
            connectorManager.addConnectorFactory(factory);
        }
    }

    @Override
    public void installCoordinatorPlugin(CoordinatorPlugin plugin) {}

    private List<PluginManagerUtil.PluginClassLoaderHandle> createPluginClassLoaders()
    {
        try {
            return PluginManagerUtil.buildClassLoaders(
                    installedPluginsDir,
                    plugins,
                    resolver,
                    SPI_PACKAGES,
                    null,
                    SERVICES_FILE,
                    getClass().getClassLoader());
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to build plugin classloaders", e);
        }
    }

    // The handle's @type selects its class, so reject handles of a connector other than the requested catalog's
    private static <T> T decodeHandle(JsonCodec<T> codec, byte[] bytes, Class<? extends T> expectedClass, String connectorName)
    {
        T handle = codec.fromJson(bytes);
        if (!expectedClass.isInstance(handle)) {
            throw new IllegalArgumentException(format("Handle of type %s does not belong to connector %s", handle.getClass().getName(), connectorName));
        }
        return handle;
    }
}
