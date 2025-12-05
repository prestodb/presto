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
import com.facebook.presto.GroupByHashPageIndexerFactory;
import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.connector.ConnectorContextInstance;
import com.facebook.presto.cost.ConnectorFilterStatsCalculatorService;
import com.facebook.presto.cost.FilterStatsCalculator;
import com.facebook.presto.cost.ScalarStatsCalculator;
import com.facebook.presto.cost.StatsNormalizer;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.StaticCatalogStoreConfig;
import com.facebook.presto.nodeManager.PluginNodeManager;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.server.PluginInstaller;
import com.facebook.presto.server.PluginManagerConfig;
import com.facebook.presto.server.PluginManagerUtil;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.CoordinatorPlugin;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.procedure.ProcedureRegistry;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.ExpressionOptimizerProvider;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.RowExpressionPredicateCompiler;
import com.facebook.presto.sql.planner.planPrinter.RowExpressionFormatter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.type.TypeDeserializer;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.inject.Inject;
import io.airlift.resolver.ArtifactResolver;
import jakarta.annotation.PreDestroy;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.server.PluginManagerUtil.SPI_PACKAGES;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.api.client.util.Preconditions.checkState;
import static com.google.common.base.MoreObjects.firstNonNull;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FlightShimPluginManager
{
    private static final Logger log = Logger.get(FlightShimPluginManager.class);
    private static final String SERVICES_FILE = "META-INF/services/" + Plugin.class.getName();
    private final Map<String, ConnectorFactory> connectorFactories = new ConcurrentHashMap<>();
    private final Map<String, ConnectorHolder> connectors = new ConcurrentHashMap<>();
    private final File installedPluginsDir;
    private final List<String> plugins;
    private final ArtifactResolver resolver;
    private final AtomicBoolean pluginsLoading = new AtomicBoolean();
    private final AtomicBoolean pluginsLoaded = new AtomicBoolean();
    private final PluginInstaller pluginInstaller;
    private final File catalogConfigurationDir;
    private final Set<String> disabledCatalogs;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();
    private final Map<String, CatalogPropertiesHolder> catalogPropertiesMap = new ConcurrentHashMap<>();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final Metadata metadata;
    private final TypeDeserializer typeDeserializer;
    private final BlockEncodingManager blockEncodingManager;
    private final ProcedureRegistry procedureRegistry;

    @Inject
    public FlightShimPluginManager(
            PluginManagerConfig pluginManagerConfig,
            StaticCatalogStoreConfig catalogStoreConfig,
            Metadata metadata,
            TypeDeserializer typeDeserializer,
            BlockEncodingManager blockEncodingManager,
            ProcedureRegistry procedureRegistry)
    {
        requireNonNull(pluginManagerConfig, "pluginManagerConfig is null");
        requireNonNull(catalogStoreConfig, "catalogStoreConfig is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
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
        this.pluginInstaller = new FlightServerPluginInstaller();
        this.catalogConfigurationDir = catalogStoreConfig.getCatalogConfigurationDir();
        this.disabledCatalogs = ImmutableSet.copyOf(firstNonNull(catalogStoreConfig.getDisabledCatalogs(), ImmutableList.of()));
        this.procedureRegistry = requireNonNull(procedureRegistry, "procedureRegistry is null");
    }

    @PreDestroy
    public synchronized void stop()
    {
        if (stopped.getAndSet(true)) {
            return;
        }

        for (Map.Entry<String, ConnectorHolder> entry : connectors.entrySet()) {
            Connector connector = entry.getValue().getConnector();
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(connector.getClass().getClassLoader())) {
                connector.shutdown();
            }
            catch (Throwable t) {
                log.error(t, "Error shutting down connector: %s", entry.getKey());
            }
        }
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
                pluginInstaller,
                getClass().getClassLoader());
    }

    public void loadCatalogs()
            throws Exception
    {
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }

        for (File file : listFiles(catalogConfigurationDir)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                loadCatalog(file);
            }
        }

        catalogsLoaded.set(true);
    }

    private void loadCatalog(File file)
            throws Exception
    {
        String catalogName = Files.getNameWithoutExtension(file.getName());

        log.info("-- Loading catalog properties %s --", file);
        Map<String, String> properties = loadProperties(file);
        checkState(properties.containsKey("connector.name"), "Catalog configuration %s does not contain connector.name", file.getAbsoluteFile());

        loadCatalog(catalogName, properties);
    }

    private void loadCatalog(String catalogName, Map<String, String> properties)
    {
        if (disabledCatalogs.contains(catalogName)) {
            log.info("Skipping disabled catalog %s", catalogName);
            return;
        }

        log.info("-- Loading catalog %s --", catalogName);

        String connectorName = null;
        ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equals("connector.name")) {
                connectorName = entry.getValue();
            }
            else {
                connectorProperties.put(entry.getKey(), entry.getValue());
            }
        }

        checkState(connectorName != null, "Configuration for catalog %s does not contain connector.name", catalogName);

        catalogPropertiesMap.put(catalogName, new CatalogPropertiesHolder(connectorName, connectorProperties.build()));

        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }

    @VisibleForTesting
    void setCatalogProperties(String catalogName, String connectorName, Map<String, String> properties)
    {
        catalogPropertiesMap.put(catalogName, new CatalogPropertiesHolder(connectorName, ImmutableMap.copyOf(properties)));
    }

    private static List<File> listFiles(File installedPluginsDir)
    {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private void registerPlugin(Plugin plugin)
    {
        for (ConnectorFactory factory : plugin.getConnectorFactories()) {
            log.info("Registering connector %s", factory.getName());
            connectorFactories.put(factory.getName(), factory);
        }
    }

    public ConnectorHolder getConnector(String connectorId)
    {
        log.debug("FlightShimPluginManager getting connector: %s", connectorId);
        CatalogPropertiesHolder catalogPropertiesHolder = catalogPropertiesMap.get(connectorId);
        if (catalogPropertiesHolder == null) {
            throw new IllegalArgumentException("Properties not loaded for " + connectorId);
        }
        final ImmutableMap<String, String> config = catalogPropertiesHolder.getCatalogProperties();

        // Create connector instances from factories as needed
        return connectors.computeIfAbsent(catalogPropertiesHolder.getConnectorName(), name -> {
            log.debug("Loading connector: %s", connectorId);
            ConnectorFactory factory = connectorFactories.get(name);
            requireNonNull(factory, format("No connector factory for %s", connectorId));

            final RowExpressionDomainTranslator domainTranslator = new RowExpressionDomainTranslator(metadata);
            final PredicateCompiler predicateCompiler = new RowExpressionPredicateCompiler(metadata);
            final DeterminismEvaluator determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager());
            final RowExpressionService rowExpressionService = new RowExpressionService()
            {
                @Override
                public DomainTranslator getDomainTranslator()
                {
                    return domainTranslator;
                }

                @Override
                public ExpressionOptimizer getExpressionOptimizer(ConnectorSession session)
                {
                    return new RowExpressionOptimizer(metadata);
                }

                @Override
                public PredicateCompiler getPredicateCompiler()
                {
                    return predicateCompiler;
                }

                @Override
                public DeterminismEvaluator getDeterminismEvaluator()
                {
                    return determinismEvaluator;
                }

                @Override
                public String formatRowExpression(ConnectorSession session, RowExpression expression)
                {
                    return new RowExpressionFormatter(metadata.getFunctionAndTypeManager()).formatRowExpression(session, expression);
                }
            };

            final ExpressionOptimizerProvider expressionOptimizerProvider = (ConnectorSession session) -> new RowExpressionOptimizer(metadata);

            ConnectorContext context = new ConnectorContextInstance(
                    new PluginNodeManager(new InMemoryNodeManager(), "flightconnector"),
                    metadata.getFunctionAndTypeManager(),
                    procedureRegistry,
                    metadata.getFunctionAndTypeManager(),
                    new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver()),
                    new PagesIndexPageSorter(new PagesIndex.TestingFactory(false)),
                    new GroupByHashPageIndexerFactory(new JoinCompiler(metadata)),
                    rowExpressionService,
                    new ConnectorFilterStatsCalculatorService(new FilterStatsCalculator(metadata, new ScalarStatsCalculator(metadata, expressionOptimizerProvider), new StatsNormalizer())),
                    blockEncodingManager,
                    () -> false);

            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
                return new ConnectorHolder(factory.create(name, config, context), factory.getHandleResolver(), typeDeserializer, blockEncodingManager);
            }
            finally {
                log.debug("Finished loading connector: %s", connectorId);
            }
        });
    }

    private class FlightServerPluginInstaller
            implements PluginInstaller
    {
        @Override
        public void installPlugin(Plugin plugin)
        {
            registerPlugin(plugin);
        }

        @Override
        public void installCoordinatorPlugin(CoordinatorPlugin plugin) {}
    }

    private static class CatalogPropertiesHolder
    {
        private final String connectorName;
        private final ImmutableMap<String, String> catalogProperties;

        CatalogPropertiesHolder(String connectorName, ImmutableMap<String, String> catalogProperties)
        {
            this.connectorName = connectorName;
            this.catalogProperties = catalogProperties;
        }

        String getConnectorName()
        {
            return connectorName;
        }

        ImmutableMap<String, String> getCatalogProperties()
        {
            return catalogProperties;
        }
    }

    public static class ConnectorHolder
    {
        private final Connector connector;
        private final JsonCodec<? extends ConnectorSplit> codecSplit;
        private final JsonCodec<? extends ColumnHandle> codecColumnHandle;
        private final Method getColumnMetadataMethod;

        ConnectorHolder(Connector connector, ConnectorHandleResolver resolver, TypeDeserializer typeDeserializer, BlockEncodingManager blockEncodingManager)
        {
            this.connector = connector;

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
            this.getColumnMetadataMethod = reflectGetColumnMetadata(resolver);
        }

        Connector getConnector()
        {
            return connector;
        }

        JsonCodec<? extends ConnectorSplit> getCodecSplit()
        {
            return codecSplit;
        }

        JsonCodec<? extends ColumnHandle> getCodecColumnHandle()
        {
            return codecColumnHandle;
        }

        ColumnMetadata getColumnMetadata(ColumnHandle handle)
        {
            try {
                return (ColumnMetadata) getColumnMetadataMethod.invoke(handle);
            }
            catch (InvocationTargetException | IllegalAccessException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unable to invoke method for getColumnMetadata", e);
            }
        }

        private static Method reflectGetColumnMetadata(ConnectorHandleResolver resolver)
        {
            try {
                return resolver.getColumnHandleClass().getMethod("getColumnMetadata");
            }
            catch (NoSuchMethodException e) {
                try {
                    return resolver.getColumnHandleClass().getMethod("toColumnMetadata");
                }
                catch (NoSuchMethodException e2) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unable to get column metadata from ColumnHandle", e);
                }
            }
        }
    }
}
