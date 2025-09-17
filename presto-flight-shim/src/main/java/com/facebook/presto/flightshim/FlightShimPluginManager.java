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
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.cost.ConnectorFilterStatsCalculatorService;
import com.facebook.presto.cost.FilterStatsCalculator;
import com.facebook.presto.cost.ScalarStatsCalculator;
import com.facebook.presto.cost.StatsNormalizer;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
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
import com.facebook.presto.spi.ConnectorSystemConfig;
import com.facebook.presto.spi.CoordinatorPlugin;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
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
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.inject.Inject;
import io.airlift.resolver.ArtifactResolver;
import jakarta.annotation.PreDestroy;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.server.PluginManagerUtil.SPI_PACKAGES;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.api.client.util.Preconditions.checkState;
import static com.google.common.base.MoreObjects.firstNonNull;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
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

    @Inject
    public FlightShimPluginManager(PluginManagerConfig pluginManagerConfig, StaticCatalogStoreConfig catalogStoreConfig)
    {
        requireNonNull(pluginManagerConfig, "pluginManagerConfig is null");
        requireNonNull(catalogStoreConfig, "pluginManagerConfig is null");
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

    public ConnectorHolder getConnector(String connectorId) {
        CatalogPropertiesHolder catalogPropertiesHolder = catalogPropertiesMap.get(connectorId);
        if (catalogPropertiesHolder == null) {
            // TODO PrestoEx
            throw new RuntimeException(format("Properties not loaded for: %s", connectorId));
        }
        final ImmutableMap<String, String> config = catalogPropertiesHolder.getCatalogProperties();

        // Create connector instances from factories as needed
        return connectors.computeIfAbsent(catalogPropertiesHolder.getConnectorName(), name -> {
            ConnectorFactory factory = connectorFactories.get(name);

            /*ConnectorContext context = new ConnectorContextInstance(
                new ConnectorAwareNodeManager(nodeManager, nodeInfo.getEnvironment(), connectorId),
                typeManager,
                metadataManager.getFunctionAndTypeManager(),
                new FunctionResolution(metadataManager.getFunctionAndTypeManager().getFunctionAndTypeResolver()),
                pageSorter,
                pageIndexerFactory,
                new ConnectorRowExpressionService(
                        domainTranslator,
                        expressionOptimizerManager,
                        predicateCompiler,
                        determinismEvaluator,
                        new RowExpressionFormatter(metadataManager.getFunctionAndTypeManager())),
                new ConnectorFilterStatsCalculatorService(filterStatsCalculator),
                blockEncodingSerde,
                connectorSystemConfig);*/

            // TODO should context load a typemanager etc??
            FlightShimConnectorContext context = new FlightShimConnectorContext();

            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
                return new ConnectorHolder(factory.create(name, config, context), factory.getHandleResolver());
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

    private static class FlightShimConnectorContext
            implements ConnectorContext
    {
        private final NodeManager nodeManager = new PluginNodeManager(new InMemoryNodeManager(), "flightconnector");
        private final FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        private final StandardFunctionResolution functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        private final PageSorter pageSorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));
        private final PageIndexerFactory pageIndexerFactory = new GroupByHashPageIndexerFactory(new JoinCompiler(MetadataManager.createTestMetadataManager()));
        private final Metadata metadata = MetadataManager.createTestMetadataManager();
        private final DomainTranslator domainTranslator = new RowExpressionDomainTranslator(metadata);
        private final PredicateCompiler predicateCompiler = new RowExpressionPredicateCompiler(metadata);
        private final DeterminismEvaluator determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
        private final ExpressionOptimizerProvider expressionOptimizerProvider = (ConnectorSession session) -> new RowExpressionOptimizer(metadata);
        private final FilterStatsCalculatorService filterStatsCalculatorService = new ConnectorFilterStatsCalculatorService(new FilterStatsCalculator(metadata, new ScalarStatsCalculator(metadata, expressionOptimizerProvider), new StatsNormalizer()));
        private final BlockEncodingSerde blockEncodingSerde = new BlockEncodingManager();

        @Override
        public NodeManager getNodeManager()
        {
            return nodeManager;
        }

        @Override
        public TypeManager getTypeManager()
        {
            return functionAndTypeManager;
        }

        @Override
        public FunctionMetadataManager getFunctionMetadataManager()
        {
            return functionAndTypeManager;
        }

        @Override
        public StandardFunctionResolution getStandardFunctionResolution()
        {
            return functionResolution;
        }

        @Override
        public PageSorter getPageSorter()
        {
            return pageSorter;
        }

        @Override
        public PageIndexerFactory getPageIndexerFactory()
        {
            return pageIndexerFactory;
        }

        @Override
        public RowExpressionService getRowExpressionService()
        {
            return new RowExpressionService()
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
                    return new RowExpressionFormatter(functionAndTypeManager).formatRowExpression(session, expression);
                }
            };
        }

        @Override
        public FilterStatsCalculatorService getFilterStatsCalculatorService()
        {
            return filterStatsCalculatorService;
        }

        @Override
        public BlockEncodingSerde getBlockEncodingSerde()
        {
            return blockEncodingSerde;
        }

        @Override
        public ConnectorSystemConfig getConnectorSystemConfig()
        {
            return () -> false;
        }
    }

    static class ConnectorHolder
    {
        private final Connector connector;
        private final JsonCodec<? extends ConnectorSplit> codecSplit;
        private final JsonCodec<? extends ColumnHandle> codecColumnHandle;
        private final Method getColumnMetadataMethod;

        ConnectorHolder(Connector connector, ConnectorHandleResolver resolver)
        {
            this.connector = connector;
            this.codecSplit = JsonCodec.jsonCodec(resolver.getSplitClass());

            // TODO better way to get a TypeDeserializer?
            JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
            provider.setJsonDeserializers(ImmutableMap.of(Type.class, new FlightShimTypeDeserializer()));
            this.codecColumnHandle = new JsonCodecFactory(provider).jsonCodec(resolver.getColumnHandleClass());
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

        private static Method reflectGetColumnMetadata(ConnectorHandleResolver resolver) {
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

    public static final class FlightShimTypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final Map<String, Type> types = ImmutableMap.of(
                StandardTypes.BOOLEAN, BOOLEAN,
                StandardTypes.TINYINT, TINYINT,
                StandardTypes.SMALLINT, SMALLINT,
                StandardTypes.INTEGER, INTEGER,
                StandardTypes.BIGINT, BIGINT,
                StandardTypes.REAL, REAL,
                StandardTypes.DOUBLE, DOUBLE,
                StandardTypes.VARCHAR, VARCHAR,
                StandardTypes.VARBINARY, VARBINARY);

        public FlightShimTypeDeserializer()
        {
            super(Type.class);
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = types.get(value.toLowerCase(ENGLISH));
            //checkArgument(type != null, "Unknown type %s", value);
            // TODO: don't think this is used but..
            if (type == null) {
                type = VARCHAR;
            }
            return type;
        }
    }
}
