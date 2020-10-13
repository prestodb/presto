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
package com.facebook.presto.connector;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.connector.informationSchema.InformationSchemaConnector;
import com.facebook.presto.connector.system.DelegatingSystemTablesProvider;
import com.facebook.presto.connector.system.MetadataBasedSystemTablesProvider;
import com.facebook.presto.connector.system.StaticSystemTablesProvider;
import com.facebook.presto.connector.system.SystemConnector;
import com.facebook.presto.connector.system.SystemTablesProvider;
import com.facebook.presto.cost.ConnectorFilterStatsCalculatorService;
import com.facebook.presto.cost.FilterStatsCalculator;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.ConnectorMetadataUpdaterManager;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorIndexProvider;
import com.facebook.presto.spi.connector.ConnectorMetadataUpdaterProvider;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.RecordPageSourceProvider;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.planPrinter.RowExpressionFormatter;
import com.facebook.presto.sql.relational.ConnectorRowExpressionService;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.spi.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.spi.ConnectorId.createSystemTablesConnectorId;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ConnectorManager
{
    private static final Logger log = Logger.get(ConnectorManager.class);

    private final MetadataManager metadataManager;
    private final CatalogManager catalogManager;
    private final AccessControlManager accessControlManager;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;
    private final IndexManager indexManager;
    private final PartitioningProviderManager partitioningProviderManager;
    private final ConnectorPlanOptimizerManager connectorPlanOptimizerManager;
    private final ConnectorMetadataUpdaterManager connectorMetadataUpdaterManager;

    private final PageSinkManager pageSinkManager;
    private final HandleResolver handleResolver;
    private final InternalNodeManager nodeManager;
    private final TypeManager typeManager;
    private final PageSorter pageSorter;
    private final PageIndexerFactory pageIndexerFactory;
    private final NodeInfo nodeInfo;
    private final TransactionManager transactionManager;
    private final DomainTranslator domainTranslator;
    private final PredicateCompiler predicateCompiler;
    private final DeterminismEvaluator determinismEvaluator;
    private final FilterStatsCalculator filterStatsCalculator;
    private final BlockEncodingSerde blockEncodingSerde;

    @GuardedBy("this")
    private final ConcurrentMap<String, ConnectorFactory> connectorFactories = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final ConcurrentMap<ConnectorId, MaterializedConnector> connectors = new ConcurrentHashMap<>();

    private final AtomicBoolean stopped = new AtomicBoolean();

    @Inject
    public ConnectorManager(
            MetadataManager metadataManager,
            CatalogManager catalogManager,
            AccessControlManager accessControlManager,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            IndexManager indexManager,
            PartitioningProviderManager partitioningProviderManager,
            ConnectorPlanOptimizerManager connectorPlanOptimizerManager,
            ConnectorMetadataUpdaterManager connectorMetadataUpdaterManager,
            PageSinkManager pageSinkManager,
            HandleResolver handleResolver,
            InternalNodeManager nodeManager,
            NodeInfo nodeInfo,
            TypeManager typeManager,
            PageSorter pageSorter,
            PageIndexerFactory pageIndexerFactory,
            TransactionManager transactionManager,
            DomainTranslator domainTranslator,
            PredicateCompiler predicateCompiler,
            DeterminismEvaluator determinismEvaluator,
            FilterStatsCalculator filterStatsCalculator,
            BlockEncodingSerde blockEncodingSerde)
    {
        this.metadataManager = requireNonNull(metadataManager, "metadataManager is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.accessControlManager = requireNonNull(accessControlManager, "accessControlManager is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceManager = requireNonNull(pageSourceManager, "pageSourceManager is null");
        this.indexManager = requireNonNull(indexManager, "indexManager is null");
        this.partitioningProviderManager = requireNonNull(partitioningProviderManager, "partitioningProviderManager is null");
        this.connectorPlanOptimizerManager = requireNonNull(connectorPlanOptimizerManager, "connectorPlanOptimizerManager is null");
        this.connectorMetadataUpdaterManager = requireNonNull(connectorMetadataUpdaterManager, "connectorMetadataUpdaterManager is null");
        this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        this.nodeInfo = requireNonNull(nodeInfo, "nodeInfo is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");
        this.predicateCompiler = requireNonNull(predicateCompiler, "predicateCompiler is null");
        this.determinismEvaluator = requireNonNull(determinismEvaluator, "determinismEvaluator is null");
        this.filterStatsCalculator = requireNonNull(filterStatsCalculator, "filterStatsCalculator is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
    }

    @PreDestroy
    public synchronized void stop()
    {
        if (stopped.getAndSet(true)) {
            return;
        }

        for (Map.Entry<ConnectorId, MaterializedConnector> entry : connectors.entrySet()) {
            Connector connector = entry.getValue().getConnector();
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(connector.getClass().getClassLoader())) {
                connector.shutdown();
            }
            catch (Throwable t) {
                log.error(t, "Error shutting down connector: %s", entry.getKey());
            }
        }
    }

    public synchronized void addConnectorFactory(ConnectorFactory connectorFactory)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        ConnectorFactory existingConnectorFactory = connectorFactories.putIfAbsent(connectorFactory.getName(), connectorFactory);
        checkArgument(existingConnectorFactory == null, "Connector %s is already registered", connectorFactory.getName());
        handleResolver.addConnectorName(connectorFactory.getName(), connectorFactory.getHandleResolver());
    }

    public synchronized ConnectorId createConnection(String catalogName, String connectorName, Map<String, String> properties)
    {
        requireNonNull(connectorName, "connectorName is null");
        ConnectorFactory connectorFactory = connectorFactories.get(connectorName);
        checkArgument(connectorFactory != null, "No factory for connector %s", connectorName);
        return createConnection(catalogName, connectorFactory, properties);
    }

    private synchronized ConnectorId createConnection(String catalogName, ConnectorFactory connectorFactory, Map<String, String> properties)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(properties, "properties is null");
        requireNonNull(connectorFactory, "connectorFactory is null");
        checkArgument(!catalogManager.getCatalog(catalogName).isPresent(), "A catalog already exists for %s", catalogName);

        ConnectorId connectorId = new ConnectorId(catalogName);
        checkState(!connectors.containsKey(connectorId), "A connector %s already exists", connectorId);

        addCatalogConnector(catalogName, connectorId, connectorFactory, properties);

        return connectorId;
    }

    private synchronized void addCatalogConnector(String catalogName, ConnectorId connectorId, ConnectorFactory factory, Map<String, String> properties)
    {
        // create all connectors before adding, so a broken connector does not leave the system half updated
        MaterializedConnector connector = new MaterializedConnector(connectorId, createConnector(connectorId, factory, properties));

        MaterializedConnector informationSchemaConnector = new MaterializedConnector(
                createInformationSchemaConnectorId(connectorId),
                new InformationSchemaConnector(catalogName, nodeManager, metadataManager, accessControlManager, connector.getSessionProperties()));

        ConnectorId systemId = createSystemTablesConnectorId(connectorId);
        SystemTablesProvider systemTablesProvider;

        if (nodeManager.getCurrentNode().isCoordinator()) {
            systemTablesProvider = new DelegatingSystemTablesProvider(
                    new StaticSystemTablesProvider(connector.getSystemTables()),
                    new MetadataBasedSystemTablesProvider(metadataManager, catalogName));
        }
        else {
            systemTablesProvider = new StaticSystemTablesProvider(connector.getSystemTables());
        }

        MaterializedConnector systemConnector = new MaterializedConnector(systemId, new SystemConnector(
                systemId,
                nodeManager,
                systemTablesProvider,
                transactionId -> transactionManager.getConnectorTransaction(transactionId, connectorId),
                connector.getSessionProperties()));

        Catalog catalog = new Catalog(
                catalogName,
                connector.getConnectorId(),
                connector.getConnector(),
                informationSchemaConnector.getConnectorId(),
                informationSchemaConnector.getConnector(),
                systemConnector.getConnectorId(),
                systemConnector.getConnector());

        try {
            addConnectorInternal(connector);
            addConnectorInternal(informationSchemaConnector);
            addConnectorInternal(systemConnector);
            catalogManager.registerCatalog(catalog);
        }
        catch (Throwable e) {
            catalogManager.removeCatalog(catalog.getCatalogName());
            removeConnectorInternal(systemConnector.getConnectorId());
            removeConnectorInternal(informationSchemaConnector.getConnectorId());
            removeConnectorInternal(connector.getConnectorId());
            throw e;
        }
    }

    private synchronized void addConnectorInternal(MaterializedConnector connector)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        ConnectorId connectorId = connector.getConnectorId();
        checkState(!connectors.containsKey(connectorId), "A connector %s already exists", connectorId);
        connectors.put(connectorId, connector);

        splitManager.addConnectorSplitManager(connectorId, connector.getSplitManager());
        pageSourceManager.addConnectorPageSourceProvider(connectorId, connector.getPageSourceProvider());

        connector.getPageSinkProvider()
                .ifPresent(pageSinkProvider -> pageSinkManager.addConnectorPageSinkProvider(connectorId, pageSinkProvider));

        connector.getIndexProvider()
                .ifPresent(indexProvider -> indexManager.addIndexProvider(connectorId, indexProvider));

        connector.getPartitioningProvider()
                .ifPresent(partitioningProvider -> partitioningProviderManager.addPartitioningProvider(connectorId, partitioningProvider));

        if (nodeManager.getCurrentNode().isCoordinator()) {
            connector.getPlanOptimizerProvider()
                    .ifPresent(planOptimizerProvider -> connectorPlanOptimizerManager.addPlanOptimizerProvider(connectorId, planOptimizerProvider));
        }

        connector.getMetadataUpdaterProvider()
                .ifPresent(metadataUpdaterProvider -> connectorMetadataUpdaterManager.addMetadataUpdaterProvider(connectorId, metadataUpdaterProvider));

        metadataManager.getProcedureRegistry().addProcedures(connectorId, connector.getProcedures());

        connector.getAccessControl()
                .ifPresent(accessControl -> accessControlManager.addCatalogAccessControl(connectorId, accessControl));

        metadataManager.getTablePropertyManager().addProperties(connectorId, connector.getTableProperties());
        metadataManager.getColumnPropertyManager().addProperties(connectorId, connector.getColumnProperties());
        metadataManager.getSchemaPropertyManager().addProperties(connectorId, connector.getSchemaProperties());
        metadataManager.getAnalyzePropertyManager().addProperties(connectorId, connector.getAnalyzeProperties());
        metadataManager.getSessionPropertyManager().addConnectorSessionProperties(connectorId, connector.getSessionProperties());
    }

    public synchronized void dropConnection(String catalogName)
    {
        requireNonNull(catalogName, "catalogName is null");

        catalogManager.removeCatalog(catalogName).ifPresent(connectorId -> {
            // todo wait for all running transactions using the connector to complete before removing the services
            removeConnectorInternal(connectorId);
            removeConnectorInternal(createInformationSchemaConnectorId(connectorId));
            removeConnectorInternal(createSystemTablesConnectorId(connectorId));
        });
    }

    private synchronized void removeConnectorInternal(ConnectorId connectorId)
    {
        splitManager.removeConnectorSplitManager(connectorId);
        pageSourceManager.removeConnectorPageSourceProvider(connectorId);
        pageSinkManager.removeConnectorPageSinkProvider(connectorId);
        indexManager.removeIndexProvider(connectorId);
        partitioningProviderManager.removePartitioningProvider(connectorId);
        metadataManager.getProcedureRegistry().removeProcedures(connectorId);
        accessControlManager.removeCatalogAccessControl(connectorId);
        metadataManager.getTablePropertyManager().removeProperties(connectorId);
        metadataManager.getColumnPropertyManager().removeProperties(connectorId);
        metadataManager.getSchemaPropertyManager().removeProperties(connectorId);
        metadataManager.getAnalyzePropertyManager().removeProperties(connectorId);
        metadataManager.getSessionPropertyManager().removeConnectorSessionProperties(connectorId);
        connectorPlanOptimizerManager.removePlanOptimizerProvider(connectorId);
        connectorMetadataUpdaterManager.removeMetadataUpdaterProvider(connectorId);

        MaterializedConnector materializedConnector = connectors.remove(connectorId);
        if (materializedConnector != null) {
            Connector connector = materializedConnector.getConnector();
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(connector.getClass().getClassLoader())) {
                connector.shutdown();
            }
            catch (Throwable t) {
                log.error(t, "Error shutting down connector: %s", connectorId);
            }
        }
    }

    private Connector createConnector(ConnectorId connectorId, ConnectorFactory factory, Map<String, String> properties)
    {
        ConnectorContext context = new ConnectorContextInstance(
                new ConnectorAwareNodeManager(nodeManager, nodeInfo.getEnvironment(), connectorId),
                typeManager,
                metadataManager.getFunctionAndTypeManager(),
                new FunctionResolution(metadataManager.getFunctionAndTypeManager()),
                pageSorter,
                pageIndexerFactory,
                new ConnectorRowExpressionService(
                        domainTranslator,
                        new RowExpressionOptimizer(metadataManager),
                        predicateCompiler,
                        determinismEvaluator,
                        new RowExpressionFormatter(metadataManager.getFunctionAndTypeManager())),
                new ConnectorFilterStatsCalculatorService(filterStatsCalculator),
                blockEncodingSerde);

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            return factory.create(connectorId.getCatalogName(), properties, context);
        }
    }

    private static class MaterializedConnector
    {
        private final ConnectorId connectorId;
        private final Connector connector;
        private final ConnectorSplitManager splitManager;
        private final Set<SystemTable> systemTables;
        private final Set<Procedure> procedures;
        private final ConnectorPageSourceProvider pageSourceProvider;
        private final Optional<ConnectorPageSinkProvider> pageSinkProvider;
        private final Optional<ConnectorIndexProvider> indexProvider;
        private final Optional<ConnectorNodePartitioningProvider> partitioningProvider;
        private final Optional<ConnectorPlanOptimizerProvider> planOptimizerProvider;
        private final Optional<ConnectorMetadataUpdaterProvider> metadataUpdaterProvider;
        private final Optional<ConnectorAccessControl> accessControl;
        private final List<PropertyMetadata<?>> sessionProperties;
        private final List<PropertyMetadata<?>> tableProperties;
        private final List<PropertyMetadata<?>> schemaProperties;
        private final List<PropertyMetadata<?>> columnProperties;
        private final List<PropertyMetadata<?>> analyzeProperties;

        public MaterializedConnector(ConnectorId connectorId, Connector connector)
        {
            this.connectorId = requireNonNull(connectorId, "connectorId is null");
            this.connector = requireNonNull(connector, "connector is null");

            splitManager = connector.getSplitManager();
            checkState(splitManager != null, "Connector %s does not have a split manager", connectorId);

            Set<SystemTable> systemTables = connector.getSystemTables();
            requireNonNull(systemTables, "Connector %s returned a null system tables set");
            this.systemTables = ImmutableSet.copyOf(systemTables);

            Set<Procedure> procedures = connector.getProcedures();
            requireNonNull(procedures, "Connector %s returned a null procedures set");
            this.procedures = ImmutableSet.copyOf(procedures);

            ConnectorPageSourceProvider connectorPageSourceProvider = null;
            try {
                connectorPageSourceProvider = connector.getPageSourceProvider();
                requireNonNull(connectorPageSourceProvider, format("Connector %s returned a null page source provider", connectorId));
            }
            catch (UnsupportedOperationException ignored) {
            }

            if (connectorPageSourceProvider == null) {
                ConnectorRecordSetProvider connectorRecordSetProvider = null;
                try {
                    connectorRecordSetProvider = connector.getRecordSetProvider();
                    requireNonNull(connectorRecordSetProvider, format("Connector %s returned a null record set provider", connectorId));
                }
                catch (UnsupportedOperationException ignored) {
                }
                checkState(connectorRecordSetProvider != null, "Connector %s has neither a PageSource or RecordSet provider", connectorId);
                connectorPageSourceProvider = new RecordPageSourceProvider(connectorRecordSetProvider);
            }
            this.pageSourceProvider = connectorPageSourceProvider;

            ConnectorPageSinkProvider connectorPageSinkProvider = null;
            try {
                connectorPageSinkProvider = connector.getPageSinkProvider();
                requireNonNull(connectorPageSinkProvider, format("Connector %s returned a null page sink provider", connectorId));
            }
            catch (UnsupportedOperationException ignored) {
            }
            this.pageSinkProvider = Optional.ofNullable(connectorPageSinkProvider);

            ConnectorIndexProvider indexProvider = null;
            try {
                indexProvider = connector.getIndexProvider();
                requireNonNull(indexProvider, format("Connector %s returned a null index provider", connectorId));
            }
            catch (UnsupportedOperationException ignored) {
            }
            this.indexProvider = Optional.ofNullable(indexProvider);

            ConnectorNodePartitioningProvider partitioningProvider = null;
            try {
                partitioningProvider = connector.getNodePartitioningProvider();
                requireNonNull(partitioningProvider, format("Connector %s returned a null partitioning provider", connectorId));
            }
            catch (UnsupportedOperationException ignored) {
            }
            this.partitioningProvider = Optional.ofNullable(partitioningProvider);

            ConnectorPlanOptimizerProvider planOptimizerProvider = null;
            try {
                planOptimizerProvider = connector.getConnectorPlanOptimizerProvider();
                requireNonNull(planOptimizerProvider, format("Connector %s returned a null plan optimizer provider", connectorId));
            }
            catch (UnsupportedOperationException ignored) {
            }
            this.planOptimizerProvider = Optional.ofNullable(planOptimizerProvider);

            ConnectorMetadataUpdaterProvider metadataUpdaterProvider = null;
            try {
                metadataUpdaterProvider = connector.getConnectorMetadataUpdaterProvider();
                requireNonNull(metadataUpdaterProvider, format("Connector %s returned null metadata updater provider", connectorId));
            }
            catch (UnsupportedOperationException ignored) {
            }
            this.metadataUpdaterProvider = Optional.ofNullable(metadataUpdaterProvider);

            ConnectorAccessControl accessControl = null;
            try {
                accessControl = connector.getAccessControl();
            }
            catch (UnsupportedOperationException ignored) {
            }
            this.accessControl = Optional.ofNullable(accessControl);

            List<PropertyMetadata<?>> sessionProperties = connector.getSessionProperties();
            requireNonNull(sessionProperties, "Connector %s returned a null system properties set");
            this.sessionProperties = ImmutableList.copyOf(sessionProperties);

            List<PropertyMetadata<?>> tableProperties = connector.getTableProperties();
            requireNonNull(tableProperties, "Connector %s returned a null table properties set");
            this.tableProperties = ImmutableList.copyOf(tableProperties);

            List<PropertyMetadata<?>> schemaProperties = connector.getSchemaProperties();
            requireNonNull(schemaProperties, "Connector %s returned a null schema properties set");
            this.schemaProperties = ImmutableList.copyOf(schemaProperties);

            List<PropertyMetadata<?>> columnProperties = connector.getColumnProperties();
            requireNonNull(columnProperties, "Connector %s returned a null column properties set");
            this.columnProperties = ImmutableList.copyOf(columnProperties);

            List<PropertyMetadata<?>> analyzeProperties = connector.getAnalyzeProperties();
            requireNonNull(analyzeProperties, "Connector %s returned a null analyze properties set");
            this.analyzeProperties = ImmutableList.copyOf(analyzeProperties);
        }

        public ConnectorId getConnectorId()
        {
            return connectorId;
        }

        public Connector getConnector()
        {
            return connector;
        }

        public ConnectorSplitManager getSplitManager()
        {
            return splitManager;
        }

        public Set<SystemTable> getSystemTables()
        {
            return systemTables;
        }

        public Set<Procedure> getProcedures()
        {
            return procedures;
        }

        public ConnectorPageSourceProvider getPageSourceProvider()
        {
            return pageSourceProvider;
        }

        public Optional<ConnectorPageSinkProvider> getPageSinkProvider()
        {
            return pageSinkProvider;
        }

        public Optional<ConnectorIndexProvider> getIndexProvider()
        {
            return indexProvider;
        }

        public Optional<ConnectorNodePartitioningProvider> getPartitioningProvider()
        {
            return partitioningProvider;
        }

        public Optional<ConnectorPlanOptimizerProvider> getPlanOptimizerProvider()
        {
            return planOptimizerProvider;
        }

        public Optional<ConnectorMetadataUpdaterProvider> getMetadataUpdaterProvider()
        {
            return metadataUpdaterProvider;
        }

        public Optional<ConnectorAccessControl> getAccessControl()
        {
            return accessControl;
        }

        public List<PropertyMetadata<?>> getSessionProperties()
        {
            return sessionProperties;
        }

        public List<PropertyMetadata<?>> getTableProperties()
        {
            return tableProperties;
        }

        public List<PropertyMetadata<?>> getColumnProperties()
        {
            return columnProperties;
        }

        public List<PropertyMetadata<?>> getSchemaProperties()
        {
            return schemaProperties;
        }

        public List<PropertyMetadata<?>> getAnalyzeProperties()
        {
            return analyzeProperties;
        }
    }
}
