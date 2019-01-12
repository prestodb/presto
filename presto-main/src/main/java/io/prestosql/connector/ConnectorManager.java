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
package io.prestosql.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.prestosql.connector.informationSchema.InformationSchemaConnector;
import io.prestosql.connector.system.DelegatingSystemTablesProvider;
import io.prestosql.connector.system.MetadataBasedSystemTablesProvider;
import io.prestosql.connector.system.StaticSystemTablesProvider;
import io.prestosql.connector.system.SystemConnector;
import io.prestosql.connector.system.SystemTablesProvider;
import io.prestosql.index.IndexManager;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.HandleResolver;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.security.AccessControlManager;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorIndexProvider;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.split.PageSinkManager;
import io.prestosql.split.PageSourceManager;
import io.prestosql.split.RecordPageSourceProvider;
import io.prestosql.split.SplitManager;
import io.prestosql.sql.planner.NodePartitioningManager;
import io.prestosql.transaction.TransactionManager;

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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.connector.ConnectorId.createInformationSchemaConnectorId;
import static io.prestosql.connector.ConnectorId.createSystemTablesConnectorId;
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
    private final NodePartitioningManager nodePartitioningManager;

    private final PageSinkManager pageSinkManager;
    private final HandleResolver handleResolver;
    private final InternalNodeManager nodeManager;
    private final TypeManager typeManager;
    private final PageSorter pageSorter;
    private final PageIndexerFactory pageIndexerFactory;
    private final NodeInfo nodeInfo;
    private final TransactionManager transactionManager;

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
            NodePartitioningManager nodePartitioningManager,
            PageSinkManager pageSinkManager,
            HandleResolver handleResolver,
            InternalNodeManager nodeManager,
            NodeInfo nodeInfo,
            TypeManager typeManager,
            PageSorter pageSorter,
            PageIndexerFactory pageIndexerFactory,
            TransactionManager transactionManager)
    {
        this.metadataManager = metadataManager;
        this.catalogManager = catalogManager;
        this.accessControlManager = accessControlManager;
        this.splitManager = splitManager;
        this.pageSourceManager = pageSourceManager;
        this.indexManager = indexManager;
        this.nodePartitioningManager = nodePartitioningManager;
        this.pageSinkManager = pageSinkManager;
        this.handleResolver = handleResolver;
        this.nodeManager = nodeManager;
        this.typeManager = typeManager;
        this.pageSorter = pageSorter;
        this.pageIndexerFactory = pageIndexerFactory;
        this.nodeInfo = nodeInfo;
        this.transactionManager = transactionManager;
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
                new InformationSchemaConnector(catalogName, nodeManager, metadataManager, accessControlManager));

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
                transactionId -> transactionManager.getConnectorTransaction(transactionId, connectorId)));

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
                .ifPresent(partitioningProvider -> nodePartitioningManager.addPartitioningProvider(connectorId, partitioningProvider));

        metadataManager.getProcedureRegistry().addProcedures(connectorId, connector.getProcedures());

        connector.getAccessControl()
                .ifPresent(accessControl -> accessControlManager.addCatalogAccessControl(connectorId, accessControl));

        metadataManager.getTablePropertyManager().addProperties(connectorId, connector.getTableProperties());
        metadataManager.getColumnPropertyManager().addProperties(connectorId, connector.getColumnProperties());
        metadataManager.getSchemaPropertyManager().addProperties(connectorId, connector.getSchemaProperties());
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
        nodePartitioningManager.removePartitioningProvider(connectorId);
        metadataManager.getProcedureRegistry().removeProcedures(connectorId);
        accessControlManager.removeCatalogAccessControl(connectorId);
        metadataManager.getTablePropertyManager().removeProperties(connectorId);
        metadataManager.getColumnPropertyManager().removeProperties(connectorId);
        metadataManager.getSchemaPropertyManager().removeProperties(connectorId);
        metadataManager.getSessionPropertyManager().removeConnectorSessionProperties(connectorId);

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
                pageSorter,
                pageIndexerFactory);

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
        private final Optional<ConnectorAccessControl> accessControl;
        private final List<PropertyMetadata<?>> sessionProperties;
        private final List<PropertyMetadata<?>> tableProperties;
        private final List<PropertyMetadata<?>> schemaProperties;
        private final List<PropertyMetadata<?>> columnProperties;

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
    }
}
