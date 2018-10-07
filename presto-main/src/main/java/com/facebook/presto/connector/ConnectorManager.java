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

import com.facebook.presto.connector.informationSchema.InformationSchemaConnector;
import com.facebook.presto.connector.system.DelegatingSystemTablesProvider;
import com.facebook.presto.connector.system.MetadataBasedSystemTablesProvider;
import com.facebook.presto.connector.system.StaticSystemTablesProvider;
import com.facebook.presto.connector.system.SystemConnector;
import com.facebook.presto.connector.system.SystemTablesProvider;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorIndexProvider;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.RecordPageSourceProvider;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

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

import static com.facebook.presto.connector.CatalogName.createInformationSchemaCatalogName;
import static com.facebook.presto.connector.CatalogName.createSystemTablesCatalogName;
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
    private final ConcurrentMap<CatalogName, MaterializedConnector> connectors = new ConcurrentHashMap<>();

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

        for (Map.Entry<CatalogName, MaterializedConnector> entry : connectors.entrySet()) {
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

    public synchronized CatalogName createConnection(String catalogName, String connectorName, Map<String, String> properties)
    {
        requireNonNull(connectorName, "connectorName is null");
        ConnectorFactory connectorFactory = connectorFactories.get(connectorName);
        checkArgument(connectorFactory != null, "No factory for connector %s", connectorName);
        return createConnection(catalogName, connectorFactory, properties);
    }

    private synchronized CatalogName createConnection(String catalogName, ConnectorFactory connectorFactory, Map<String, String> properties)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(properties, "properties is null");
        requireNonNull(connectorFactory, "connectorFactory is null");
        checkArgument(!catalogManager.getCatalog(catalogName).isPresent(), "A catalog already exists for %s", catalogName);

        CatalogName connectorId = new CatalogName(catalogName);
        checkState(!connectors.containsKey(connectorId), "A connector %s already exists", connectorId);

        addCatalogConnector(catalogName, connectorId, connectorFactory, properties);

        return connectorId;
    }

    private synchronized void addCatalogConnector(String catalogName, CatalogName connectorId, ConnectorFactory factory, Map<String, String> properties)
    {
        // create all connectors before adding, so a broken connector does not leave the system half updated
        MaterializedConnector connector = new MaterializedConnector(connectorId, createConnector(connectorId, factory, properties));

        MaterializedConnector informationSchemaConnector = new MaterializedConnector(
                createInformationSchemaCatalogName(connectorId),
                new InformationSchemaConnector(catalogName, nodeManager, metadataManager, accessControlManager));

        CatalogName systemId = createSystemTablesCatalogName(connectorId);
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
                connector.getCatalogName(),
                connector.getConnector(),
                informationSchemaConnector.getCatalogName(),
                informationSchemaConnector.getConnector(),
                systemConnector.getCatalogName(),
                systemConnector.getConnector());

        try {
            addConnectorInternal(connector);
            addConnectorInternal(informationSchemaConnector);
            addConnectorInternal(systemConnector);
            catalogManager.registerCatalog(catalog);
        }
        catch (Throwable e) {
            catalogManager.removeCatalog(catalog.getCatalogName());
            removeConnectorInternal(systemConnector.getCatalogName());
            removeConnectorInternal(informationSchemaConnector.getCatalogName());
            removeConnectorInternal(connector.getCatalogName());
            throw e;
        }
    }

    private synchronized void addConnectorInternal(MaterializedConnector connector)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        CatalogName catalogName = connector.getCatalogName();
        checkState(!connectors.containsKey(catalogName), "A connector %s already exists", catalogName);
        connectors.put(catalogName, connector);

        splitManager.addConnectorSplitManager(catalogName, connector.getSplitManager());
        pageSourceManager.addConnectorPageSourceProvider(catalogName, connector.getPageSourceProvider());

        connector.getPageSinkProvider()
                .ifPresent(pageSinkProvider -> pageSinkManager.addConnectorPageSinkProvider(catalogName, pageSinkProvider));

        connector.getIndexProvider()
                .ifPresent(indexProvider -> indexManager.addIndexProvider(catalogName, indexProvider));

        connector.getPartitioningProvider()
                .ifPresent(partitioningProvider -> nodePartitioningManager.addPartitioningProvider(catalogName, partitioningProvider));

        metadataManager.getProcedureRegistry().addProcedures(catalogName, connector.getProcedures());

        connector.getAccessControl()
                .ifPresent(accessControl -> accessControlManager.addCatalogAccessControl(catalogName, accessControl));

        metadataManager.getTablePropertyManager().addProperties(catalogName, connector.getTableProperties());
        metadataManager.getColumnPropertyManager().addProperties(catalogName, connector.getColumnProperties());
        metadataManager.getSchemaPropertyManager().addProperties(catalogName, connector.getSchemaProperties());
        metadataManager.getSessionPropertyManager().addConnectorSessionProperties(catalogName, connector.getSessionProperties());
    }

    public synchronized void dropConnection(String catalogName)
    {
        requireNonNull(catalogName, "catalogName is null");

        catalogManager.removeCatalog(catalogName).ifPresent(connectorId -> {
            // todo wait for all running transactions using the connector to complete before removing the services
            removeConnectorInternal(connectorId);
            removeConnectorInternal(createInformationSchemaCatalogName(connectorId));
            removeConnectorInternal(createSystemTablesCatalogName(connectorId));
        });
    }

    private synchronized void removeConnectorInternal(CatalogName catalogName)
    {
        splitManager.removeConnectorSplitManager(catalogName);
        pageSourceManager.removeConnectorPageSourceProvider(catalogName);
        pageSinkManager.removeConnectorPageSinkProvider(catalogName);
        indexManager.removeIndexProvider(catalogName);
        nodePartitioningManager.removePartitioningProvider(catalogName);
        metadataManager.getProcedureRegistry().removeProcedures(catalogName);
        accessControlManager.removeCatalogAccessControl(catalogName);
        metadataManager.getTablePropertyManager().removeProperties(catalogName);
        metadataManager.getSchemaPropertyManager().removeProperties(catalogName);
        metadataManager.getSessionPropertyManager().removeConnectorSessionProperties(catalogName);

        MaterializedConnector materializedConnector = connectors.remove(catalogName);
        if (materializedConnector != null) {
            Connector connector = materializedConnector.getConnector();
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(connector.getClass().getClassLoader())) {
                connector.shutdown();
            }
            catch (Throwable t) {
                log.error(t, "Error shutting down connector: %s", catalogName);
            }
        }
    }

    private Connector createConnector(CatalogName catalogName, ConnectorFactory factory, Map<String, String> properties)
    {
        ConnectorContext context = new ConnectorContextInstance(
                new ConnectorAwareNodeManager(nodeManager, nodeInfo.getEnvironment(), catalogName),
                typeManager,
                pageSorter,
                pageIndexerFactory);

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            return factory.create(catalogName.getCatalogName(), properties, context);
        }
    }

    private static class MaterializedConnector
    {
        private final CatalogName catalogName;
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

        public MaterializedConnector(CatalogName catalogName, Connector connector)
        {
            this.catalogName = requireNonNull(catalogName, "connectorId is null");
            this.connector = requireNonNull(connector, "connector is null");

            splitManager = connector.getSplitManager();
            checkState(splitManager != null, "Connector %s does not have a split manager", catalogName);

            Set<SystemTable> systemTables = connector.getSystemTables();
            requireNonNull(systemTables, "Connector %s returned a null system tables set");
            this.systemTables = ImmutableSet.copyOf(systemTables);

            Set<Procedure> procedures = connector.getProcedures();
            requireNonNull(procedures, "Connector %s returned a null procedures set");
            this.procedures = ImmutableSet.copyOf(procedures);

            ConnectorPageSourceProvider connectorPageSourceProvider = null;
            try {
                connectorPageSourceProvider = connector.getPageSourceProvider();
                requireNonNull(connectorPageSourceProvider, format("Connector %s returned a null page source provider", catalogName));
            }
            catch (UnsupportedOperationException ignored) {
            }

            if (connectorPageSourceProvider == null) {
                ConnectorRecordSetProvider connectorRecordSetProvider = null;
                try {
                    connectorRecordSetProvider = connector.getRecordSetProvider();
                    requireNonNull(connectorRecordSetProvider, format("Connector %s returned a null record set provider", catalogName));
                }
                catch (UnsupportedOperationException ignored) {
                }
                checkState(connectorRecordSetProvider != null, "Connector %s has neither a PageSource or RecordSet provider", catalogName);
                connectorPageSourceProvider = new RecordPageSourceProvider(connectorRecordSetProvider);
            }
            this.pageSourceProvider = connectorPageSourceProvider;

            ConnectorPageSinkProvider connectorPageSinkProvider = null;
            try {
                connectorPageSinkProvider = connector.getPageSinkProvider();
                requireNonNull(connectorPageSinkProvider, format("Connector %s returned a null page sink provider", catalogName));
            }
            catch (UnsupportedOperationException ignored) {
            }
            this.pageSinkProvider = Optional.ofNullable(connectorPageSinkProvider);

            ConnectorIndexProvider indexProvider = null;
            try {
                indexProvider = connector.getIndexProvider();
                requireNonNull(indexProvider, format("Connector %s returned a null index provider", catalogName));
            }
            catch (UnsupportedOperationException ignored) {
            }
            this.indexProvider = Optional.ofNullable(indexProvider);

            ConnectorNodePartitioningProvider partitioningProvider = null;
            try {
                partitioningProvider = connector.getNodePartitioningProvider();
                requireNonNull(partitioningProvider, format("Connector %s returned a null partitioning provider", catalogName));
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

        public CatalogName getCatalogName()
        {
            return catalogName;
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
