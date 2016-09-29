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
import com.facebook.presto.connector.system.SystemConnector;
import com.facebook.presto.index.IndexManager;
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
import com.facebook.presto.spi.connector.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.RecordPageSinkProvider;
import com.facebook.presto.split.RecordPageSourceProvider;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.transaction.LegacyTransactionConnectorFactory;
import com.facebook.presto.transaction.TransactionManager;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.connector.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.connector.ConnectorId.createSystemTablesConnectorId;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ConnectorManager
{
    private static final Logger log = Logger.get(ConnectorManager.class);

    private final MetadataManager metadataManager;
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
    private final Set<String> catalogs = newConcurrentHashSet();
    @GuardedBy("this")
    private final ConcurrentMap<ConnectorId, Connector> connectors = new ConcurrentHashMap<>();

    private final AtomicBoolean stopped = new AtomicBoolean();

    @Inject
    public ConnectorManager(MetadataManager metadataManager,
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

        for (Map.Entry<ConnectorId, Connector> entry : connectors.entrySet()) {
            Connector connector = entry.getValue();
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
        checkArgument(!catalogs.contains(catalogName), "A catalog already exists for %s", catalogName);

        ConnectorId connectorId = new ConnectorId(catalogName);
        checkState(!connectors.containsKey(connectorId), "A connector %s already exists", connectorId);

        addCatalogConnector(catalogName, connectorId, connectorFactory, properties);

        catalogs.add(catalogName);

        return connectorId;
    }

    private synchronized void addCatalogConnector(String catalogName, ConnectorId connectorId, ConnectorFactory factory, Map<String, String> properties)
    {
        Connector connector = createConnector(connectorId, factory, properties);

        addConnectorInternal(ConnectorType.STANDARD, catalogName, connectorId, connector);

        ConnectorId informationSchemaId = createInformationSchemaConnectorId(connectorId);
        addConnectorInternal(ConnectorType.INFORMATION_SCHEMA, catalogName, informationSchemaId, new InformationSchemaConnector(catalogName, nodeManager, metadataManager));

        ConnectorId systemId = createSystemTablesConnectorId(connectorId);
        addConnectorInternal(ConnectorType.SYSTEM, catalogName, systemId, new SystemConnector(
                systemId,
                nodeManager,
                connector.getSystemTables(),
                transactionId -> transactionManager.getConnectorTransaction(transactionId, connectorId)));

        // Register session and table properties once per catalog
        metadataManager.getSessionPropertyManager().addConnectorSessionProperties(catalogName, connector.getSessionProperties());
        metadataManager.getSchemaPropertyManager().addProperties(catalogName, connector.getSchemaProperties());
        metadataManager.getTablePropertyManager().addProperties(catalogName, connector.getTableProperties());
    }

    private synchronized void addConnectorInternal(ConnectorType type, String catalogName, ConnectorId connectorId, Connector connector)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        checkState(!connectors.containsKey(connectorId), "A connector %s already exists", connectorId);
        connectors.put(connectorId, connector);

        ConnectorSplitManager connectorSplitManager = connector.getSplitManager();
        checkState(connectorSplitManager != null, "Connector %s does not have a split manager", connectorId);

        Set<SystemTable> systemTables = connector.getSystemTables();
        requireNonNull(systemTables, "Connector %s returned a null system tables set");

        Set<Procedure> procedures = connector.getProcedures();
        requireNonNull(procedures, "Connector %s returned a null procedures set");

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

        ConnectorPageSinkProvider connectorPageSinkProvider = null;
        try {
            connectorPageSinkProvider = connector.getPageSinkProvider();
            requireNonNull(connectorPageSinkProvider, format("Connector %s returned a null page sink provider", connectorId));
        }
        catch (UnsupportedOperationException ignored) {
        }

        if (connectorPageSinkProvider == null) {
            ConnectorRecordSinkProvider connectorRecordSinkProvider;
            try {
                connectorRecordSinkProvider = connector.getRecordSinkProvider();
                requireNonNull(connectorRecordSinkProvider, format("Connector %s returned a null record sink provider", connectorId));
                connectorPageSinkProvider = new RecordPageSinkProvider(connectorRecordSinkProvider);
            }
            catch (UnsupportedOperationException ignored) {
            }
        }

        ConnectorIndexProvider indexProvider = null;
        try {
            indexProvider = connector.getIndexProvider();
            requireNonNull(indexProvider, format("Connector %s returned a null index provider", connectorId));
        }
        catch (UnsupportedOperationException ignored) {
        }

        ConnectorNodePartitioningProvider partitioningProvider = null;
        try {
            partitioningProvider = connector.getNodePartitioningProvider();
            requireNonNull(partitioningProvider, format("Connector %s returned a null partitioning provider", connectorId));
        }
        catch (UnsupportedOperationException ignored) {
        }

        ConnectorAccessControl accessControl = null;
        try {
            accessControl = connector.getAccessControl();
        }
        catch (UnsupportedOperationException ignored) {
        }

        // IMPORTANT: all the instances need to be fetched from the connector *before* we add them to the corresponding managers.
        // Otherwise, a broken connector would leave the managers in an inconsistent state with respect to each other

        transactionManager.addConnector(connectorId, connector);

        if (type == ConnectorType.STANDARD) {
            metadataManager.registerConnectorCatalog(connectorId, catalogName);
        }
        else if (type == ConnectorType.INFORMATION_SCHEMA) {
            metadataManager.registerInformationSchemaCatalog(connectorId, catalogName);
        }
        else if (type == ConnectorType.SYSTEM) {
            metadataManager.registerSystemTablesCatalog(connectorId, catalogName);
        }
        else {
            throw new IllegalArgumentException("Unhandled type: " + type);
        }

        splitManager.addConnectorSplitManager(connectorId, connectorSplitManager);
        pageSourceManager.addConnectorPageSourceProvider(connectorId, connectorPageSourceProvider);

        for (Procedure procedure : procedures) {
            metadataManager.getProcedureRegistry().addProcedure(catalogName, procedure);
        }

        if (connectorPageSinkProvider != null) {
            pageSinkManager.addConnectorPageSinkProvider(connectorId, connectorPageSinkProvider);
        }

        if (indexProvider != null) {
            indexManager.addIndexProvider(connectorId, indexProvider);
        }

        if (partitioningProvider != null) {
            nodePartitioningManager.addPartitioningProvider(connectorId, partitioningProvider);
        }

        if (accessControl != null) {
            accessControlManager.addCatalogAccessControl(connectorId, catalogName, accessControl);
        }
    }

    private Connector createConnector(ConnectorId connectorId, ConnectorFactory factory, Map<String, String> properties)
    {
        Class<?> factoryClass = factory.getClass();
        if (factory instanceof LegacyTransactionConnectorFactory) {
            factoryClass = ((LegacyTransactionConnectorFactory) factory).getConnectorFactory().getClass();
        }

        ConnectorContext context = new ConnectorContextInstance(
                new ConnectorAwareNodeManager(nodeManager, nodeInfo.getEnvironment(), connectorId),
                typeManager,
                pageSorter,
                pageIndexerFactory);

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factoryClass.getClassLoader())) {
            return factory.create(connectorId.getCatalogName(), properties, context);
        }
    }

    private enum ConnectorType
    {
        STANDARD,
        INFORMATION_SCHEMA,
        SYSTEM
    }
}
