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
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TransactionalConnectorIndexProvider;
import com.facebook.presto.spi.TransactionalConnectorPageSinkProvider;
import com.facebook.presto.spi.TransactionalConnectorPageSourceProvider;
import com.facebook.presto.spi.TransactionalConnectorRecordSetProvider;
import com.facebook.presto.spi.TransactionalConnectorRecordSinkProvider;
import com.facebook.presto.spi.TransactionalConnectorSplitManager;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.security.TransactionalConnectorAccessControl;
import com.facebook.presto.spi.transaction.TransactionalConnector;
import com.facebook.presto.spi.transaction.TransactionalConnectorFactory;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.RecordPageSinkProvider;
import com.facebook.presto.split.RecordPageSourceProvider;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.transaction.LegacyTransactionConnectorFactory;
import com.facebook.presto.transaction.TransactionManager;
import io.airlift.log.Logger;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ConnectorManager
{
    public static final String INFORMATION_SCHEMA_CONNECTOR_PREFIX = "$info_schema@";
    public static final String SYSTEM_TABLES_CONNECTOR_PREFIX = "$system@";

    private static final Logger log = Logger.get(ConnectorManager.class);

    private final MetadataManager metadataManager;
    private final AccessControlManager accessControlManager;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;
    private final IndexManager indexManager;

    private final PageSinkManager pageSinkManager;
    private final HandleResolver handleResolver;
    private final NodeManager nodeManager;
    private final TransactionManager transactionManager;

    private final ConcurrentMap<String, TransactionalConnectorFactory> connectorFactories = new ConcurrentHashMap<>();

    private final Set<String> catalogs = newConcurrentHashSet();
    private final ConcurrentMap<String, TransactionalConnector> connectors = new ConcurrentHashMap<>();

    private final AtomicBoolean stopped = new AtomicBoolean();

    @Inject
    public ConnectorManager(MetadataManager metadataManager,
            AccessControlManager accessControlManager,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            IndexManager indexManager,
            PageSinkManager pageSinkManager,
            HandleResolver handleResolver,
            NodeManager nodeManager,
            TransactionManager transactionManager)
    {
        this.metadataManager = metadataManager;
        this.accessControlManager = accessControlManager;
        this.splitManager = splitManager;
        this.pageSourceManager = pageSourceManager;
        this.indexManager = indexManager;
        this.pageSinkManager = pageSinkManager;
        this.handleResolver = handleResolver;
        this.nodeManager = nodeManager;
        this.transactionManager = transactionManager;
    }

    @PreDestroy
    public void stop()
    {
        if (stopped.getAndSet(true)) {
            return;
        }

        for (Map.Entry<String, TransactionalConnector> entry : connectors.entrySet()) {
            TransactionalConnector connector = entry.getValue();
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(connector.getClass().getClassLoader())) {
                connector.shutdown();
            }
            catch (Throwable t) {
                log.error(t, "Error shutting down connector: %s", entry.getKey());
            }
        }
    }

    public void addConnectorFactory(ConnectorFactory connectorFactory)
    {
        addConnectorFactory(new LegacyTransactionConnectorFactory(connectorFactory));
    }

    public void addConnectorFactory(TransactionalConnectorFactory connectorFactory)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        TransactionalConnectorFactory existingConnectorFactory = connectorFactories.putIfAbsent(connectorFactory.getName(), connectorFactory);
        checkArgument(existingConnectorFactory == null, "Connector %s is already registered", connectorFactory.getName());
    }

    public synchronized void createConnection(String catalogName, String connectorName, Map<String, String> properties)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(connectorName, "connectorName is null");
        requireNonNull(properties, "properties is null");

        TransactionalConnectorFactory connectorFactory = connectorFactories.get(connectorName);
        checkArgument(connectorFactory != null, "No factory for connector %s", connectorName);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(connectorFactory.getClass().getClassLoader())) {
            createConnection(catalogName, connectorFactory, properties);
        }
    }

    public synchronized void createConnection(String catalogName, ConnectorFactory connectorFactory, Map<String, String> properties)
    {
        createConnection(catalogName, new LegacyTransactionConnectorFactory(connectorFactory), properties);
    }

    public synchronized void createConnection(String catalogName, TransactionalConnectorFactory connectorFactory, Map<String, String> properties)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(properties, "properties is null");
        requireNonNull(connectorFactory, "connectorFactory is null");
        checkArgument(!catalogs.contains(catalogName), "A catalog already exists for %s", catalogName);

        String connectorId = getConnectorId(catalogName);
        checkState(!connectors.containsKey(connectorId), "A connector %s already exists", connectorId);

        TransactionalConnector connector = connectorFactory.create(connectorId, properties);

        addCatalogConnector(catalogName, connectorId, connector);
        catalogs.add(catalogName);
    }

    private synchronized void addCatalogConnector(String catalogName, String connectorId, TransactionalConnector connector)
    {
        addConnectorInternal(ConnectorType.STANDARD, catalogName, connectorId, connector);
        String informationSchemaId = makeInformationSchemaConnectorId(connectorId);
        addConnectorInternal(ConnectorType.INFORMATION_SCHEMA, catalogName, informationSchemaId, new InformationSchemaConnector(informationSchemaId, catalogName, nodeManager, metadataManager));
        String systemId = makeSystemTablesConnectorId(connectorId);
        addConnectorInternal(ConnectorType.SYSTEM, catalogName, systemId, new SystemConnector(
                systemId,
                nodeManager,
                connector.getSystemTables(),
                transactionId -> transactionManager.getConnectorTransaction(transactionId, connectorId)));

        // Register session and table properties once per catalog
        metadataManager.getSessionPropertyManager().addConnectorSessionProperties(catalogName, connector.getSessionProperties());
        metadataManager.getTablePropertyManager().addTableProperties(catalogName, connector.getTableProperties());
    }

    private synchronized void addConnectorInternal(ConnectorType type, String catalogName, String connectorId, TransactionalConnector connector)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        checkState(!connectors.containsKey(connectorId), "A connector %s already exists", connectorId);
        connectors.put(connectorId, connector);

        TransactionalConnectorSplitManager connectorSplitManager = connector.getSplitManager();
        checkState(connectorSplitManager != null, "Connector %s does not have a split manager", connectorId);

        Set<SystemTable> systemTables = connector.getSystemTables();
        requireNonNull(systemTables, "Connector %s returned a null system tables set");

        TransactionalConnectorPageSourceProvider connectorPageSourceProvider = null;
        try {
            connectorPageSourceProvider = connector.getPageSourceProvider();
            requireNonNull(connectorPageSourceProvider, format("Connector %s returned a null page source provider", connectorId));
        }
        catch (UnsupportedOperationException ignored) {
        }

        if (connectorPageSourceProvider == null) {
            TransactionalConnectorRecordSetProvider connectorRecordSetProvider = null;
            try {
                connectorRecordSetProvider = connector.getRecordSetProvider();
                requireNonNull(connectorRecordSetProvider, format("Connector %s returned a null record set provider", connectorId));
            }
            catch (UnsupportedOperationException ignored) {
            }
            checkState(connectorRecordSetProvider != null, "Connector %s has neither a PageSource or RecordSet provider", connectorId);
            connectorPageSourceProvider = new RecordPageSourceProvider(connectorRecordSetProvider);
        }

        ConnectorHandleResolver connectorHandleResolver = connector.getHandleResolver();
        requireNonNull(connectorHandleResolver, format("Connector %s does not have a handle resolver", connectorId));

        TransactionalConnectorPageSinkProvider connectorPageSinkProvider = null;
        try {
            connectorPageSinkProvider = connector.getPageSinkProvider();
            requireNonNull(connectorPageSinkProvider, format("Connector %s returned a null page sink provider", connectorId));
        }
        catch (UnsupportedOperationException ignored) {
        }

        if (connectorPageSinkProvider == null) {
            TransactionalConnectorRecordSinkProvider connectorRecordSinkProvider = null;
            try {
                connectorRecordSinkProvider = connector.getRecordSinkProvider();
                requireNonNull(connectorRecordSinkProvider, format("Connector %s returned a null record sink provider", connectorId));
                connectorPageSinkProvider = new RecordPageSinkProvider(connectorRecordSinkProvider);
            }
            catch (UnsupportedOperationException ignored) {
            }
        }

        TransactionalConnectorIndexProvider indexProvider = null;
        try {
            indexProvider = connector.getIndexProvider();
            requireNonNull(indexProvider, format("Connector %s returned a null index provider", connectorId));
        }
        catch (UnsupportedOperationException ignored) {
        }

        requireNonNull(connector.getSessionProperties(), format("Connector %s returned null session properties", connectorId));
        requireNonNull(connector.getTableProperties(), format("Connector %s returned null table properties", connectorId));

        TransactionalConnectorAccessControl accessControl = null;
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
        handleResolver.addHandleResolver(connectorId, connectorHandleResolver);
        pageSourceManager.addConnectorPageSourceProvider(connectorId, connectorPageSourceProvider);

        if (connectorPageSinkProvider != null) {
            pageSinkManager.addConnectorPageSinkProvider(connectorId, connectorPageSinkProvider);
        }

        if (indexProvider != null) {
            indexManager.addIndexProvider(connectorId, indexProvider);
        }

        if (accessControl != null) {
            accessControlManager.addCatalogAccessControl(connectorId, catalogName, accessControl);
        }
    }

    private enum ConnectorType
    {
        STANDARD,
        INFORMATION_SCHEMA,
        SYSTEM
    }

    private static String makeInformationSchemaConnectorId(String connectorId)
    {
        return INFORMATION_SCHEMA_CONNECTOR_PREFIX + connectorId;
    }

    private static String makeSystemTablesConnectorId(String connectorId)
    {
        return SYSTEM_TABLES_CONNECTOR_PREFIX + connectorId;
    }

    private static String getConnectorId(String catalogName)
    {
        // for now connectorId == catalogName
        return catalogName;
    }
}
