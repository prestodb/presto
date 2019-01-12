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
package io.prestosql.plugin.raptor.legacy;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.prestosql.plugin.raptor.legacy.metadata.ForMetadata;
import io.prestosql.plugin.raptor.legacy.metadata.MetadataDao;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.transaction.IsolationLevel;
import org.skife.jdbi.v2.IDBI;

import javax.annotation.PostConstruct;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.plugin.raptor.legacy.util.DatabaseUtil.onDemandDao;
import static io.prestosql.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.prestosql.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RaptorConnector
        implements Connector
{
    private static final Logger log = Logger.get(RaptorConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final RaptorMetadataFactory metadataFactory;
    private final RaptorSplitManager splitManager;
    private final RaptorPageSourceProvider pageSourceProvider;
    private final RaptorPageSinkProvider pageSinkProvider;
    private final RaptorNodePartitioningProvider nodePartitioningProvider;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final List<PropertyMetadata<?>> tableProperties;
    private final Set<SystemTable> systemTables;
    private final MetadataDao dao;
    private final ConnectorAccessControl accessControl;
    private final boolean coordinator;

    private final ConcurrentMap<ConnectorTransactionHandle, RaptorMetadata> transactions = new ConcurrentHashMap<>();

    private final ScheduledExecutorService unblockMaintenanceExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("raptor-unblock-maintenance"));

    @GuardedBy("this")
    private final SetMultimap<Long, UUID> deletions = HashMultimap.create();

    @Inject
    public RaptorConnector(
            LifeCycleManager lifeCycleManager,
            NodeManager nodeManager,
            RaptorMetadataFactory metadataFactory,
            RaptorSplitManager splitManager,
            RaptorPageSourceProvider pageSourceProvider,
            RaptorPageSinkProvider pageSinkProvider,
            RaptorNodePartitioningProvider nodePartitioningProvider,
            RaptorSessionProperties sessionProperties,
            RaptorTableProperties tableProperties,
            Set<SystemTable> systemTables,
            ConnectorAccessControl accessControl,
            @ForMetadata IDBI dbi)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.nodePartitioningProvider = requireNonNull(nodePartitioningProvider, "nodePartitioningProvider is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null").getSessionProperties();
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null").getTableProperties();
        this.systemTables = requireNonNull(systemTables, "systemTables is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.dao = onDemandDao(dbi, MetadataDao.class);
        this.coordinator = nodeManager.getCurrentNode().isCoordinator();
    }

    @PostConstruct
    public void start()
    {
        if (coordinator) {
            dao.unblockAllMaintenance();
        }
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return true;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        RaptorTransactionHandle transaction = new RaptorTransactionHandle();
        transactions.put(transaction, metadataFactory.create(tableId -> beginDelete(tableId, transaction.getUuid())));
        return transaction;
    }

    @Override
    public void commit(ConnectorTransactionHandle transaction)
    {
        checkArgument(transactions.remove(transaction) != null, "no such transaction: %s", transaction);
        finishDelete(((RaptorTransactionHandle) transaction).getUuid());
    }

    @Override
    public void rollback(ConnectorTransactionHandle transaction)
    {
        RaptorMetadata metadata = transactions.remove(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        finishDelete(((RaptorTransactionHandle) transaction).getUuid());
        metadata.rollback();
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
    {
        RaptorMetadata metadata = transactions.get(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return nodePartitioningProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return systemTables;
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return accessControl;
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }

    private synchronized void beginDelete(long tableId, UUID transactionId)
    {
        dao.blockMaintenance(tableId);
        verify(deletions.put(tableId, transactionId));
    }

    private synchronized void finishDelete(UUID transactionId)
    {
        deletions.entries().stream()
                .filter(entry -> entry.getValue().equals(transactionId))
                .findFirst()
                .ifPresent(entry -> {
                    long tableId = entry.getKey();
                    deletions.remove(tableId, transactionId);
                    if (!deletions.containsKey(tableId)) {
                        unblockMaintenance(tableId);
                    }
                });
    }

    private void unblockMaintenance(long tableId)
    {
        try {
            dao.unblockMaintenance(tableId);
        }
        catch (Throwable t) {
            log.warn(t, "Failed to unblock maintenance for table ID %s, will retry", tableId);
            unblockMaintenanceExecutor.schedule(() -> unblockMaintenance(tableId), 2, SECONDS);
        }
    }
}
