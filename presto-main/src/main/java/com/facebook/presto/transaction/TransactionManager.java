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
package com.facebook.presto.transaction;

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.StandardErrorCode.AUTOCOMMIT_WRITE_CONFLICT;
import static com.facebook.presto.spi.StandardErrorCode.MULTI_CATALOG_WRITE_CONFLICT;
import static com.facebook.presto.spi.StandardErrorCode.READ_ONLY_VIOLATION;
import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_ALREADY_ABORTED;
import static com.facebook.presto.spi.StandardErrorCode.UNKNOWN_TRANSACTION;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.allAsList;
import static io.airlift.concurrent.MoreFutures.failedFuture;
import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

@ThreadSafe
public class TransactionManager
{
    private static final Logger log = Logger.get(TransactionManager.class);
    public static final IsolationLevel DEFAULT_ISOLATION = IsolationLevel.READ_UNCOMMITTED;
    public static final boolean DEFAULT_READ_ONLY = false;

    private final Duration idleTimeout;
    private final int maxFinishingConcurrency;

    private final ConcurrentMap<ConnectorId, Connector> connectorsById = new ConcurrentHashMap<>();
    private final ConcurrentMap<TransactionId, TransactionMetadata> transactions = new ConcurrentHashMap<>();
    private final Executor finishingExecutor;

    private TransactionManager(Duration idleTimeout, int maxFinishingConcurrency, Executor finishingExecutor)
    {
        requireNonNull(idleTimeout, "idleTimeout is null");
        checkArgument(maxFinishingConcurrency > 0, "maxFinishingConcurrency must be at least 1");
        requireNonNull(finishingExecutor, "finishingExecutor is null");

        this.idleTimeout = idleTimeout;
        this.maxFinishingConcurrency = maxFinishingConcurrency;
        this.finishingExecutor = finishingExecutor;
    }

    public static TransactionManager create(
            TransactionManagerConfig config,
            ScheduledExecutorService idleCheckExecutor,
            ExecutorService finishingExecutor)
    {
        TransactionManager transactionManager = new TransactionManager(config.getIdleTimeout(), config.getMaxFinishingConcurrency(), finishingExecutor);
        transactionManager.scheduleIdleChecks(config.getIdleCheckInterval(), idleCheckExecutor);
        return transactionManager;
    }

    public static TransactionManager createTestTransactionManager()
    {
        // No idle checks needed
        return new TransactionManager(new Duration(1, TimeUnit.DAYS), 1, directExecutor());
    }

    private void scheduleIdleChecks(Duration idleCheckInterval, ScheduledExecutorService idleCheckExecutor)
    {
        idleCheckExecutor.scheduleWithFixedDelay(() -> {
            try {
                cleanUpExpiredTransactions();
            }
            catch (Throwable t) {
                log.error(t, "Unexpected exception while cleaning up expired transactions");
            }
        }, idleCheckInterval.toMillis(), idleCheckInterval.toMillis(), MILLISECONDS);
    }

    private synchronized void cleanUpExpiredTransactions()
    {
        Iterator<Map.Entry<TransactionId, TransactionMetadata>> iterator = transactions.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TransactionId, TransactionMetadata> entry = iterator.next();
            if (entry.getValue().isExpired(idleTimeout)) {
                iterator.remove();
                log.info("Removing expired transaction: %s", entry.getKey());
                entry.getValue().asyncAbort();
            }
        }
    }

    public void addConnector(ConnectorId connectorId, Connector connector)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(connector, "connector is null");
        checkArgument(connectorsById.put(connectorId, connector) == null, "Connector '%s' is already registered", connectorId);
    }

    public TransactionInfo getTransactionInfo(TransactionId transactionId)
    {
        return getTransactionMetadata(transactionId).getTransactionInfo();
    }

    public List<TransactionInfo> getAllTransactionInfos()
    {
        return transactions.values().stream()
                .map(TransactionMetadata::getTransactionInfo)
                .collect(toImmutableList());
    }

    public TransactionId beginTransaction(boolean autoCommitContext)
    {
        return beginTransaction(DEFAULT_ISOLATION, DEFAULT_READ_ONLY, autoCommitContext);
    }

    public TransactionId beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommitContext)
    {
        TransactionId transactionId = TransactionId.create();
        BoundedExecutor executor = new BoundedExecutor(finishingExecutor, maxFinishingConcurrency);
        TransactionMetadata transactionMetadata = new TransactionMetadata(transactionId, isolationLevel, readOnly, autoCommitContext, executor);
        checkState(transactions.put(transactionId, transactionMetadata) == null, "Duplicate transaction ID: %s", transactionId);
        return transactionId;
    }

    public ConnectorMetadata getMetadata(TransactionId transactionId, ConnectorId connectorId)
    {
        TransactionMetadata transactionMetadata = getTransactionMetadata(transactionId);
        Connector connector = getConnector(connectorId);
        return transactionMetadata.getConnectorTransactionMetadata(connectorId, connector).getConnectorMetadata();
    }

    public ConnectorTransactionHandle getConnectorTransaction(TransactionId transactionId, ConnectorId connectorId)
    {
        TransactionMetadata transactionMetadata = getTransactionMetadata(transactionId);
        Connector connector = getConnector(connectorId);
        return transactionMetadata.getConnectorTransactionMetadata(connectorId, connector).getTransactionHandle();
    }

    public void checkConnectorWrite(TransactionId transactionId, ConnectorId connectorId)
    {
        getTransactionMetadata(transactionId).checkConnectorWrite(connectorId);
    }

    public void checkAndSetActive(TransactionId transactionId)
    {
        TransactionMetadata metadata = getTransactionMetadata(transactionId);
        metadata.checkOpenTransaction();
        metadata.setActive();
    }

    public void trySetActive(TransactionId transactionId)
    {
        tryGetTransactionMetadata(transactionId).ifPresent(TransactionMetadata::setActive);
    }

    public void trySetInactive(TransactionId transactionId)
    {
        tryGetTransactionMetadata(transactionId).ifPresent(TransactionMetadata::setInActive);
    }

    private Connector getConnector(ConnectorId connectorId)
    {
        Connector connector = connectorsById.get(connectorId);
        checkArgument(connector != null, "Unknown connector ID: %s", connectorId);
        return connector;
    }

    private TransactionMetadata getTransactionMetadata(TransactionId transactionId)
    {
        TransactionMetadata transactionMetadata = transactions.get(transactionId);
        if (transactionMetadata == null) {
            throw unknownTransactionError(transactionId);
        }
        return transactionMetadata;
    }

    private Optional<TransactionMetadata> tryGetTransactionMetadata(TransactionId transactionId)
    {
        return Optional.ofNullable(transactions.get(transactionId));
    }

    private CompletableFuture<TransactionMetadata> removeTransactionMetadataAsFuture(TransactionId transactionId)
    {
        TransactionMetadata transactionMetadata = transactions.remove(transactionId);
        if (transactionMetadata == null) {
            return failedFuture(unknownTransactionError(transactionId));
        }
        return completedFuture(transactionMetadata);
    }

    private static PrestoException unknownTransactionError(TransactionId transactionId)
    {
        return new PrestoException(UNKNOWN_TRANSACTION, format("Unknown transaction ID: %s. Possibly expired? Commands ignored until end of transaction block", transactionId));
    }

    public CompletableFuture<?> asyncCommit(TransactionId transactionId)
    {
        return unmodifiableFuture(removeTransactionMetadataAsFuture(transactionId)
                .thenCompose(metadata -> metadata.asyncCommit()));
    }

    public CompletableFuture<?> asyncAbort(TransactionId transactionId)
    {
        return unmodifiableFuture(removeTransactionMetadataAsFuture(transactionId)
                .thenCompose(metadata -> metadata.asyncAbort()));
    }

    public void fail(TransactionId transactionId)
    {
        // Mark transaction as failed, but don't remove it.
        tryGetTransactionMetadata(transactionId).ifPresent(TransactionMetadata::asyncAbort);
    }

    @ThreadSafe
    private static class TransactionMetadata
    {
        private final DateTime createTime = DateTime.now();
        private final TransactionId transactionId;
        private final IsolationLevel isolationLevel;
        private final boolean readOnly;
        private final boolean autoCommitContext;
        private final Map<ConnectorId, ConnectorTransactionMetadata> connectorIdToMetadata = new ConcurrentHashMap<>();
        private final AtomicReference<ConnectorId> writtenConnectorId = new AtomicReference<>();
        private final Executor finishingExecutor;
        private final AtomicReference<Boolean> completedSuccessfully = new AtomicReference<>();
        private final AtomicReference<Long> idleStartTime = new AtomicReference<>();

        public TransactionMetadata(TransactionId transactionId, IsolationLevel isolationLevel, boolean readOnly, boolean autoCommitContext, Executor finishingExecutor)
        {
            this.transactionId = requireNonNull(transactionId, "transactionId is null");
            this.isolationLevel = requireNonNull(isolationLevel, "isolationLevel is null");
            this.readOnly = readOnly;
            this.autoCommitContext = autoCommitContext;
            this.finishingExecutor = requireNonNull(finishingExecutor, "finishingExecutor is null");
        }

        public void setActive()
        {
            idleStartTime.set(null);
        }

        public void setInActive()
        {
            idleStartTime.set(System.nanoTime());
        }

        public boolean isExpired(Duration idleTimeout)
        {
            Long idleStartTime = this.idleStartTime.get();
            return idleStartTime != null && Duration.nanosSince(idleStartTime).compareTo(idleTimeout) > 0;
        }

        public void checkOpenTransaction()
        {
            Boolean completedStatus = this.completedSuccessfully.get();
            if (completedStatus != null) {
                if (completedStatus) {
                    // Should not happen normally
                    throw new IllegalStateException("Current transaction already committed");
                }
                else {
                    throw new PrestoException(TRANSACTION_ALREADY_ABORTED, "Current transaction is aborted, commands ignored until end of transaction block");
                }
            }
        }

        public synchronized ConnectorTransactionMetadata getConnectorTransactionMetadata(ConnectorId connectorId, Connector connector)
        {
            checkOpenTransaction();
            ConnectorTransactionMetadata transactionMetadata = connectorIdToMetadata.get(connectorId);
            if (transactionMetadata == null) {
                transactionMetadata = new ConnectorTransactionMetadata(connector, beginTransaction(connector));
                // Don't use computeIfAbsent b/c the beginTransaction call might be recursive
                checkState(connectorIdToMetadata.put(connectorId, transactionMetadata) == null);
            }
            return transactionMetadata;
        }

        private ConnectorTransactionHandle beginTransaction(Connector connector)
        {
            if (connector instanceof InternalConnector) {
                return ((InternalConnector) connector).beginTransaction(transactionId, isolationLevel, readOnly);
            }
            else {
                return connector.beginTransaction(isolationLevel, readOnly);
            }
        }

        public synchronized void checkConnectorWrite(ConnectorId connectorId)
        {
            checkOpenTransaction();
            ConnectorTransactionMetadata transactionMetadata = connectorIdToMetadata.get(connectorId);
            checkArgument(transactionMetadata != null, "Cannot record write for connector not part of transaction");
            if (readOnly) {
                throw new PrestoException(READ_ONLY_VIOLATION, "Cannot execute write in a read-only transaction");
            }
            if (!writtenConnectorId.compareAndSet(null, connectorId) && !writtenConnectorId.get().equals(connectorId)) {
                throw new PrestoException(MULTI_CATALOG_WRITE_CONFLICT, "Multi-catalog writes not supported in a single transaction. Already wrote to catalog " + writtenConnectorId.get());
            }
            if (transactionMetadata.isSingleStatementWritesOnly() && !autoCommitContext) {
                throw new PrestoException(AUTOCOMMIT_WRITE_CONFLICT, "Catalog " + connectorId + " only supports writes using autocommit");
            }
        }

        public synchronized CompletableFuture<?> asyncCommit()
        {
            if (!completedSuccessfully.compareAndSet(null, true)) {
                if (completedSuccessfully.get()) {
                    // Already done
                    return completedFuture(null);
                }
                // Transaction already aborted
                return failedFuture(new PrestoException(TRANSACTION_ALREADY_ABORTED, "Current transaction has already been aborted"));
            }

            ConnectorId writeConnectorId = this.writtenConnectorId.get();
            if (writeConnectorId == null) {
                List<CompletableFuture<?>> futures = connectorIdToMetadata.values().stream()
                        .map(transactionMetadata -> runAsync(transactionMetadata::commit, finishingExecutor))
                        .collect(toList());
                return unmodifiableFuture(allAsList(futures)
                        .whenComplete((value, throwable) -> {
                            if (throwable != null) {
                                abortInternal();
                                log.error(throwable, "Read-only connector should not throw exception on commit");
                            }
                        }));
            }

            Supplier<CompletableFuture<?>> commitReadOnlyConnectors = () -> allAsList(connectorIdToMetadata.entrySet().stream()
                    .filter(entry -> !entry.getKey().equals(writeConnectorId))
                    .map(Map.Entry::getValue)
                    .map(transactionMetadata -> runAsync(transactionMetadata::commit, finishingExecutor))
                    .collect(toList()))
                    .whenComplete((value, throwable) -> {
                        if (throwable != null) {
                            log.error(throwable, "Read-only connector should not throw exception on commit");
                        }
                    });

            ConnectorTransactionMetadata writeConnector = connectorIdToMetadata.get(writeConnectorId);
            return unmodifiableFuture(runAsync(writeConnector::commit, finishingExecutor)
                    .thenCompose(aVoid -> commitReadOnlyConnectors.get())
                    .whenComplete((value, throwable) -> {
                        if (throwable != null) {
                            abortInternal();
                        }
                    }));
        }

        public synchronized CompletableFuture<?> asyncAbort()
        {
            if (!completedSuccessfully.compareAndSet(null, false)) {
                if (completedSuccessfully.get()) {
                    // Should not happen normally
                    return failedFuture(new IllegalStateException("Current transaction already committed"));
                }
                // Already done
                return completedFuture(null);
            }
            return abortInternal();
        }

        private CompletableFuture<?> abortInternal()
        {
            CompletableFuture<List<Void>> futures = allAsList(connectorIdToMetadata.values().stream()
                    .map(connection -> runAsync(() -> safeAbort(connection), finishingExecutor))
                    .collect(toList()));
            return unmodifiableFuture(futures);
        }

        private static void safeAbort(ConnectorTransactionMetadata connection)
        {
            try {
                connection.abort();
            }
            catch (Exception e) {
                log.error(e, "Connector threw exception on abort");
            }
        }

        public TransactionInfo getTransactionInfo()
        {
            Duration idleTime = Optional.ofNullable(idleStartTime.get())
                    .map(Duration::nanosSince)
                    .orElse(new Duration(0, MILLISECONDS));
            Optional<ConnectorId> writtenConnectorId = Optional.ofNullable(this.writtenConnectorId.get());
            List<ConnectorId> connectorIds = ImmutableList.copyOf(connectorIdToMetadata.keySet());
            return new TransactionInfo(transactionId, isolationLevel, readOnly, autoCommitContext, createTime, idleTime, connectorIds, writtenConnectorId);
        }

        private static class ConnectorTransactionMetadata
        {
            private final Connector connector;
            private final ConnectorTransactionHandle transactionHandle;
            private final Supplier<ConnectorMetadata> connectorMetadataSupplier;
            private final AtomicBoolean finished = new AtomicBoolean();

            public ConnectorTransactionMetadata(Connector connector, ConnectorTransactionHandle transactionHandle)
            {
                this.connector = requireNonNull(connector, "connector is null");
                this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
                this.connectorMetadataSupplier = Suppliers.memoize(() -> connector.getMetadata(transactionHandle));
            }

            public boolean isSingleStatementWritesOnly()
            {
                return connector.isSingleStatementWritesOnly();
            }

            public synchronized ConnectorMetadata getConnectorMetadata()
            {
                checkState(!finished.get(), "Already finished");
                return connectorMetadataSupplier.get();
            }

            public ConnectorTransactionHandle getTransactionHandle()
            {
                checkState(!finished.get(), "Already finished");
                return transactionHandle;
            }

            public void commit()
            {
                if (finished.compareAndSet(false, true)) {
                    connector.commit(transactionHandle);
                }
            }

            public void abort()
            {
                if (finished.compareAndSet(false, true)) {
                    connector.rollback(transactionHandle);
                }
            }
        }
    }
}
