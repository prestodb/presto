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

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.concurrent.ExecutorServiceAdapter;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.CatalogMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.facebook.airlift.concurrent.MoreFutures.addExceptionCallback;
import static com.facebook.presto.spi.StandardErrorCode.AUTOCOMMIT_WRITE_CONFLICT;
import static com.facebook.presto.spi.StandardErrorCode.MULTI_CATALOG_WRITE_CONFLICT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.READ_ONLY_VIOLATION;
import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_ALREADY_ABORTED;
import static com.facebook.presto.spi.StandardErrorCode.UNKNOWN_TRANSACTION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

@ThreadSafe
public class InMemoryTransactionManager
        implements TransactionManager
{
    private static final Logger log = Logger.get(InMemoryTransactionManager.class);

    private final Duration idleTimeout;
    private final int maxFinishingConcurrency;

    private final ConcurrentMap<TransactionId, TransactionMetadata> transactions = new ConcurrentHashMap<>();
    private final CatalogManager catalogManager;
    private final Executor finishingExecutor;

    private final Map<String, FunctionNamespaceManager<?>> functionNamespaceManagers = new HashMap<>();
    private final Map<String, String> companionCatalogs;

    private InMemoryTransactionManager(Duration idleTimeout, int maxFinishingConcurrency, CatalogManager catalogManager, Executor finishingExecutor, Map<String, String> companionCatalogs)
    {
        this.catalogManager = catalogManager;
        requireNonNull(idleTimeout, "idleTimeout is null");
        checkArgument(maxFinishingConcurrency > 0, "maxFinishingConcurrency must be at least 1");
        requireNonNull(finishingExecutor, "finishingExecutor is null");

        this.idleTimeout = idleTimeout;
        this.maxFinishingConcurrency = maxFinishingConcurrency;
        this.finishingExecutor = finishingExecutor;
        this.companionCatalogs = ImmutableMap.copyOf(companionCatalogs);
    }

    public static TransactionManager create(
            TransactionManagerConfig config,
            ScheduledExecutorService idleCheckExecutor,
            CatalogManager catalogManager,
            ExecutorService finishingExecutor)
    {
        InMemoryTransactionManager transactionManager = new InMemoryTransactionManager(config.getIdleTimeout(), config.getMaxFinishingConcurrency(), catalogManager, finishingExecutor, config.getCompanionCatalogs());
        transactionManager.scheduleIdleChecks(config.getIdleCheckInterval(), idleCheckExecutor);
        return transactionManager;
    }

    public static TransactionManager createTestTransactionManager()
    {
        return createTestTransactionManager(new CatalogManager());
    }

    public static TransactionManager createTestTransactionManager(CatalogManager catalogManager)
    {
        // No idle checks needed
        return new InMemoryTransactionManager(new Duration(1, TimeUnit.DAYS), 1, catalogManager, directExecutor(), ImmutableMap.of());
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

    synchronized void cleanUpExpiredTransactions()
    {
        Iterator<Entry<TransactionId, TransactionMetadata>> iterator = transactions.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<TransactionId, TransactionMetadata> entry = iterator.next();
            if (entry.getValue().isExpired(idleTimeout)) {
                iterator.remove();
                log.info("Removing expired transaction: %s", entry.getKey());
                entry.getValue().asyncAbort();
            }
        }
    }

    @Override
    public TransactionInfo getTransactionInfo(TransactionId transactionId)
    {
        return getTransactionMetadata(transactionId).getTransactionInfo();
    }

    @Override
    public Optional<TransactionInfo> getOptionalTransactionInfo(TransactionId transactionId)
    {
        return tryGetTransactionMetadata(transactionId).map(TransactionMetadata::getTransactionInfo);
    }

    @Override
    public List<TransactionInfo> getAllTransactionInfos()
    {
        return transactions.values().stream()
                .map(TransactionMetadata::getTransactionInfo)
                .collect(toImmutableList());
    }

    @Override
    public TransactionId beginTransaction(boolean autoCommitContext)
    {
        return beginTransaction(DEFAULT_ISOLATION, DEFAULT_READ_ONLY, autoCommitContext);
    }

    @Override
    public TransactionId beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommitContext)
    {
        TransactionId transactionId = TransactionId.create();
        BoundedExecutor executor = new BoundedExecutor(finishingExecutor, maxFinishingConcurrency);
        TransactionMetadata transactionMetadata = new TransactionMetadata(transactionId, isolationLevel, readOnly, autoCommitContext, catalogManager, executor, functionNamespaceManagers, companionCatalogs);
        checkState(transactions.put(transactionId, transactionMetadata) == null, "Duplicate transaction ID: %s", transactionId);
        return transactionId;
    }

    @Override
    public Map<String, ConnectorId> getCatalogNames(TransactionId transactionId)
    {
        return getTransactionMetadata(transactionId).getCatalogNames();
    }

    @Override
    public Optional<CatalogMetadata> getOptionalCatalogMetadata(TransactionId transactionId, String catalogName)
    {
        TransactionMetadata transactionMetadata = getTransactionMetadata(transactionId);
        return transactionMetadata.getConnectorId(catalogName)
                .map(transactionMetadata::getTransactionCatalogMetadata);
    }

    @Override
    public CatalogMetadata getCatalogMetadata(TransactionId transactionId, ConnectorId connectorId)
    {
        return getTransactionMetadata(transactionId).getTransactionCatalogMetadata(connectorId);
    }

    @Override
    public CatalogMetadata getCatalogMetadataForWrite(TransactionId transactionId, ConnectorId connectorId)
    {
        CatalogMetadata catalogMetadata = getCatalogMetadata(transactionId, connectorId);
        checkConnectorWrite(transactionId, connectorId);
        return catalogMetadata;
    }

    @Override
    public CatalogMetadata getCatalogMetadataForWrite(TransactionId transactionId, String catalogName)
    {
        TransactionMetadata transactionMetadata = getTransactionMetadata(transactionId);

        // there is no need to ask for a connector specific id since the overlay connectors are read only
        ConnectorId connectorId = transactionMetadata.getConnectorId(catalogName)
                .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog does not exist: " + catalogName));

        return getCatalogMetadataForWrite(transactionId, connectorId);
    }

    @Override
    public ConnectorTransactionHandle getConnectorTransaction(TransactionId transactionId, ConnectorId connectorId)
    {
        return getCatalogMetadata(transactionId, connectorId).getTransactionHandleFor(connectorId);
    }

    @Override
    public synchronized void registerFunctionNamespaceManager(String catalogName, FunctionNamespaceManager<?> functionNamespaceManager)
    {
        checkArgument(!functionNamespaceManagers.containsKey(catalogName), "FunctionNamespaceManager is already registered for catalog [%s]", catalogName);
        functionNamespaceManagers.put(catalogName, functionNamespaceManager);
    }

    @Override
    public FunctionNamespaceTransactionHandle getFunctionNamespaceTransaction(TransactionId transactionId, String catalogName)
    {
        return getTransactionMetadata(transactionId).getFunctionNamespaceTransaction(catalogName).getTransactionHandle();
    }

    private void checkConnectorWrite(TransactionId transactionId, ConnectorId connectorId)
    {
        getTransactionMetadata(transactionId).checkConnectorWrite(connectorId);
    }

    @Override
    public void checkAndSetActive(TransactionId transactionId)
    {
        TransactionMetadata metadata = getTransactionMetadata(transactionId);
        metadata.checkOpenTransaction();
        metadata.setActive();
    }

    @Override
    public void trySetActive(TransactionId transactionId)
    {
        tryGetTransactionMetadata(transactionId).ifPresent(TransactionMetadata::setActive);
    }

    @Override
    public void trySetInactive(TransactionId transactionId)
    {
        tryGetTransactionMetadata(transactionId).ifPresent(TransactionMetadata::setInactive);
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

    private ListenableFuture<TransactionMetadata> removeTransactionMetadataAsFuture(TransactionId transactionId)
    {
        TransactionMetadata transactionMetadata = transactions.remove(transactionId);
        if (transactionMetadata == null) {
            return immediateFailedFuture(unknownTransactionError(transactionId));
        }
        return immediateFuture(transactionMetadata);
    }

    private static PrestoException unknownTransactionError(TransactionId transactionId)
    {
        return new PrestoException(UNKNOWN_TRANSACTION, format("Unknown transaction ID: %s. Possibly expired? Commands ignored until end of transaction block", transactionId));
    }

    @Override
    public ListenableFuture<?> asyncCommit(TransactionId transactionId)
    {
        return nonCancellationPropagating(Futures.transformAsync(removeTransactionMetadataAsFuture(transactionId), TransactionMetadata::asyncCommit, directExecutor()));
    }

    @Override
    public ListenableFuture<?> asyncAbort(TransactionId transactionId)
    {
        return nonCancellationPropagating(Futures.transformAsync(removeTransactionMetadataAsFuture(transactionId), TransactionMetadata::asyncAbort, directExecutor()));
    }

    @Override
    public void fail(TransactionId transactionId)
    {
        // Mark transaction as failed, but don't remove it.
        tryGetTransactionMetadata(transactionId).ifPresent(TransactionMetadata::asyncAbort);
    }

    @ThreadSafe
    private static class TransactionMetadata
    {
        private final DateTime createTime = DateTime.now();
        private final CatalogManager catalogManager;
        private final TransactionId transactionId;
        private final IsolationLevel isolationLevel;
        private final boolean readOnly;
        private final boolean autoCommitContext;
        @GuardedBy("this")
        private final Map<ConnectorId, ConnectorTransactionMetadata> connectorIdToMetadata = new ConcurrentHashMap<>();
        @GuardedBy("this")
        private final AtomicReference<ConnectorId> writtenConnectorId = new AtomicReference<>();
        private final ListeningExecutorService finishingExecutor;
        private final AtomicReference<Boolean> completedSuccessfully = new AtomicReference<>();
        private final AtomicReference<Long> idleStartTime = new AtomicReference<>();

        @GuardedBy("this")
        private final Map<String, Optional<Catalog>> catalogByName = new ConcurrentHashMap<>();
        @GuardedBy("this")
        private final Map<ConnectorId, Catalog> catalogsByConnectorId = new ConcurrentHashMap<>();
        @GuardedBy("this")
        private final Map<ConnectorId, CatalogMetadata> catalogMetadata = new ConcurrentHashMap<>();

        private final Map<String, FunctionNamespaceManager<?>> functionNamespaceManagers;
        @GuardedBy("this")
        private final Map<String, FunctionNamespaceTransactionMetadata> functionNamespaceTransactions = new ConcurrentHashMap<>();
        private final Map<String, String> companionCatalogs;

        public TransactionMetadata(
                TransactionId transactionId,
                IsolationLevel isolationLevel,
                boolean readOnly,
                boolean autoCommitContext,
                CatalogManager catalogManager,
                Executor finishingExecutor,
                Map<String, FunctionNamespaceManager<?>> functionNamespaceManagers,
                Map<String, String> companionCatalogs)
        {
            this.transactionId = requireNonNull(transactionId, "transactionId is null");
            this.isolationLevel = requireNonNull(isolationLevel, "isolationLevel is null");
            this.readOnly = readOnly;
            this.autoCommitContext = autoCommitContext;
            this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
            this.finishingExecutor = listeningDecorator(ExecutorServiceAdapter.from(requireNonNull(finishingExecutor, "finishingExecutor is null")));
            this.functionNamespaceManagers = requireNonNull(functionNamespaceManagers, "functionNamespaceManagers is null");
            this.companionCatalogs = requireNonNull(companionCatalogs, "companionCatalogs is null");
        }

        public void setActive()
        {
            idleStartTime.set(null);
        }

        public void setInactive()
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

        private synchronized Map<String, ConnectorId> getCatalogNames()
        {
            // todo if repeatable read, this must be recorded
            Map<String, ConnectorId> catalogNames = new HashMap<>();
            catalogByName.values().stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(catalog -> catalogNames.put(catalog.getCatalogName(), catalog.getConnectorId()));

            catalogManager.getCatalogs().stream()
                    .forEach(catalog -> catalogNames.putIfAbsent(catalog.getCatalogName(), catalog.getConnectorId()));

            return ImmutableMap.copyOf(catalogNames);
        }

        private synchronized Optional<ConnectorId> getConnectorId(String catalogName)
        {
            Optional<Catalog> catalog = catalogByName.get(catalogName);
            if (catalog == null) {
                catalog = catalogManager.getCatalog(catalogName);
                catalogByName.put(catalogName, catalog);
                if (catalog.isPresent()) {
                    registerCatalog(catalog.get());
                }

                if (companionCatalogs.containsKey(catalogName)) {
                    Optional<Catalog> companionCatalog = catalogManager.getCatalog(companionCatalogs.get(catalogName));
                    checkArgument(companionCatalog.isPresent(), format("Invalid config, no catalog exists for catalog name %s: %s", catalogName, companionCatalogs.get(catalogName)));
                    registerCatalog(companionCatalog.get());
                }
            }
            return catalog.map(Catalog::getConnectorId);
        }

        private synchronized void registerCatalog(Catalog catalog)
        {
            catalogsByConnectorId.put(catalog.getConnectorId(), catalog);
            catalogsByConnectorId.put(catalog.getInformationSchemaId(), catalog);
            catalogsByConnectorId.put(catalog.getSystemTablesId(), catalog);
        }

        private synchronized CatalogMetadata getTransactionCatalogMetadata(ConnectorId connectorId)
        {
            checkOpenTransaction();

            CatalogMetadata catalogMetadata = this.catalogMetadata.get(connectorId);
            if (catalogMetadata == null) {
                Catalog catalog = catalogsByConnectorId.get(connectorId);
                verify(catalog != null, "Unknown connectorId: %s", connectorId);
                Connector connector = catalog.getConnector(connectorId);

                ConnectorTransactionMetadata metadata = createConnectorTransactionMetadata(catalog.getConnectorId(), catalog);
                ConnectorTransactionMetadata informationSchema = createConnectorTransactionMetadata(catalog.getInformationSchemaId(), catalog);
                ConnectorTransactionMetadata systemTables = createConnectorTransactionMetadata(catalog.getSystemTablesId(), catalog);

                catalogMetadata = new CatalogMetadata(
                        metadata.getConnectorId(),
                        metadata.getConnectorMetadata(),
                        metadata.getTransactionHandle(),
                        informationSchema.getConnectorId(),
                        informationSchema.getConnectorMetadata(),
                        informationSchema.getTransactionHandle(),
                        systemTables.getConnectorId(),
                        systemTables.getConnectorMetadata(),
                        systemTables.getTransactionHandle(),
                        connector.getCapabilities());

                this.catalogMetadata.put(catalog.getConnectorId(), catalogMetadata);
                this.catalogMetadata.put(catalog.getInformationSchemaId(), catalogMetadata);
                this.catalogMetadata.put(catalog.getSystemTablesId(), catalogMetadata);
            }
            return catalogMetadata;
        }

        private synchronized FunctionNamespaceTransactionMetadata getFunctionNamespaceTransaction(String catalogName)
        {
            checkOpenTransaction();

            return functionNamespaceTransactions.computeIfAbsent(
                    catalogName, catalog -> {
                        verify(catalog != null, "catalog is null");
                        FunctionNamespaceManager<?> functionNamespaceManager = functionNamespaceManagers.get(catalog);
                        FunctionNamespaceTransactionHandle transactionHandle = functionNamespaceManager.beginTransaction();
                        return new FunctionNamespaceTransactionMetadata(functionNamespaceManager, transactionHandle);
                    });
        }

        public synchronized ConnectorTransactionMetadata createConnectorTransactionMetadata(ConnectorId connectorId, Catalog catalog)
        {
            Connector connector = catalog.getConnector(connectorId);
            ConnectorTransactionMetadata transactionMetadata = new ConnectorTransactionMetadata(connectorId, connector, beginTransaction(connector));
            checkState(connectorIdToMetadata.put(connectorId, transactionMetadata) == null);
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

        public synchronized ListenableFuture<?> asyncCommit()
        {
            if (!completedSuccessfully.compareAndSet(null, true)) {
                if (completedSuccessfully.get()) {
                    // Already done
                    return immediateFuture(null);
                }
                // Transaction already aborted
                return immediateFailedFuture(new PrestoException(TRANSACTION_ALREADY_ABORTED, "Current transaction has already been aborted"));
            }

            ListenableFuture<?> functionNamespaceFuture = Futures.allAsList(functionNamespaceTransactions.values().stream()
                    .map(transactionMetadata -> finishingExecutor.submit(transactionMetadata::commit))
                    .collect(toImmutableList()));

            ConnectorId writeConnectorId = this.writtenConnectorId.get();
            if (writeConnectorId == null) {
                Supplier<ListenableFuture<?>> commitReadOnlyConnectors = () -> {
                    ListenableFuture<? extends List<?>> future = Futures.allAsList(connectorIdToMetadata.values().stream()
                            .map(transactionMetadata -> finishingExecutor.submit(transactionMetadata::commit))
                            .collect(toList()));
                    addExceptionCallback(future, throwable -> log.error(throwable, "Read-only connector should not throw exception on commit"));
                    return future;
                };

                ListenableFuture<?> readOnlyCommitFuture = Futures.transformAsync(functionNamespaceFuture, ignored -> commitReadOnlyConnectors.get(), directExecutor());
                addExceptionCallback(readOnlyCommitFuture, this::abortInternal);
                return nonCancellationPropagating(readOnlyCommitFuture);
            }

            Supplier<ListenableFuture<?>> commitReadOnlyConnectors = () -> {
                ListenableFuture<? extends List<?>> future = Futures.allAsList(connectorIdToMetadata.entrySet().stream()
                        .filter(entry -> !entry.getKey().equals(writeConnectorId))
                        .map(Entry::getValue)
                        .map(transactionMetadata -> finishingExecutor.submit(transactionMetadata::commit))
                        .collect(toList()));
                addExceptionCallback(future, throwable -> log.error(throwable, "Read-only connector should not throw exception on commit"));
                return future;
            };

            ConnectorTransactionMetadata writeConnector = connectorIdToMetadata.get(writeConnectorId);
            Supplier<ListenableFuture> commitFunctionNamespaceTransactions = () -> functionNamespaceFuture;
            ListenableFuture<?> commitFuture = Futures.transformAsync(finishingExecutor.submit(writeConnector::commit), ignored -> commitFunctionNamespaceTransactions.get(), directExecutor());
            ListenableFuture<?> readOnlyCommitFuture = Futures.transformAsync(commitFuture, ignored -> commitReadOnlyConnectors.get(), directExecutor());
            addExceptionCallback(readOnlyCommitFuture, this::abortInternal);

            return nonCancellationPropagating(readOnlyCommitFuture);
        }

        public synchronized ListenableFuture<?> asyncAbort()
        {
            if (!completedSuccessfully.compareAndSet(null, false)) {
                if (completedSuccessfully.get()) {
                    // Should not happen normally
                    return immediateFailedFuture(new IllegalStateException("Current transaction already committed"));
                }
                // Already done
                return immediateFuture(null);
            }
            return abortInternal();
        }

        private synchronized ListenableFuture<?> abortInternal()
        {
            // the callbacks in statement performed on another thread so are safe
            List<ListenableFuture<?>> abortFutures = Stream.concat(
                    functionNamespaceTransactions.values().stream()
                            .map(transactionMetadata -> finishingExecutor.submit(() -> safeAbort(transactionMetadata))),
                    connectorIdToMetadata.values().stream()
                            .map(connection -> finishingExecutor.submit(() -> safeAbort(connection))))
                    .collect(toList());
            return nonCancellationPropagating(Futures.allAsList(abortFutures));
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

        private static void safeAbort(FunctionNamespaceTransactionMetadata transactionMetadata)
        {
            try {
                transactionMetadata.abort();
            }
            catch (Exception e) {
                log.error(e, "Function namespace transaction threw exception on abort");
            }
        }

        public TransactionInfo getTransactionInfo()
        {
            Duration idleTime = Optional.ofNullable(idleStartTime.get())
                    .map(Duration::nanosSince)
                    .orElse(new Duration(0, MILLISECONDS));

            // dereferencing this field is safe because the field is atomic
            @SuppressWarnings("FieldAccessNotGuarded") Optional<ConnectorId> writtenConnectorId = Optional.ofNullable(this.writtenConnectorId.get());

            // copying the key set is safe here because the map is concurrent
            @SuppressWarnings("FieldAccessNotGuarded") List<ConnectorId> connectorIds = ImmutableList.copyOf(connectorIdToMetadata.keySet());

            return new TransactionInfo(transactionId, isolationLevel, readOnly, autoCommitContext, createTime, idleTime, connectorIds, writtenConnectorId);
        }

        private static class ConnectorTransactionMetadata
        {
            private final ConnectorId connectorId;
            private final Connector connector;
            private final ConnectorTransactionHandle transactionHandle;
            private final ConnectorMetadata connectorMetadata;
            private final AtomicBoolean finished = new AtomicBoolean();

            public ConnectorTransactionMetadata(ConnectorId connectorId, Connector connector, ConnectorTransactionHandle transactionHandle)
            {
                this.connectorId = requireNonNull(connectorId, "connectorId is null");
                this.connector = requireNonNull(connector, "connector is null");
                this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
                this.connectorMetadata = connector.getMetadata(transactionHandle);
            }

            public ConnectorId getConnectorId()
            {
                return connectorId;
            }

            public boolean isSingleStatementWritesOnly()
            {
                return connector.isSingleStatementWritesOnly();
            }

            public synchronized ConnectorMetadata getConnectorMetadata()
            {
                checkState(!finished.get(), "Already finished");
                return connectorMetadata;
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

        private static class FunctionNamespaceTransactionMetadata
        {
            private final FunctionNamespaceManager<?> functionNamespaceManager;
            private final FunctionNamespaceTransactionHandle transactionHandle;
            private final AtomicBoolean finished = new AtomicBoolean();

            public FunctionNamespaceTransactionMetadata(FunctionNamespaceManager<?> functionNamespaceManager, FunctionNamespaceTransactionHandle transactionHandle)
            {
                this.functionNamespaceManager = requireNonNull(functionNamespaceManager, "functionNamespaceManager is null");
                this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
            }

            public FunctionNamespaceManager<?> getFunctionNamespaceManager()
            {
                return functionNamespaceManager;
            }

            public FunctionNamespaceTransactionHandle getTransactionHandle()
            {
                return transactionHandle;
            }

            public void commit()
            {
                if (finished.compareAndSet(false, true)) {
                    functionNamespaceManager.commit(transactionHandle);
                }
            }

            public void abort()
            {
                if (finished.compareAndSet(false, true)) {
                    functionNamespaceManager.abort(transactionHandle);
                }
            }
        }
    }
}
