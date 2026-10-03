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
package com.facebook.presto.iceberg.transaction;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.transaction.IsolationLevel;
import jakarta.annotation.Nullable;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataTableScan;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.UpdateStatistics;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.facebook.presto.iceberg.IcebergCommitFailures.toPrestoException;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_TRANSACTION_CONFLICT_ERROR;
import static com.facebook.presto.iceberg.IcebergUtil.opsFromTable;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterators.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.IcebergLibUtils.getScanContext;

public class IcebergTransactionContext
{
    private static final Logger log = Logger.get(IcebergTransactionContext.class);

    private final IsolationLevel isolationLevel;
    private final boolean autoCommitContext;
    private final Map<SchemaTableName, TableTransaction> txByTable;
    private final Map<SchemaTableName, Table> initiallyReadTables;
    private final AtomicReference<Runnable> callbacksOnCommit = new AtomicReference<>();

    public IcebergTransactionContext(IsolationLevel isolationLevel, boolean autoCommitContext)
    {
        this.isolationLevel = requireNonNull(isolationLevel, "isolationLevel is null");
        this.autoCommitContext = autoCommitContext;
        txByTable = new ConcurrentHashMap<>();
        initiallyReadTables = new ConcurrentHashMap<>();
    }

    public IsolationLevel getIsolationLevel()
    {
        return this.isolationLevel;
    }

    public boolean isAutoCommitContext()
    {
        return this.autoCommitContext;
    }

    public Optional<Table> getTransactionTable(SchemaTableName tableName)
    {
        return getTransaction(tableName).map(Transaction::table);
    }

    public Optional<Transaction> getTransaction(SchemaTableName tableName)
    {
        return Optional.ofNullable(txByTable.get(tableName)).map(TableTransaction::getTransaction);
    }

    public Optional<Table> initiallyReadTable(SchemaTableName tableName)
    {
        if (initiallyReadTables.containsKey(tableName)) {
            return Optional.ofNullable(initiallyReadTables.get(tableName));
        }

        return Optional.empty();
    }

    public void registerTransaction(SchemaTableName tableName, Transaction transaction)
    {
        if (txByTable.isEmpty()) {
            // the table this transaction creates does not exist yet, so there is no state to conflict with
            txByTable.put(tableName, new TableTransaction(transaction, null));
        }
        else if (!txByTable.containsKey(tableName)) {
            throw new PrestoException(ICEBERG_TRANSACTION_CONFLICT_ERROR, "Not allowed to open write transactions on different tables");
        }
    }

    public Table getIcebergTable(SchemaTableName schemaTableName, Function<SchemaTableName, Table> rawIcebergTableLoader)
    {
        Table table = getTransactionTable(schemaTableName)
                .orElseGet(() -> initiallyReadTable(schemaTableName)
                        .orElseGet(() -> {
                            Table loadTable = rawIcebergTableLoader.apply(schemaTableName);
                            initiallyReadTables.computeIfAbsent(schemaTableName, ignored -> loadTable);
                            return loadTable;
                        }));
        return new TransactionalTable(schemaTableName, table, opsFromTable(table));
    }

    public void registerCallback(Runnable callback)
    {
        checkArgument(this.callbacksOnCommit.get() == null, "Cannot set callbacksOnCommit multiple times");
        this.callbacksOnCommit.set(callback);
    }

    public void commit()
    {
        if (!txByTable.isEmpty()) {
            Map.Entry<SchemaTableName, TableTransaction> entry = getOnlyElement(txByTable.entrySet().iterator());
            TableTransaction tableTransaction = entry.getValue();
            Transaction transaction = tableTransaction.getTransaction();
            try {
                transaction.commitTransaction();
            }
            catch (RuntimeException e) {
                throw toPrestoException(e, entry.getKey(), tableTransaction::isTableChangedConcurrently);
            }
            if (callbacksOnCommit.get() != null) {
                callbacksOnCommit.get().run();
            }
            txByTable.clear();
        }
        initiallyReadTables.clear();
        callbacksOnCommit.set(null);
    }

    public void rollback()
    {
        txByTable.clear();
        initiallyReadTables.clear();
    }

    /**
     * We're using a {@link Transaction} per table so that we can keep track of pending changes for a
     * particular table.
     */
    private Transaction txForTable(SchemaTableName tableName, Table table)
    {
        if (!txByTable.isEmpty() && !txByTable.containsKey(tableName)) {
            throw new PrestoException(ICEBERG_TRANSACTION_CONFLICT_ERROR, "Not allowed to open write transactions on multiple tables");
        }

        return txByTable.computeIfAbsent(
                tableName,
                k -> {
                    TableOperations operations = ((HasTableOperations) table).operations();
                    Transaction transaction = Transactions.newTransaction(table.name(), operations);
                    return new TableTransaction(transaction, operations);
                })
                .getTransaction();
    }

    /**
     * An Iceberg {@link Transaction} together with the state needed to tell, once its commit is
     * rejected, whether the rejection was caused by another writer committing to the same table.
     */
    private static class TableTransaction
    {
        private final Transaction transaction;
        @Nullable
        private final TableOperations tableOperations;
        private final OptionalLong tableBaseSnapshotId;

        TableTransaction(Transaction transaction, @Nullable TableOperations tableOperations)
        {
            this.transaction = requireNonNull(transaction, "transaction is null");
            this.tableOperations = tableOperations;
            this.tableBaseSnapshotId = tableOperations == null ? OptionalLong.empty() : getCurrentSnapshotId(tableOperations.current());
        }

        Transaction getTransaction()
        {
            return transaction;
        }

        boolean isTableChangedConcurrently()
        {
            if (tableOperations == null) {
                return false;
            }

            try {
                return !getCurrentSnapshotId(tableOperations.refresh()).equals(tableBaseSnapshotId);
            }
            catch (RuntimeException e) {
                // Without knowing the current state of the table, it’s impossible to distinguish a conflict from a deterministic failure.
                // We assume the commit lost a race, so that the more common case stays retriable, a table that
                // cannot be read will fail the retry right away anyway.
                log.debug(e, "Could not refresh %s to tell whether its commit hit a concurrent modification", transaction.table().name());
                return true;
            }
        }

        private static OptionalLong getCurrentSnapshotId(TableMetadata metadata)
        {
            Snapshot snapshot = metadata.currentSnapshot();
            return snapshot == null ? OptionalLong.empty() : OptionalLong.of(snapshot.snapshotId());
        }
    }

    private class TransactionalTable
            extends BaseTable
    {
        private final SchemaTableName tableName;
        private final Table table;

        private TransactionalTable(SchemaTableName tableName, Table table, TableOperations ops)
        {
            super(ops, table.name());
            this.tableName = tableName;
            this.table = table;
        }

        @Override
        public TableScan newScan()
        {
            TableScan tableScan = super.newScan();
            if (tableScan instanceof DataTableScan) {
                return new TransactionalTableScan((DataTableScan) tableScan);
            }

            return tableScan;
        }

        @Override
        public UpdateSchema updateSchema()
        {
            return txForTable(tableName, table).updateSchema();
        }

        @Override
        public UpdatePartitionSpec updateSpec()
        {
            return txForTable(tableName, table).updateSpec();
        }

        @Override
        public UpdateProperties updateProperties()
        {
            return txForTable(tableName, table).updateProperties();
        }

        @Override
        public ReplaceSortOrder replaceSortOrder()
        {
            return txForTable(tableName, table).replaceSortOrder();
        }

        @Override
        public UpdateLocation updateLocation()
        {
            return txForTable(tableName, table).updateLocation();
        }

        @Override
        public AppendFiles newAppend()
        {
            return txForTable(tableName, table).newAppend();
        }

        @Override
        public AppendFiles newFastAppend()
        {
            return txForTable(tableName, table).newFastAppend();
        }

        @Override
        public RewriteFiles newRewrite()
        {
            return txForTable(tableName, table).newRewrite();
        }

        @Override
        public RewriteManifests rewriteManifests()
        {
            return txForTable(tableName, table).rewriteManifests();
        }

        @Override
        public OverwriteFiles newOverwrite()
        {
            return txForTable(tableName, table).newOverwrite();
        }

        @Override
        public RowDelta newRowDelta()
        {
            return txForTable(tableName, table).newRowDelta();
        }

        @Override
        public ReplacePartitions newReplacePartitions()
        {
            return txForTable(tableName, table).newReplacePartitions();
        }

        @Override
        public DeleteFiles newDelete()
        {
            return txForTable(tableName, table).newDelete();
        }

        @Override
        public UpdateStatistics updateStatistics()
        {
            return txForTable(tableName, table).updateStatistics();
        }

        @Override
        public ExpireSnapshots expireSnapshots()
        {
            return txForTable(tableName, table).expireSnapshots();
        }

        @Override
        public ManageSnapshots manageSnapshots()
        {
            return txForTable(tableName, table).manageSnapshots();
        }

        @Override
        public Transaction newTransaction()
        {
            return txForTable(tableName, table);
        }
    }

    private static class TransactionalTableScan
            extends DataTableScan
    {
        protected TransactionalTableScan(DataTableScan delegate)
        {
            super(delegate.table(), delegate.schema(), getScanContext(delegate));
        }
    }
}
