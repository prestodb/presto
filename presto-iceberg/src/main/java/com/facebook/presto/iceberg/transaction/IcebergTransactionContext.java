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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.transaction.IsolationLevel;
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
import org.apache.iceberg.Table;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_TRANSACTION_CONFLICT_ERROR;
import static com.facebook.presto.iceberg.IcebergUtil.opsFromTable;
import static com.google.common.collect.Iterators.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.IcebergLibUtils.getScanContext;

public class IcebergTransactionContext
{
    private final IsolationLevel isolationLevel;
    private final boolean autoCommitContext;
    private final Map<SchemaTableName, Transaction> txByTable;
    private final Map<SchemaTableName, Table> initiallyReadTables;
    private Optional<Runnable> callbacksOnCommit = Optional.empty();

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
        if (txByTable.containsKey(tableName)) {
            return Optional.ofNullable(txByTable.get(tableName).table());
        }

        return Optional.empty();
    }

    public Optional<Transaction> getTransaction(SchemaTableName tableName)
    {
        if (txByTable.containsKey(tableName)) {
            return Optional.ofNullable(txByTable.get(tableName));
        }

        return Optional.empty();
    }

    public Optional<Table> initiallyReadTable(SchemaTableName tableName)
    {
        if (initiallyReadTables.containsKey(tableName)) {
            return Optional.ofNullable(initiallyReadTables.get(tableName));
        }

        return Optional.empty();
    }

    public void setCreateTableTransaction(SchemaTableName tableName, Transaction transaction)
    {
        if (txByTable.isEmpty()) {
            txByTable.put(tableName, transaction);
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

    public void setCallback(Runnable callback)
    {
        this.callbacksOnCommit = Optional.of(callback);
    }

    public void commit()
    {
        if (!txByTable.isEmpty()) {
            getOnlyElement(txByTable.values().iterator()).commitTransaction();
            callbacksOnCommit.ifPresent(Runnable::run);
            txByTable.clear();
        }
        initiallyReadTables.clear();
        callbacksOnCommit = Optional.empty();
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
                k -> Transactions.newTransaction(table.name(), ((HasTableOperations) table).operations()));
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
