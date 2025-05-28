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
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_TRANSACTION_CONFLICT_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.UNKNOWN_TRANSACTION;

public class IcebergTransactionContext
{
    protected final boolean autoCommitContext;
    private Optional<Transaction> writeTableTransaction = Optional.empty();
    private final ConcurrentMap<SchemaTableName, Table> loadedIcebergTables = new ConcurrentHashMap<>();

    public IcebergTransactionContext(boolean autoCommitContext)
    {
        this.autoCommitContext = autoCommitContext;
    }

    public boolean isAutoCommitContext()
    {
        return autoCommitContext;
    }

    public Transaction getWriteTableTransactionOrThrow()
    {
        return writeTableTransaction.orElseThrow(() -> new PrestoException(UNKNOWN_TRANSACTION, "No write transaction has been opened on any Iceberg table"));
    }

    public IcebergTransactionContext setWriteTableTransaction(Transaction transaction)
    {
        if (!writeTableTransaction.isPresent()) {
            this.writeTableTransaction = Optional.of(transaction);
        }
        else if (!writeTableTransaction.get().table().uuid().equals(transaction.table().uuid())) {
            throw new PrestoException(ICEBERG_TRANSACTION_CONFLICT_ERROR, "Not allowed to open write transactions on different tables");
        }
        return this;
    }

    public Table getIcebergTable(SchemaTableName schemaTableName, Function<SchemaTableName, Table> rawIcebergTableLoader)
    {
        return loadedIcebergTables.computeIfAbsent(
                schemaTableName,
                rawIcebergTableLoader);
    }

    public void commit()
    {
        writeTableTransaction.ifPresent(Transaction::commitTransaction);
        writeTableTransaction = Optional.empty();
    }

    public void rollback()
    {
        writeTableTransaction = Optional.empty();
    }
}
