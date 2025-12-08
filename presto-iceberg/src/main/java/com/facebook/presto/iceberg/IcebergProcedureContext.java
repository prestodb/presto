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
package com.facebook.presto.iceberg;

import com.facebook.presto.spi.connector.ConnectorProcedureContext;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;

import static java.util.Objects.requireNonNull;

public class IcebergProcedureContext
        implements ConnectorProcedureContext
{
    final Table table;
    final Transaction transaction;

    public IcebergProcedureContext(Table table, Transaction transaction)
    {
        this.table = requireNonNull(table, "table is null");
        this.transaction = requireNonNull(transaction, "transaction is null");
    }

    public Table getTable()
    {
        return table;
    }

    public Transaction getTransaction()
    {
        return transaction;
    }
}
