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
package com.facebook.presto.raptorx.util;

import com.facebook.presto.raptorx.transaction.Transaction;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.NullableValue;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

public final class PredicateUtil
{
    private PredicateUtil() {}

    public static int getColumnIndex(ConnectorTableMetadata tableMetadata, String columnName)
    {
        List<ColumnMetadata> columns = tableMetadata.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column not found" + columnName);
    }

    public static Optional<String> getStringValue(NullableValue value)
    {
        if ((value == null) || value.isNull()) {
            return Optional.empty();
        }
        return Optional.of(((Slice) value.getValue()).toStringUtf8());
    }

    public static List<SchemaTableName> listTables(Transaction transaction, Optional<String> schemaName, Optional<String> tableName)
    {
        if (schemaName.isPresent() && tableName.isPresent()) {
            return ImmutableList.of(new SchemaTableName(schemaName.get(), tableName.get()));
        }

        if (schemaName.isPresent()) {
            return transaction.listTables(schemaName.get());
        }

        if (tableName.isPresent()) {
            return transaction.listTables().stream()
                    .filter(name -> tableName.get().equals(name.getTableName()))
                    .collect(toImmutableList());
        }

        return transaction.listTables();
    }
}
