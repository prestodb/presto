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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ColumnReference
    implements RvalueMatcher
{
    private final String tableName;
    private final String columnName;

    public ColumnReference(String tableName, String columnName)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
    }

    @Override
    public Optional<Symbol> getAssignedSymbol(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        if (!(node instanceof TableScanNode)) {
            return Optional.empty();
        }

        TableScanNode tableScanNode = (TableScanNode) node;
        TableMetadata tableMetadata = metadata.getTableMetadata(session, tableScanNode.getTable());
        String actualTableName = tableMetadata.getTable().getTableName();

        // Wrong table -> doesn't match.
        if (!tableName.equalsIgnoreCase(actualTableName)) {
            return Optional.empty();
        }

        Optional<ColumnHandle> columnHandle = getColumnHandle(tableScanNode.getTable(), session, metadata);

        checkState(columnHandle.isPresent(), format("Table %s doesn't have column %s. Typo in test?", tableName, columnName));

        return getAssignedSymbol(tableScanNode, columnHandle.get());
    }

    private Optional<Symbol> getAssignedSymbol(TableScanNode tableScanNode, ColumnHandle columnHandle)
    {
        Optional<Symbol> result = Optional.empty();
        for (Map.Entry<Symbol, ColumnHandle> entry : tableScanNode.getAssignments().entrySet()) {
            if (entry.getValue().equals(columnHandle)) {
                checkState(!result.isPresent(), "Multiple ColumnHandles found for %s:%s in table scan assignments", tableName, columnName);
                result = Optional.of(entry.getKey());
            }
        }
        return result;
    }

    private Optional<ColumnHandle> getColumnHandle(TableHandle tableHandle, Session session, Metadata metadata)
    {
        return metadata.getColumnHandles(session, tableHandle).entrySet()
                .stream()
                .filter(entry -> columnName.equals(entry.getKey()))
                .map(Map.Entry::getValue)
                .findFirst();
    }

    @Override
    public String toString()
    {
        return format("Column %s:%s", tableName, columnName);
    }
}
