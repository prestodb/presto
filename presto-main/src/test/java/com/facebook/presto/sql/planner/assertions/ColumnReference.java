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
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;

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
    public Optional<VariableReferenceExpression> getAssignedVariable(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        TableHandle tableHandle;
        Map<VariableReferenceExpression, ColumnHandle> assignments;

        if (node instanceof TableScanNode) {
            TableScanNode tableScanNode = (TableScanNode) node;
            tableHandle = tableScanNode.getTable();
            assignments = tableScanNode.getAssignments();
        }
        else if (node instanceof IndexSourceNode) {
            IndexSourceNode indexSourceNode = (IndexSourceNode) node;
            tableHandle = indexSourceNode.getTableHandle();
            assignments = indexSourceNode.getAssignments();
        }
        else {
            return Optional.empty();
        }

        TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
        String actualTableName = tableMetadata.getTable().getTableName();

        // Wrong table -> doesn't match.
        if (!tableName.equalsIgnoreCase(actualTableName)) {
            return Optional.empty();
        }

        Optional<ColumnHandle> columnHandle = getColumnHandle(tableHandle, session, metadata);

        checkState(columnHandle.isPresent(), format("Table %s doesn't have column %s. Typo in test?", tableName, columnName));

        return getAssignedVariable(assignments, columnHandle.get());
    }

    private Optional<VariableReferenceExpression> getAssignedVariable(Map<VariableReferenceExpression, ColumnHandle> assignments, ColumnHandle columnHandle)
    {
        Optional<VariableReferenceExpression> result = Optional.empty();
        for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : assignments.entrySet()) {
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
