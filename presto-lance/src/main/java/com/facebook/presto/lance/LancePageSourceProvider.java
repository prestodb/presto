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
package com.facebook.presto.lance;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.plugin.arrow.ArrowBlockBuilder;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.inject.Inject;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class LancePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final LanceNamespaceHolder namespaceHolder;
    private final LanceConfig config;
    private final ArrowBlockBuilder arrowBlockBuilder;
    private final JsonCodec<LanceMergeCommitData> mergeCommitDataCodec;

    @Inject
    public LancePageSourceProvider(
            LanceNamespaceHolder namespaceHolder,
            LanceConfig config,
            ArrowBlockBuilder arrowBlockBuilder,
            JsonCodec<LanceMergeCommitData> mergeCommitDataCodec)
    {
        this.namespaceHolder = requireNonNull(namespaceHolder, "namespaceHolder is null");
        this.config = requireNonNull(config, "config is null");
        this.arrowBlockBuilder = requireNonNull(arrowBlockBuilder, "arrowBlockBuilder is null");
        this.mergeCommitDataCodec = requireNonNull(mergeCommitDataCodec, "mergeCommitDataCodec is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableLayoutHandle layout,
            List<ColumnHandle> columns,
            SplitContext splitContext,
            RuntimeStats runtimeStats)
    {
        LanceSplit lanceSplit = (LanceSplit) split;
        LanceTableLayoutHandle layoutHandle = (LanceTableLayoutHandle) layout;
        LanceTableHandle tableHandle = layoutHandle.getTable();

        List<LanceColumnHandle> lanceColumns = columns.stream()
                .map(LanceColumnHandle.class::cast)
                .collect(toImmutableList());

        // Row-level DELETE/UPDATE request a synthetic row-id column; in that case the scan must
        // surface row addresses and the result is wrapped in an UpdatablePageSource.
        boolean isUpdate = lanceColumns.stream().anyMatch(LanceColumnHandle::isUpdateRowId);
        boolean rowLevelChange = isUpdate || lanceColumns.stream().anyMatch(LanceColumnHandle::isRowAddress);

        String tablePath = tableHandle.getTablePath();

        // For UPDATE the synthetic $row_id struct is flattened into the real columns the scan
        // must read (row address + the unmodified columns), then re-assembled on output.
        List<LanceColumnHandle> delegateColumns = lanceColumns;
        Optional<LanceUpdatablePageSource.UpdateContext> updateContext = Optional.empty();
        if (isUpdate) {
            LanceColumnHandle rowIdColumn = lanceColumns.stream()
                    .filter(LanceColumnHandle::isUpdateRowId)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Missing update row id column"));
            List<LanceColumnHandle> rowIdChildren = rowIdStructColumns(rowIdColumn);

            LinkedHashMap<String, LanceColumnHandle> delegateByName = new LinkedHashMap<>();
            for (LanceColumnHandle column : lanceColumns) {
                if (!column.isUpdateRowId()) {
                    delegateByName.putIfAbsent(column.getColumnName(), column);
                }
            }
            for (LanceColumnHandle column : rowIdChildren) {
                delegateByName.putIfAbsent(column.getColumnName(), column);
            }
            delegateColumns = ImmutableList.copyOf(delegateByName.values());

            Schema arrowSchema = namespaceHolder.describeTable(tableHandle.getTablePath(), tableHandle.getDatasetVersion());
            updateContext = Optional.of(new LanceUpdatablePageSource.UpdateContext(
                    lanceColumns,
                    delegateColumns,
                    rowIdColumn,
                    tableColumns(arrowSchema),
                    tableHandle.getUpdatedColumns(),
                    arrowSchema,
                    namespaceHolder.getAllocator(),
                    tableHandle.getTablePath()));
        }

        // Build filter from predicate pushdown
        TupleDomain<ColumnHandle> predicate = layoutHandle.getTupleDomain();
        Optional<String> filter = LanceSessionProperties.isFilterPushdownEnabled(session)
                ? LanceSqlFilterBuilder.buildFilter(predicate)
                : Optional.empty();

        // Determine filter projection columns (needed for filter but not in output).
        // Only add extra columns when a filter was actually pushed down.
        // TODO: extractFilterColumnNames returns all TupleDomain columns, but buildFilter
        //   silently skips unsupported types (e.g. VARBINARY). In mixed queries this may
        //   add skipped columns to filterProjectionColumns, causing unnecessary I/O.
        //   Fix: have buildFilter also return the set of successfully pushed column names.
        Set<String> outputColumnNames = delegateColumns.stream()
                .map(LanceColumnHandle::getColumnName)
                .collect(toImmutableSet());
        List<String> filterProjectionColumns = filter.isPresent()
                ? LanceSqlFilterBuilder.extractFilterColumnNames(predicate).stream()
                        .filter(name -> !outputColumnNames.contains(name))
                        .collect(toImmutableList())
                : ImmutableList.of();

        LanceFragmentPageSource pageSource = new LanceFragmentPageSource(
                tableHandle,
                delegateColumns,
                lanceSplit.getFragments(),
                tableHandle.getTablePath(),
                config.getReadBatchSize(),
                namespaceHolder,
                tableHandle.getDatasetVersion(),
                arrowBlockBuilder,
                namespaceHolder.getAllocator(),
                filter,
                filterProjectionColumns,
                rowLevelChange);

        if (rowLevelChange) {
            long readVersion = tableHandle.getDatasetVersion()
                    .orElseGet(() -> namespaceHolder.getLatestVersion(tablePath));
            return new LanceUpdatablePageSource(pageSource, readVersion, mergeCommitDataCodec, updateContext);
        }

        return pageSource;
    }

    private static List<LanceColumnHandle> rowIdStructColumns(LanceColumnHandle rowIdColumn)
    {
        RowType rowType = (RowType) rowIdColumn.getColumnType();
        ImmutableList.Builder<LanceColumnHandle> children = ImmutableList.builder();
        List<RowType.Field> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            RowType.Field field = fields.get(i);
            String name = field.getName().orElseThrow(() -> new IllegalStateException("Row id struct field is missing a name"));
            if (i == 0) {
                children.add(LanceColumnHandle.rowAddressColumn());
            }
            else {
                children.add(new LanceColumnHandle(name, field.getType()));
            }
        }
        return children.build();
    }

    private static List<LanceColumnHandle> tableColumns(Schema arrowSchema)
    {
        return arrowSchema.getFields().stream()
                .map(field -> new LanceColumnHandle(field.getName(), LanceColumnHandle.toPrestoType(field), field.isNullable()))
                .collect(toImmutableList());
    }
}
