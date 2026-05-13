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

import com.facebook.plugin.arrow.ArrowBlockBuilder;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

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

    @Inject
    public LancePageSourceProvider(LanceNamespaceHolder namespaceHolder, LanceConfig config, ArrowBlockBuilder arrowBlockBuilder)
    {
        this.namespaceHolder = requireNonNull(namespaceHolder, "namespaceHolder is null");
        this.config = requireNonNull(config, "config is null");
        this.arrowBlockBuilder = requireNonNull(arrowBlockBuilder, "arrowBlockBuilder is null");
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

        String tablePath = lanceSplit.getDatasetPath();

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
        Set<String> outputColumnNames = lanceColumns.stream()
                .map(LanceColumnHandle::getColumnName)
                .collect(toImmutableSet());
        List<String> filterProjectionColumns = filter.isPresent()
                ? LanceSqlFilterBuilder.extractFilterColumnNames(predicate).stream()
                        .filter(name -> !outputColumnNames.contains(name))
                        .collect(toImmutableList())
                : ImmutableList.of();

        return new LanceFragmentPageSource(
                tableHandle,
                lanceColumns,
                lanceSplit.getFragments(),
                tablePath,
                config.getReadBatchSize(),
                namespaceHolder,
                tableHandle.getDatasetVersion(),
                arrowBlockBuilder,
                namespaceHolder.getAllocator(),
                filter,
                filterProjectionColumns);
    }
}
