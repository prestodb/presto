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
package io.prestosql.plugin.memory;

import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedPageSource;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class MemoryPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final MemoryPagesStore pagesStore;

    @Inject
    public MemoryPageSourceProvider(MemoryPagesStore pagesStore)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            List<ColumnHandle> columns)
    {
        MemorySplit memorySplit = (MemorySplit) split;
        long tableId = memorySplit.getTableHandle().getTableId();
        int partNumber = memorySplit.getPartNumber();
        int totalParts = memorySplit.getTotalPartsPerWorker();
        long expectedRows = memorySplit.getExpectedRows();

        List<Integer> columnIndexes = columns.stream()
                .map(MemoryColumnHandle.class::cast)
                .map(MemoryColumnHandle::getColumnIndex).collect(toList());
        List<Page> pages = pagesStore.getPages(
                tableId,
                partNumber,
                totalParts,
                columnIndexes,
                expectedRows);

        return new FixedPageSource(pages);
    }
}
