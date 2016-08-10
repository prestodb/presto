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

package com.facebook.presto.plugin.memory;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.List;

import static com.facebook.presto.plugin.memory.Types.checkType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class MemoryPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final MemoryPagesStore pagesStore;

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
        MemorySplit memorySplit = checkType(split, MemorySplit.class, "split");
        SchemaTableName schemaTableName = memorySplit.getTableHandle().toSchemaTableName();
        int partNumber = memorySplit.getPartNumber();
        int totalParts = memorySplit.getTotalPartsPerWorker();

        List<Integer> columnIndexes = columns.stream()
                .map(value -> checkType(value, MemoryColumnHandle.class, "columns"))
                .map(MemoryColumnHandle::getColumnIndex).collect(toList());
        List<Page> pages = pagesStore.getPages(schemaTableName, partNumber, totalParts, columnIndexes);

        return new FixedPageSource(pages);
    }
}
