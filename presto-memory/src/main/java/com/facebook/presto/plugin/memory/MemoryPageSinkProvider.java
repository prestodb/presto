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

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.plugin.memory.Types.checkType;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MemoryPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final MemoryPagesStore pagesStore;

    public MemoryPageSinkProvider(MemoryPagesStore pagesStore)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        MemoryOutputTableHandle memoryOutputTableHandle = checkType(outputTableHandle, MemoryOutputTableHandle.class, "outputTableHandle");
        MemoryTableHandle tableHandle = memoryOutputTableHandle.getTable();
        long tableId = tableHandle.getTableId();
        checkState(tableHandle.getActiveTableIds().contains(tableId));

        pagesStore.cleanUp(tableHandle.getActiveTableIds());
        return new MemoryPageSink(pagesStore, tableId);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        MemoryInsertTableHandle memoryInsertTableHandle = checkType(insertTableHandle, MemoryInsertTableHandle.class, "insertTableHandle");
        MemoryTableHandle tableHandle = memoryInsertTableHandle.getTable();
        long tableId = tableHandle.getTableId();
        checkState(tableHandle.getActiveTableIds().contains(tableId));

        pagesStore.cleanUp(tableHandle.getActiveTableIds());
        return new MemoryPageSink(pagesStore, tableId);
    }

    private static class MemoryPageSink
            implements ConnectorPageSink
    {
        private final MemoryPagesStore pagesStore;
        private final long tableId;

        public MemoryPageSink(MemoryPagesStore pagesStore, long tableId)
        {
            this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
            this.tableId = tableId;
        }

        @Override
        public CompletableFuture<?> appendPage(Page page, Block sampleWeightBlock)
        {
            pagesStore.add(tableId, page);
            return NOT_BLOCKED;
        }

        @Override
        public Collection<Slice> finish()
        {
            return ImmutableList.of();
        }

        @Override
        public void abort()
        {
        }
    }
}
