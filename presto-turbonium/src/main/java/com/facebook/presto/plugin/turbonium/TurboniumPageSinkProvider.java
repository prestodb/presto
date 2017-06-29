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

package com.facebook.presto.plugin.turbonium;

import com.facebook.presto.plugin.turbonium.config.TurboniumConfigManager;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class TurboniumPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private static final Logger log = Logger.get(TurboniumPageSinkProvider.class);
    private final TurboniumPagesStore pagesStore;
    private final TurboniumConfigManager configManager;

    @Inject
    public TurboniumPageSinkProvider(TurboniumPagesStore pagesStore, TurboniumConfigManager configManager)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
        this.configManager = requireNonNull(configManager, "configManager is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        TurboniumOutputTableHandle turboniumOutputTableHandle = (TurboniumOutputTableHandle) outputTableHandle;
        TurboniumTableHandle tableHandle = turboniumOutputTableHandle.getTable();
        long tableId = tableHandle.getTableId();
        checkState(turboniumOutputTableHandle.getActiveTableIds().contains(tableId));
        configManager.setFromConfig(turboniumOutputTableHandle.getMemoryConfig());
        pagesStore.cleanUp(turboniumOutputTableHandle.getActiveTableIds());
        pagesStore.initialize(tableId, tableHandle);
        return new MemoryPageSink(pagesStore, tableId);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        TurboniumInsertTableHandle turboniumInsertTableHandle = (TurboniumInsertTableHandle) insertTableHandle;
        TurboniumTableHandle tableHandle = turboniumInsertTableHandle.getTable();
        long tableId = tableHandle.getTableId();
        checkState(turboniumInsertTableHandle.getActiveTableIds().contains(tableId));
        configManager.setFromConfig(turboniumInsertTableHandle.getMemoryConfig());
        pagesStore.cleanUp(turboniumInsertTableHandle.getActiveTableIds());
        return new MemoryPageSink(pagesStore, tableId);
    }

    private static class MemoryPageSink
            implements ConnectorPageSink
    {
        private final TurboniumPagesStore pagesStore;
        private final long tableId;

        public MemoryPageSink(TurboniumPagesStore pagesStore, long tableId)
        {
            this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
            this.tableId = tableId;
        }

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            pagesStore.add(tableId, page);
            return NOT_BLOCKED;
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            pagesStore.finishCreate(tableId);
            return completedFuture(ImmutableList.of());
        }

        @Override
        public void abort()
        {
        }
    }
}
