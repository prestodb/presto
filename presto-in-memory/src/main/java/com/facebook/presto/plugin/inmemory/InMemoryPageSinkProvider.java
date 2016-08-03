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

package com.facebook.presto.plugin.inmemory;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.plugin.inmemory.Types.checkType;
import static java.util.Objects.requireNonNull;

public class InMemoryPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final InMemoryPagesStore pagesStore;

    public InMemoryPageSinkProvider(InMemoryPagesStore pagesStore)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        InMemoryOutputTableHandle inMemoryOutputTableHandle = checkType(outputTableHandle, InMemoryOutputTableHandle.class, "outputTableHandle");
        SchemaTableName schemaTableName = inMemoryOutputTableHandle.getTable().toSchemaTableName();
        return new InMemoryPageSink(pagesStore, schemaTableName);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        InMemoryInsertTableHandle inMemoryInsertTableHandle = checkType(insertTableHandle, InMemoryInsertTableHandle.class, "insertTableHandle");
        SchemaTableName schemaTableName = inMemoryInsertTableHandle.getTable().toSchemaTableName();
        return new InMemoryPageSink(pagesStore, schemaTableName);
    }

    private static class InMemoryPageSink
            implements ConnectorPageSink
    {
        private final InMemoryPagesStore pagesStore;
        private final SchemaTableName schemaTableName;

        public InMemoryPageSink(InMemoryPagesStore pagesStore, SchemaTableName schemaTableName)
        {
            this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        }

        @Override
        public CompletableFuture<?> appendPage(Page page, Block sampleWeightBlock)
        {
            pagesStore.add(schemaTableName, page);
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
