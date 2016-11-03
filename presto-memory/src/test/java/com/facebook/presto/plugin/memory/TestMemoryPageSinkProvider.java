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
import com.facebook.presto.spi.block.FixedWidthBlockBuilder;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static java.lang.String.format;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMemoryPageSinkProvider
{
    public static final ConnectorSession SESSION = new TestingConnectorSession(ImmutableList.of());

    private MemoryPagesStore pagesStore;
    private MemoryPageSinkProvider pageSinkProvider;

    @BeforeMethod
    public void setUp()
    {
        pagesStore = new MemoryPagesStore();
        pageSinkProvider = new MemoryPageSinkProvider(pagesStore);
    }

    @Test
    public void testCleanUp()
    {
        createTable(0L, 0L);
        createTable(1L, 0L, 1L);
        createTable(2L, 0L, 1L, 2L);

        assertTrue(pagesStore.contains(0L));
        assertTrue(pagesStore.contains(1L));
        assertTrue(pagesStore.contains(2L));

        insertToTable(1L, 0L, 1L);

        assertTrue(pagesStore.contains(0L));
        assertTrue(pagesStore.contains(1L));
        assertTrue(pagesStore.contains(2L));

        insertToTable(2L, 0L, 2L);

        assertTrue(pagesStore.contains(0L));
        assertFalse(pagesStore.contains(1L));
        assertTrue(pagesStore.contains(2L));
    }

    private void insertToTable(long tableId, Long... activeTableIds)
    {
        ConnectorPageSink pageSink = pageSinkProvider.createPageSink(
                MemoryTransactionHandle.INSTANCE,
                SESSION,
                createMemoryInsertTableHandle(tableId, activeTableIds));
        pageSink.appendPage(createEmptyPage());
    }

    private void createTable(long tableId, Long... activeTableIds)
    {
        ConnectorPageSink pageSink = pageSinkProvider.createPageSink(
                MemoryTransactionHandle.INSTANCE,
                SESSION,
                createMemoryOutputTableHandle(tableId, activeTableIds));
        pageSink.appendPage(createEmptyPage());
    }

    private ConnectorOutputTableHandle createMemoryOutputTableHandle(long tableId, Long... activeTableIds)
    {
        return new MemoryOutputTableHandle(new MemoryTableHandle("test", "schema", format("table_%d", tableId), tableId, ImmutableList.of(), ImmutableSet.copyOf(activeTableIds)));
    }

    private ConnectorInsertTableHandle createMemoryInsertTableHandle(long tableId, Long[] activeTableIds)
    {
        return new MemoryInsertTableHandle(new MemoryTableHandle("test", "schema", format("table_%d", tableId), tableId, ImmutableList.of(), ImmutableSet.copyOf(activeTableIds)));
    }

    private Page createEmptyPage()
    {
        FixedWidthBlockBuilder blockBuilder = new FixedWidthBlockBuilder(4, 0);
        return new Page(0, blockBuilder.build());
    }
}
