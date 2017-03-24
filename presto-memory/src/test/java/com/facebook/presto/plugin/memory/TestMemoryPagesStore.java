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
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMemoryPagesStore
{
    public static final ConnectorSession SESSION = new TestingConnectorSession(ImmutableList.of());

    private MemoryPagesStore pagesStore;
    private MemoryPageSinkProvider pageSinkProvider;

    @BeforeMethod
    public void setUp()
    {
        pagesStore = new MemoryPagesStore(new MemoryConfig().setMaxDataPerNode(new DataSize(1, DataSize.Unit.MEGABYTE)));
        pageSinkProvider = new MemoryPageSinkProvider(pagesStore);
    }

    @Test
    public void testCreateEmptyTable()
    {
        createTable(0L, 0L);
        assertEquals(pagesStore.getPages(0L, 0, 1, ImmutableList.of(0)), ImmutableList.of());
    }

    @Test
    public void testInsertPage()
    {
        createTable(0L, 0L);
        insertToTable(0L, 0L);
        assertEquals(pagesStore.getPages(0L, 0, 1, ImmutableList.of(0)).size(), 1);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testReadFromUnknownTable()
    {
        pagesStore.getPages(0L, 0, 1, ImmutableList.of(0));
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testWriteToUnknownTable()
    {
        insertToTable(0L, 0L);
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

    @Test(expectedExceptions = PrestoException.class)
    public void testMemoryLimitExceeded()
    {
        createTable(0L, 0L);
        insertToTable(0L, createOneMegaBytePage(), 0L);
        insertToTable(0L, createOneMegaBytePage(), 0L);
    }

    private void insertToTable(long tableId, Long... activeTableIds)
    {
        insertToTable(tableId, createPage(), activeTableIds);
    }

    private void insertToTable(long tableId, Page page, Long... activeTableIds)
    {
        ConnectorPageSink pageSink = pageSinkProvider.createPageSink(
                MemoryTransactionHandle.INSTANCE,
                SESSION,
                createMemoryInsertTableHandle(tableId, activeTableIds));
        pageSink.appendPage(page);
        pageSink.finish();
    }

    private void createTable(long tableId, Long... activeTableIds)
    {
        ConnectorPageSink pageSink = pageSinkProvider.createPageSink(
                MemoryTransactionHandle.INSTANCE,
                SESSION,
                createMemoryOutputTableHandle(tableId, activeTableIds));
        pageSink.finish();
    }

    private static ConnectorOutputTableHandle createMemoryOutputTableHandle(long tableId, Long... activeTableIds)
    {
        return new MemoryOutputTableHandle(
                new MemoryTableHandle(
                        "test",
                        "schema",
                        format("table_%d", tableId),
                        tableId, ImmutableList.of(),
                        ImmutableList.of(HostAddress.fromString("localhost:8080"))),
                ImmutableSet.copyOf(activeTableIds));
    }

    private static ConnectorInsertTableHandle createMemoryInsertTableHandle(long tableId, Long[] activeTableIds)
    {
        return new MemoryInsertTableHandle(
                new MemoryTableHandle(
                        "test",
                        "schema",
                        format("table_%d", tableId),
                        tableId,
                        ImmutableList.of(),
                        ImmutableList.of(HostAddress.fromString("localhost:8080"))),
                ImmutableSet.copyOf(activeTableIds));
    }

    private static Page createPage()
    {
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(1);
        BIGINT.writeLong(blockBuilder, 42L);
        return new Page(0, blockBuilder.build());
    }

    private static Page createOneMegaBytePage()
    {
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(1);
        while (blockBuilder.getRetainedSizeInBytes() < 1024 * 1024) {
            BIGINT.writeLong(blockBuilder, 42L);
        }
        return new Page(0, blockBuilder.build());
    }
}
