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
package com.facebook.presto.operator.exchange;

import com.facebook.presto.SequencePageBuilder;
import com.facebook.presto.operator.InterpretedHashGenerator;
import com.facebook.presto.operator.PageAssertions;
import com.facebook.presto.operator.exchange.LocalExchange.LocalExchangeSinkFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_RANDOM_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.airlift.units.DataSize.Unit.BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestLocalExchange
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);
    private static final DataSize PAGE_SIZE = new DataSize(createPage(42).getSizeInBytes(), BYTE);

    @Test
    public void testGatherSingleWriter()
    {
        LocalExchange exchange = new LocalExchange(SINGLE_DISTRIBUTION, 8, TYPES, ImmutableList.of(), Optional.empty(), new DataSize(sizeOfPages(99), BYTE));
        assertEquals(exchange.getBufferCount(), 1);
        assertExchangeTotalBufferPages(exchange, 0);

        LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();

        LocalExchangeSource source = exchange.getSource(0);
        assertSource(source, 0);

        LocalExchangeSink sink = sinkFactory.createSink();
        sinkFactory.close();
        sinkFactory.noMoreSinkFactories();

        assertSinkCanWrite(sink);
        assertSource(source, 0);

        // add the first page which should cause the reader to unblock
        ListenableFuture<?> readFuture = source.waitForReading();
        assertFalse(readFuture.isDone());
        sink.addPage(createPage(0));
        assertTrue(readFuture.isDone());
        assertExchangeTotalBufferPages(exchange, 1);

        assertSource(source, 1);

        sink.addPage(createPage(1));
        assertSource(source, 2);
        assertExchangeTotalBufferPages(exchange, 2);

        assertRemovePage(source, createPage(0));
        assertSource(source, 1);
        assertExchangeTotalBufferPages(exchange, 1);

        assertRemovePage(source, createPage(1));
        assertSource(source, 0);
        assertExchangeTotalBufferPages(exchange, 0);

        sink.addPage(createPage(2));
        sink.addPage(createPage(3));
        assertSource(source, 2);
        assertExchangeTotalBufferPages(exchange, 2);

        sink.finish();
        assertSinkFinished(sink);

        assertSource(source, 2);

        assertRemovePage(source, createPage(2));
        assertSource(source, 1);
        assertSinkFinished(sink);
        assertExchangeTotalBufferPages(exchange, 1);

        assertRemovePage(source, createPage(3));
        assertSourceFinished(source);
        assertExchangeTotalBufferPages(exchange, 0);
    }

    @Test
    public void testBroadcast()
    {
        LocalExchange exchange = new LocalExchange(FIXED_BROADCAST_DISTRIBUTION, 2, TYPES, ImmutableList.of(), Optional.empty());
        assertEquals(exchange.getBufferCount(), 2);
        assertExchangeTotalBufferPages(exchange, 0);

        LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
        LocalExchangeSink sinkA = sinkFactory.createSink();
        assertSinkCanWrite(sinkA);
        LocalExchangeSink sinkB = sinkFactory.createSink();
        assertSinkCanWrite(sinkB);
        sinkFactory.close();
        sinkFactory.noMoreSinkFactories();

        LocalExchangeSource sourceA = exchange.getSource(0);
        assertSource(sourceA, 0);

        LocalExchangeSource sourceB = exchange.getSource(1);
        assertSource(sourceB, 0);

        sinkA.addPage(createPage(0));

        assertSource(sourceA, 1);
        assertSource(sourceB, 1);
        assertExchangeTotalBufferPages(exchange, 1);

        sinkA.addPage(createPage(0));

        assertSource(sourceA, 2);
        assertSource(sourceB, 2);
        assertExchangeTotalBufferPages(exchange, 2);

        assertRemovePage(sourceA, createPage(0));
        assertSource(sourceA, 1);
        assertSource(sourceB, 2);
        assertExchangeTotalBufferPages(exchange, 2);

        assertRemovePage(sourceA, createPage(0));
        assertSource(sourceA, 0);
        assertSource(sourceB, 2);
        assertExchangeTotalBufferPages(exchange, 2);

        sinkA.finish();
        assertSinkFinished(sinkA);
        assertExchangeTotalBufferPages(exchange, 2);

        sinkB.addPage(createPage(0));
        assertSource(sourceA, 1);
        assertSource(sourceB, 3);
        assertExchangeTotalBufferPages(exchange, 3);

        sinkB.finish();
        assertSinkFinished(sinkB);
        assertSource(sourceA, 1);
        assertSource(sourceB, 3);
        assertExchangeTotalBufferPages(exchange, 3);

        assertRemovePage(sourceA, createPage(0));
        assertSourceFinished(sourceA);
        assertSource(sourceB, 3);
        assertExchangeTotalBufferPages(exchange, 3);

        assertRemovePage(sourceB, createPage(0));
        assertRemovePage(sourceB, createPage(0));
        assertSourceFinished(sourceA);
        assertSource(sourceB, 1);
        assertExchangeTotalBufferPages(exchange, 1);

        assertRemovePage(sourceB, createPage(0));
        assertSourceFinished(sourceA);
        assertSourceFinished(sourceB);
        assertExchangeTotalBufferPages(exchange, 0);
    }

    @Test
    public void testRandom()
    {
        LocalExchange exchange = new LocalExchange(FIXED_RANDOM_DISTRIBUTION, 2, TYPES, ImmutableList.of(), Optional.empty());
        assertEquals(exchange.getBufferCount(), 2);
        assertExchangeTotalBufferPages(exchange, 0);

        LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
        LocalExchangeSink sink = sinkFactory.createSink();
        assertSinkCanWrite(sink);
        sinkFactory.close();
        sinkFactory.noMoreSinkFactories();

        LocalExchangeSource sourceA = exchange.getSource(0);
        assertSource(sourceA, 0);

        LocalExchangeSource sourceB = exchange.getSource(1);
        assertSource(sourceB, 0);

        for (int i = 0; i < 100; i++) {
            Page page = createPage(0);
            sink.addPage(page);
            assertExchangeTotalBufferPages(exchange, i + 1);

            LocalExchangeBufferInfo bufferInfoA = sourceA.getBufferInfo();
            LocalExchangeBufferInfo bufferInfoB = sourceB.getBufferInfo();
            assertEquals(bufferInfoA.getBufferedBytes() + bufferInfoB.getBufferedBytes(), sizeOfPages(i + 1));
            assertEquals(bufferInfoA.getBufferedPages() + bufferInfoB.getBufferedPages(), i + 1);
        }

        // we should get ~50 pages per source, but we should get at least some pages in each buffer
        assertTrue(sourceA.getBufferInfo().getBufferedPages() > 0);
        assertTrue(sourceB.getBufferInfo().getBufferedPages() > 0);
        assertExchangeTotalBufferPages(exchange, 100);
    }

    @Test
    public void testPartition()
    {
        LocalExchange exchange = new LocalExchange(FIXED_HASH_DISTRIBUTION, 2, TYPES, ImmutableList.of(0), Optional.empty());
        assertEquals(exchange.getBufferCount(), 2);
        assertExchangeTotalBufferPages(exchange, 0);

        LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
        LocalExchangeSink sink = sinkFactory.createSink();
        assertSinkCanWrite(sink);
        sinkFactory.close();
        sinkFactory.noMoreSinkFactories();

        LocalExchangeSource sourceA = exchange.getSource(0);
        assertSource(sourceA, 0);

        LocalExchangeSource sourceB = exchange.getSource(1);
        assertSource(sourceB, 0);

        sink.addPage(createPage(0));

        assertSource(sourceA, 1);
        assertSource(sourceB, 1);
        assertExchangeTotalBufferPages(exchange, 1);

        sink.addPage(createPage(0));

        assertSource(sourceA, 2);
        assertSource(sourceB, 2);
        assertExchangeTotalBufferPages(exchange, 2);

        assertPartitionedRemovePage(sourceA, 0, 2);
        assertSource(sourceA, 1);
        assertSource(sourceB, 2);

        assertPartitionedRemovePage(sourceA, 0, 2);
        assertSource(sourceA, 0);
        assertSource(sourceB, 2);

        sink.finish();
        assertSinkFinished(sink);
        assertSourceFinished(sourceA);
        assertSource(sourceB, 2);

        assertPartitionedRemovePage(sourceB, 1, 2);
        assertSourceFinished(sourceA);
        assertSource(sourceB, 1);

        assertPartitionedRemovePage(sourceB, 1, 2);
        assertSourceFinished(sourceA);
        assertSourceFinished(sourceB);
        assertExchangeTotalBufferPages(exchange, 0);
    }

    @Test
    public void writeUnblockWhenAllReadersFinish()
    {
        ImmutableList<BigintType> types = ImmutableList.of(BIGINT);

        LocalExchange exchange = new LocalExchange(FIXED_BROADCAST_DISTRIBUTION, 2, types, ImmutableList.of(), Optional.empty());
        assertEquals(exchange.getBufferCount(), 2);
        assertExchangeTotalBufferPages(exchange, 0);

        LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
        LocalExchangeSink sinkA = sinkFactory.createSink();
        assertSinkCanWrite(sinkA);
        LocalExchangeSink sinkB = sinkFactory.createSink();
        assertSinkCanWrite(sinkB);
        sinkFactory.close();
        sinkFactory.noMoreSinkFactories();

        LocalExchangeSource sourceA = exchange.getSource(0);
        assertSource(sourceA, 0);

        LocalExchangeSource sourceB = exchange.getSource(1);
        assertSource(sourceB, 0);

        sourceA.finish();
        assertSourceFinished(sourceA);

        assertSinkCanWrite(sinkA);
        assertSinkCanWrite(sinkB);

        sourceB.finish();
        assertSourceFinished(sourceB);

        assertSinkFinished(sinkA);
        assertSinkFinished(sinkB);
    }

    @Test
    public void writeUnblockWhenAllReadersFinishAndPagesConsumed()
    {
        LocalExchange exchange = new LocalExchange(FIXED_BROADCAST_DISTRIBUTION, 2, TYPES, ImmutableList.of(), Optional.empty(), new DataSize(1, BYTE));
        assertEquals(exchange.getBufferCount(), 2);
        assertExchangeTotalBufferPages(exchange, 0);

        LocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
        LocalExchangeSink sinkA = sinkFactory.createSink();
        assertSinkCanWrite(sinkA);
        LocalExchangeSink sinkB = sinkFactory.createSink();
        assertSinkCanWrite(sinkB);
        sinkFactory.close();
        sinkFactory.noMoreSinkFactories();

        LocalExchangeSource sourceA = exchange.getSource(0);
        assertSource(sourceA, 0);

        LocalExchangeSource sourceB = exchange.getSource(1);
        assertSource(sourceB, 0);

        sinkA.addPage(createPage(0));
        ListenableFuture<?> sinkAFuture = assertSinkWriteBlocked(sinkA);
        ListenableFuture<?> sinkBFuture = assertSinkWriteBlocked(sinkB);

        assertSource(sourceA, 1);
        assertSource(sourceB, 1);
        assertExchangeTotalBufferPages(exchange, 1);

        sourceA.finish();
        assertSource(sourceA, 1);
        assertRemovePage(sourceA, createPage(0));
        assertSourceFinished(sourceA);
        assertExchangeTotalBufferPages(exchange, 1);

        assertSource(sourceB, 1);
        assertSinkWriteBlocked(sinkA);
        assertSinkWriteBlocked(sinkB);

        sourceB.finish();
        assertSource(sourceB, 1);
        assertRemovePage(sourceB, createPage(0));
        assertSourceFinished(sourceB);
        assertExchangeTotalBufferPages(exchange, 0);

        assertTrue(sinkAFuture.isDone());
        assertTrue(sinkBFuture.isDone());

        assertSinkFinished(sinkA);
        assertSinkFinished(sinkB);
    }

    private static void assertSource(LocalExchangeSource source, int pageCount)
    {
        assertEquals(source.getTypes(), TYPES);
        LocalExchangeBufferInfo bufferInfo = source.getBufferInfo();
        assertEquals(bufferInfo.getBufferedPages(), pageCount);
        assertFalse(source.isFinished());
        if (pageCount == 0) {
            assertFalse(source.waitForReading().isDone());
            assertNull(source.removePage());
            assertFalse(source.waitForReading().isDone());
            assertFalse(source.isFinished());
            assertEquals(bufferInfo.getBufferedBytes(), 0);
        }
        else {
            assertTrue(source.waitForReading().isDone());
            assertTrue(bufferInfo.getBufferedBytes() > 0);
        }
    }

    private static void assertSourceFinished(LocalExchangeSource source)
    {
        assertEquals(source.getTypes(), TYPES);
        assertTrue(source.isFinished());
        LocalExchangeBufferInfo bufferInfo = source.getBufferInfo();
        assertEquals(bufferInfo.getBufferedPages(), 0);
        assertEquals(bufferInfo.getBufferedBytes(), 0);

        assertTrue(source.waitForReading().isDone());
        assertNull(source.removePage());
        assertTrue(source.waitForReading().isDone());

        assertTrue(source.isFinished());
    }

    private static void assertRemovePage(LocalExchangeSource source, Page expectedPage)
    {
        assertEquals(source.getTypes(), TYPES);
        assertTrue(source.waitForReading().isDone());
        Page actualPage = source.removePage();
        assertNotNull(actualPage);

        assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
        PageAssertions.assertPageEquals(TYPES, actualPage, expectedPage);
    }

    private static void assertPartitionedRemovePage(LocalExchangeSource source, int partition, int partitionCount)
    {
        assertEquals(source.getTypes(), TYPES);
        assertTrue(source.waitForReading().isDone());
        Page page = source.removePage();
        assertNotNull(page);

        LocalPartitionGenerator partitionGenerator = new LocalPartitionGenerator(new InterpretedHashGenerator(TYPES, new int[] {0}), partitionCount);
        for (int position = 0; position < page.getPositionCount(); position++) {
            assertEquals(partitionGenerator.getPartition(position, page), partition);
        }
    }

    private static void assertSinkCanWrite(LocalExchangeSink sink)
    {
        assertEquals(sink.getTypes(), TYPES);
        assertFalse(sink.isFinished());
        assertTrue(sink.waitForWriting().isDone());
    }

    private static ListenableFuture<?> assertSinkWriteBlocked(LocalExchangeSink sink)
    {
        assertEquals(sink.getTypes(), TYPES);
        assertFalse(sink.isFinished());
        ListenableFuture<?> writeFuture = sink.waitForWriting();
        assertFalse(writeFuture.isDone());
        return writeFuture;
    }

    private static void assertSinkFinished(LocalExchangeSink sink)
    {
        assertEquals(sink.getTypes(), TYPES);
        assertTrue(sink.isFinished());
        assertTrue(sink.waitForWriting().isDone());

        // this will be ignored
        sink.addPage(createPage(0));
        assertTrue(sink.isFinished());
        assertTrue(sink.waitForWriting().isDone());
    }

    private static void assertExchangeTotalBufferPages(LocalExchange exchange, int pageCount)
    {
        assertEquals(exchange.getBufferedBytes(), sizeOfPages(pageCount));
    }

    private static Page createPage(int i)
    {
        return SequencePageBuilder.createSequencePage(TYPES, 100, i);
    }

    public static long sizeOfPages(int count)
    {
        return PAGE_SIZE.toBytes() * count;
    }
}
