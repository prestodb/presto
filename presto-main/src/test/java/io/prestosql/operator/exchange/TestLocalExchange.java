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
package io.prestosql.operator.exchange;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.prestosql.SequencePageBuilder;
import io.prestosql.execution.Lifespan;
import io.prestosql.operator.InterpretedHashGenerator;
import io.prestosql.operator.PageAssertions;
import io.prestosql.operator.PipelineExecutionStrategy;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeFactory;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeSinkFactory;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeSinkFactoryId;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static io.airlift.testing.Assertions.assertContains;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.operator.PipelineExecutionStrategy.GROUPED_EXECUTION;
import static io.prestosql.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestLocalExchange
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);
    private static final DataSize RETAINED_PAGE_SIZE = new DataSize(createPage(42).getRetainedSizeInBytes(), BYTE);
    private static final DataSize LOCAL_EXCHANGE_MAX_BUFFERED_BYTES = new DataSize(32, DataSize.Unit.MEGABYTE);

    @DataProvider
    public static Object[][] executionStrategy()
    {
        return new Object[][] {{UNGROUPED_EXECUTION}, {GROUPED_EXECUTION}};
    }

    @Test(dataProvider = "executionStrategy")
    public void testGatherSingleWriter(PipelineExecutionStrategy executionStrategy)
    {
        LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                SINGLE_DISTRIBUTION,
                8,
                TYPES,
                ImmutableList.of(),
                Optional.empty(),
                executionStrategy,
                new DataSize(retainedSizeOfPages(99), BYTE));
        LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        run(localExchangeFactory, executionStrategy, exchange -> {
            assertEquals(exchange.getBufferCount(), 1);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.getSinkFactory(localExchangeSinkFactoryId);

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
            assertExchangeTotalBufferedBytes(exchange, 1);

            assertSource(source, 1);

            sink.addPage(createPage(1));
            assertSource(source, 2);
            assertExchangeTotalBufferedBytes(exchange, 2);

            assertRemovePage(source, createPage(0));
            assertSource(source, 1);
            assertExchangeTotalBufferedBytes(exchange, 1);

            assertRemovePage(source, createPage(1));
            assertSource(source, 0);
            assertExchangeTotalBufferedBytes(exchange, 0);

            sink.addPage(createPage(2));
            sink.addPage(createPage(3));
            assertSource(source, 2);
            assertExchangeTotalBufferedBytes(exchange, 2);

            sink.finish();
            assertSinkFinished(sink);

            assertSource(source, 2);

            assertRemovePage(source, createPage(2));
            assertSource(source, 1);
            assertSinkFinished(sink);
            assertExchangeTotalBufferedBytes(exchange, 1);

            assertRemovePage(source, createPage(3));
            assertSourceFinished(source);
            assertExchangeTotalBufferedBytes(exchange, 0);
        });
    }

    @Test(dataProvider = "executionStrategy")
    public void testBroadcast(PipelineExecutionStrategy executionStrategy)
    {
        LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                FIXED_BROADCAST_DISTRIBUTION,
                2,
                TYPES,
                ImmutableList.of(),
                Optional.empty(),
                executionStrategy,
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES);
        LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        run(localExchangeFactory, executionStrategy, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.getSinkFactory(localExchangeSinkFactoryId);
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
            assertExchangeTotalBufferedBytes(exchange, 1);

            sinkA.addPage(createPage(0));

            assertSource(sourceA, 2);
            assertSource(sourceB, 2);
            assertExchangeTotalBufferedBytes(exchange, 2);

            assertRemovePage(sourceA, createPage(0));
            assertSource(sourceA, 1);
            assertSource(sourceB, 2);
            assertExchangeTotalBufferedBytes(exchange, 2);

            assertRemovePage(sourceA, createPage(0));
            assertSource(sourceA, 0);
            assertSource(sourceB, 2);
            assertExchangeTotalBufferedBytes(exchange, 2);

            sinkA.finish();
            assertSinkFinished(sinkA);
            assertExchangeTotalBufferedBytes(exchange, 2);

            sinkB.addPage(createPage(0));
            assertSource(sourceA, 1);
            assertSource(sourceB, 3);
            assertExchangeTotalBufferedBytes(exchange, 3);

            sinkB.finish();
            assertSinkFinished(sinkB);
            assertSource(sourceA, 1);
            assertSource(sourceB, 3);
            assertExchangeTotalBufferedBytes(exchange, 3);

            assertRemovePage(sourceA, createPage(0));
            assertSourceFinished(sourceA);
            assertSource(sourceB, 3);
            assertExchangeTotalBufferedBytes(exchange, 3);

            assertRemovePage(sourceB, createPage(0));
            assertRemovePage(sourceB, createPage(0));
            assertSourceFinished(sourceA);
            assertSource(sourceB, 1);
            assertExchangeTotalBufferedBytes(exchange, 1);

            assertRemovePage(sourceB, createPage(0));
            assertSourceFinished(sourceA);
            assertSourceFinished(sourceB);
            assertExchangeTotalBufferedBytes(exchange, 0);
        });
    }

    @Test(dataProvider = "executionStrategy")
    public void testRandom(PipelineExecutionStrategy executionStrategy)
    {
        LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                FIXED_ARBITRARY_DISTRIBUTION,
                2,
                TYPES,
                ImmutableList.of(),
                Optional.empty(),
                executionStrategy,
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES);
        LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        run(localExchangeFactory, executionStrategy, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.getSinkFactory(localExchangeSinkFactoryId);
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
                assertExchangeTotalBufferedBytes(exchange, i + 1);

                LocalExchangeBufferInfo bufferInfoA = sourceA.getBufferInfo();
                LocalExchangeBufferInfo bufferInfoB = sourceB.getBufferInfo();
                assertEquals(bufferInfoA.getBufferedBytes() + bufferInfoB.getBufferedBytes(), retainedSizeOfPages(i + 1));
                assertEquals(bufferInfoA.getBufferedPages() + bufferInfoB.getBufferedPages(), i + 1);
            }

            // we should get ~50 pages per source, but we should get at least some pages in each buffer
            assertTrue(sourceA.getBufferInfo().getBufferedPages() > 0);
            assertTrue(sourceB.getBufferInfo().getBufferedPages() > 0);
            assertExchangeTotalBufferedBytes(exchange, 100);
        });
    }

    @Test(dataProvider = "executionStrategy")
    public void testPassthrough(PipelineExecutionStrategy executionStrategy)
    {
        LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                FIXED_PASSTHROUGH_DISTRIBUTION,
                2,
                TYPES,
                ImmutableList.of(),
                Optional.empty(),
                executionStrategy,
                new DataSize(retainedSizeOfPages(1), BYTE));

        LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        run(localExchangeFactory, executionStrategy, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.getSinkFactory(localExchangeSinkFactoryId);
            LocalExchangeSink sinkA = sinkFactory.createSink();
            LocalExchangeSink sinkB = sinkFactory.createSink();
            assertSinkCanWrite(sinkA);
            assertSinkCanWrite(sinkB);
            sinkFactory.close();
            sinkFactory.noMoreSinkFactories();

            LocalExchangeSource sourceA = exchange.getSource(0);
            assertSource(sourceA, 0);

            LocalExchangeSource sourceB = exchange.getSource(1);
            assertSource(sourceB, 0);

            sinkA.addPage(createPage(0));
            assertSource(sourceA, 1);
            assertSource(sourceB, 0);
            assertSinkWriteBlocked(sinkA);

            assertSinkCanWrite(sinkB);
            sinkB.addPage(createPage(1));
            assertSource(sourceA, 1);
            assertSource(sourceB, 1);
            assertSinkWriteBlocked(sinkA);

            assertExchangeTotalBufferedBytes(exchange, 2);

            assertRemovePage(sourceA, createPage(0));
            assertSource(sourceA, 0);
            assertSinkCanWrite(sinkA);
            assertSinkWriteBlocked(sinkB);
            assertExchangeTotalBufferedBytes(exchange, 1);

            sinkA.finish();
            assertSinkFinished(sinkA);
            assertSource(sourceB, 1);

            sourceA.finish();
            sourceB.finish();
            assertRemovePage(sourceB, createPage(1));
            assertSourceFinished(sourceA);
            assertSourceFinished(sourceB);

            assertSinkFinished(sinkB);
            assertExchangeTotalBufferedBytes(exchange, 0);
        });
    }

    @Test(dataProvider = "executionStrategy")
    public void testPartition(PipelineExecutionStrategy executionStrategy)
    {
        LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                FIXED_HASH_DISTRIBUTION,
                2,
                TYPES,
                ImmutableList.of(0),
                Optional.empty(),
                executionStrategy,
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES);
        LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        run(localExchangeFactory, executionStrategy, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.getSinkFactory(localExchangeSinkFactoryId);
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
            assertTrue(exchange.getBufferedBytes() >= retainedSizeOfPages(1));

            sink.addPage(createPage(0));

            assertSource(sourceA, 2);
            assertSource(sourceB, 2);
            assertTrue(exchange.getBufferedBytes() >= retainedSizeOfPages(2));

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
            assertExchangeTotalBufferedBytes(exchange, 0);
        });
    }

    @Test(dataProvider = "executionStrategy")
    public void writeUnblockWhenAllReadersFinish(PipelineExecutionStrategy executionStrategy)
    {
        ImmutableList<Type> types = ImmutableList.of(BIGINT);

        LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                FIXED_BROADCAST_DISTRIBUTION,
                2,
                types,
                ImmutableList.of(),
                Optional.empty(),
                executionStrategy,
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES);
        LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        run(localExchangeFactory, executionStrategy, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.getSinkFactory(localExchangeSinkFactoryId);
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
        });
    }

    @Test(dataProvider = "executionStrategy")
    public void writeUnblockWhenAllReadersFinishAndPagesConsumed(PipelineExecutionStrategy executionStrategy)
    {
        LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                FIXED_BROADCAST_DISTRIBUTION,
                2,
                TYPES,
                ImmutableList.of(),
                Optional.empty(),
                executionStrategy,
                new DataSize(1, BYTE));
        LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        run(localExchangeFactory, executionStrategy, exchange -> {
            assertEquals(exchange.getBufferCount(), 2);
            assertExchangeTotalBufferedBytes(exchange, 0);

            LocalExchangeSinkFactory sinkFactory = exchange.getSinkFactory(localExchangeSinkFactoryId);
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
            assertExchangeTotalBufferedBytes(exchange, 1);

            sourceA.finish();
            assertSource(sourceA, 1);
            assertRemovePage(sourceA, createPage(0));
            assertSourceFinished(sourceA);
            assertExchangeTotalBufferedBytes(exchange, 1);

            assertSource(sourceB, 1);
            assertSinkWriteBlocked(sinkA);
            assertSinkWriteBlocked(sinkB);

            sourceB.finish();
            assertSource(sourceB, 1);
            assertRemovePage(sourceB, createPage(0));
            assertSourceFinished(sourceB);
            assertExchangeTotalBufferedBytes(exchange, 0);

            assertTrue(sinkAFuture.isDone());
            assertTrue(sinkBFuture.isDone());

            assertSinkFinished(sinkA);
            assertSinkFinished(sinkB);
        });
    }

    @Test
    public void testMismatchedExecutionStrategy()
    {
        // If sink/source didn't create a matching set of exchanges, operators will block forever,
        // waiting for the other half that will never show up.
        // The most common reason of mismatch is when one of sink/source created the wrong kind of local exchange.
        // In such case, we want to fail loudly.
        LocalExchangeFactory ungroupedLocalExchangeFactory = new LocalExchangeFactory(
                FIXED_HASH_DISTRIBUTION,
                2,
                TYPES,
                ImmutableList.of(0),
                Optional.empty(),
                UNGROUPED_EXECUTION,
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES);
        try {
            ungroupedLocalExchangeFactory.getLocalExchange(Lifespan.driverGroup(3));
            fail("expected failure");
        }
        catch (IllegalArgumentException e) {
            assertContains(e.getMessage(), "Driver-group exchange cannot be created.");
        }

        LocalExchangeFactory groupedLocalExchangeFactory = new LocalExchangeFactory(
                FIXED_HASH_DISTRIBUTION,
                2,
                TYPES,
                ImmutableList.of(0),
                Optional.empty(),
                GROUPED_EXECUTION,
                LOCAL_EXCHANGE_MAX_BUFFERED_BYTES);
        try {
            groupedLocalExchangeFactory.getLocalExchange(Lifespan.taskWide());
            fail("expected failure");
        }
        catch (IllegalArgumentException e) {
            assertContains(e.getMessage(), "Task-wide exchange cannot be created.");
        }
    }

    private void run(LocalExchangeFactory localExchangeFactory, PipelineExecutionStrategy pipelineExecutionStrategy, Consumer<LocalExchange> test)
    {
        switch (pipelineExecutionStrategy) {
            case UNGROUPED_EXECUTION:
                test.accept(localExchangeFactory.getLocalExchange(Lifespan.taskWide()));
                break;
            case GROUPED_EXECUTION:
                test.accept(localExchangeFactory.getLocalExchange(Lifespan.driverGroup(1)));
                test.accept(localExchangeFactory.getLocalExchange(Lifespan.driverGroup(12)));
                test.accept(localExchangeFactory.getLocalExchange(Lifespan.driverGroup(23)));
                break;
            default:
                throw new IllegalArgumentException("Unknown pipelineExecutionStrategy");
        }
    }

    private static void assertSource(LocalExchangeSource source, int pageCount)
    {
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
        assertTrue(source.waitForReading().isDone());
        Page actualPage = source.removePage();
        assertNotNull(actualPage);

        assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
        PageAssertions.assertPageEquals(TYPES, actualPage, expectedPage);
    }

    private static void assertPartitionedRemovePage(LocalExchangeSource source, int partition, int partitionCount)
    {
        assertTrue(source.waitForReading().isDone());
        Page page = source.removePage();
        assertNotNull(page);

        LocalPartitionGenerator partitionGenerator = new LocalPartitionGenerator(new InterpretedHashGenerator(TYPES, new int[] {0}), partitionCount);
        for (int position = 0; position < page.getPositionCount(); position++) {
            assertEquals(partitionGenerator.getPartition(page, position), partition);
        }
    }

    private static void assertSinkCanWrite(LocalExchangeSink sink)
    {
        assertFalse(sink.isFinished());
        assertTrue(sink.waitForWriting().isDone());
    }

    private static ListenableFuture<?> assertSinkWriteBlocked(LocalExchangeSink sink)
    {
        assertFalse(sink.isFinished());
        ListenableFuture<?> writeFuture = sink.waitForWriting();
        assertFalse(writeFuture.isDone());
        return writeFuture;
    }

    private static void assertSinkFinished(LocalExchangeSink sink)
    {
        assertTrue(sink.isFinished());
        assertTrue(sink.waitForWriting().isDone());

        // this will be ignored
        sink.addPage(createPage(0));
        assertTrue(sink.isFinished());
        assertTrue(sink.waitForWriting().isDone());
    }

    private static void assertExchangeTotalBufferedBytes(LocalExchange exchange, int pageCount)
    {
        assertEquals(exchange.getBufferedBytes(), retainedSizeOfPages(pageCount));
    }

    private static Page createPage(int i)
    {
        return SequencePageBuilder.createSequencePage(TYPES, 100, i);
    }

    public static long retainedSizeOfPages(int count)
    {
        return RETAINED_PAGE_SIZE.toBytes() * count;
    }
}
