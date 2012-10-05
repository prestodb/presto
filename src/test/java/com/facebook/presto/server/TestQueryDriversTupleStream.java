/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.server.QueryDriversTupleStream.QueryDriversBlockIterator;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.block.Blocks.createStringBlock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestQueryDriversTupleStream
{
    @Test
    public void testNormalExecution()
            throws Exception
    {
        List<UncompressedBlock> blocks = createBlocks();
        int expectedCount = 0;
        for (UncompressedBlock block : blocks) {
            expectedCount += block.getCount();
        }

        ExecutorService executor = Executors.newCachedThreadPool();
        try {

            QueryDriversTupleStream tupleStream = new QueryDriversTupleStream(TupleInfo.SINGLE_VARBINARY, 10,
                    new StaticQueryDriverProvider(executor, blocks),
                    new StaticQueryDriverProvider(executor, blocks),
                    new StaticQueryDriverProvider(executor, blocks)
            );
            assertEquals(tupleStream.getTupleInfo(), TupleInfo.SINGLE_VARBINARY);

            int count = 0;
            Cursor cursor = tupleStream.cursor(new QuerySession());
            while (Cursors.advanceNextPositionNoYield(cursor)) {
                count++;
            }
            assertEquals(count, expectedCount * 3);
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testCancel()
            throws Exception
    {
        List<UncompressedBlock> blocks = createBlocks();
        ExecutorService executor = Executors.newCachedThreadPool();
        try {

            StaticQueryDriverProvider provider = new StaticQueryDriverProvider(executor, blocks);
            QueryDriversTupleStream tupleStream = new QueryDriversTupleStream(TupleInfo.SINGLE_VARBINARY, 1, provider, provider, provider);

            int count = 0;
            QueryDriversBlockIterator iterator = tupleStream.iterator(new QuerySession());
            while (count < 20 && iterator.hasNext()) {
                iterator.next();
                count++;
            }
            assertEquals(count, 20);

            // verify we have more data
            assertTrue(iterator.hasNext());

            // verify all producers are not finished
            IdentityHashMap<StaticQueryDriver, Integer> driverBlocksAdded = new IdentityHashMap<>();
            for (StaticQueryDriver driver : provider.getCreatedDrivers()) {
                assertFalse(driver.isDone());
                driverBlocksAdded.put(driver, driver.getBlocksAdded());
            }

            // cancel the iterator
            iterator.cancel();

            // verify all producers are finished, and no additional blocks have been added
            for (StaticQueryDriver driver : provider.getCreatedDrivers()) {
                assertTrue(driver.isDone());
                assertEquals(driver.getBlocksAdded(), (int) driverBlocksAdded.get(driver));
            }

            // drain any blocks buffered in the iterator implementation
            while (iterator.hasNext()) {
                iterator.next();
            }
            assertFalse(iterator.hasNext());
        }
        finally {
            executor.shutdownNow();
        }
    }

    private List<UncompressedBlock> createBlocks()
    {
        ImmutableList.Builder<UncompressedBlock> blocks = ImmutableList.builder();
        List<String> data = ImmutableList.of("apple", "banana", "cherry", "date");
        for (int i = 0; i < 12; i++) {
            UncompressedBlock block = createStringBlock(0, Iterables.concat(Collections.nCopies(i + 1, data)));
            blocks.add(block);
        }
        return blocks.build();
    }

    private class StaticQueryDriverProvider implements QueryDriverProvider
    {
        private final ExecutorService executor;
        private final List<UncompressedBlock> blocks;
        private final List<StaticQueryDriver> createdDrivers = new ArrayList<>();

        private StaticQueryDriverProvider(ExecutorService executor, List<UncompressedBlock> blocks)
        {
            this.executor = executor;
            this.blocks = blocks;
        }

        public List<StaticQueryDriver> getCreatedDrivers()
        {
            return ImmutableList.copyOf(createdDrivers);
        }

        @Override
        public QueryDriver create(QueryState queryState, TupleInfo info)
        {
            StaticQueryDriver driver = new StaticQueryDriver(executor, queryState, blocks);
            createdDrivers.add(driver);
            return driver;
        }
    }

    private class StaticQueryDriver implements QueryDriver
    {
        private final ExecutorService executor;
        private final QueryState queryState;
        private final List<UncompressedBlock> blocks;
        private Future<?> jobFuture;
        private AddBlocksJob job;

        public StaticQueryDriver(ExecutorService executor, QueryState queryState, List<UncompressedBlock> blocks)
        {
            this.executor = executor;
            this.queryState = queryState;
            this.blocks = blocks;
        }

        public synchronized int getBlocksAdded()
        {
            return job.getBlocksAdded();
        }

        @Override
        public synchronized void start()
        {
            job = new AddBlocksJob(queryState, blocks);
            jobFuture = executor.submit(job);
        }

        @Override
        public synchronized boolean isDone()
        {
            return jobFuture.isDone();
        }

        @Override
        public synchronized void cancel()
        {
            jobFuture.cancel(true);
        }
    }

    private static class AddBlocksJob implements Runnable
    {
        private final QueryState queryState;
        private final List<UncompressedBlock> blocks;
        private final AtomicInteger blocksAdded = new AtomicInteger();

        private AddBlocksJob(QueryState queryState, List<UncompressedBlock> blocks)
        {
            this.queryState = queryState;
            this.blocks = ImmutableList.copyOf(blocks);
        }

        public int getBlocksAdded()
        {
            return blocksAdded.get();
        }

        @Override
        public void run()
        {
            for (UncompressedBlock block : blocks) {
                try {
                    if (queryState.addBlock(block)) {
                        blocksAdded.incrementAndGet();
                    }
                }
                catch (InterruptedException e) {
                    queryState.queryFailed(e);
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                }
            }
            queryState.sourceFinished();
        }
    }
}
