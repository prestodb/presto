/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.server.QueryDriversTupleStream.QueryDriversBlockIterator;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.facebook.presto.block.Blocks.createStringBlock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
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
            Cursor cursor = tupleStream.cursor();
            while (cursor.advanceNextPosition()) {
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

            QueryDriversTupleStream tupleStream = new QueryDriversTupleStream(TupleInfo.SINGLE_VARBINARY, 10,
                    new StaticQueryDriverProvider(executor, blocks),
                    new StaticQueryDriverProvider(executor, blocks),
                    new StaticQueryDriverProvider(executor, blocks)
            );


            int count = 0;
            QueryDriversBlockIterator iterator = tupleStream.iterator();
            while (count < 20 && iterator.hasNext()) {
                iterator.next();
                count++;
            }
            assertEquals(count, 20);

            // verify we have more data
            assertTrue(iterator.hasNext());

            // cancel the iterator
            iterator.cancel();

            // due to buffering in the iterator, we still have one more element, so read it and
            // verify there are no more elements
            assertTrue(iterator.hasNext());
            assertNotNull(iterator.next());
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

        private StaticQueryDriverProvider(ExecutorService executor, List<UncompressedBlock> blocks)
        {
            this.executor = executor;
            this.blocks = blocks;
        }

        @Override
        public QueryDriver create(QueryState queryState, TupleInfo info)
        {
            return new StaticQueryDriver(executor, queryState, blocks);
        }
    }

    private class StaticQueryDriver implements QueryDriver
    {
        private final ExecutorService executor;
        private final QueryState queryState;
        private final List<UncompressedBlock> blocks;
        private Future<?> jobFuture;

        public StaticQueryDriver(ExecutorService executor, QueryState queryState, List<UncompressedBlock> blocks)
        {
            this.executor = executor;
            this.queryState = queryState;
            this.blocks = blocks;
        }

        @Override
        public void start()
        {
            AddBlocksJob job = new AddBlocksJob(queryState, blocks);
            jobFuture = executor.submit(job);
        }

        @Override
        public boolean isDone()
        {
            return jobFuture.isDone();
        }

        @Override
        public void cancel()
        {
            jobFuture.cancel(true);
        }
    }

    private static class AddBlocksJob implements Runnable
    {
        private final QueryState queryState;
        private final List<UncompressedBlock> blocks;

        private AddBlocksJob(QueryState queryState, List<UncompressedBlock> blocks)
        {
            this.queryState = queryState;
            this.blocks = ImmutableList.copyOf(blocks);
        }

        @Override
        public void run()
        {
            for (UncompressedBlock block : blocks) {
                try {
                    queryState.addBlock(block);
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
