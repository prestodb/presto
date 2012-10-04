/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.block.Blocks;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestQueryState
{
    private ExecutorService executor;

    @BeforeMethod
    protected void setUp()
            throws Exception
    {
        executor = Executors.newCachedThreadPool();
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        executor.shutdownNow();
    }

    @Test
    public void testInvalidConstruction()
            throws Exception
    {
        try {
            new QueryState(0, 4);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {

        }
        try {
            new QueryState(4, 0);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testNormalExecution()
            throws Exception
    {
        QueryState queryState = new QueryState(1, 20);
        assertRunning(queryState);

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            queryState.addBlock(Blocks.createLongsBlock(i, i));
        }

        // verify blocks are in correct order
        assertRunning(queryState);

        List<UncompressedBlock> nextBlocks = queryState.getNextBlocks(2);
        assertEquals(nextBlocks.size(), 2);
        assertEquals(getBlockOnlyValue(nextBlocks.get(0)), 0);
        assertEquals(getBlockOnlyValue(nextBlocks.get(1)), 1);

        assertRunning(queryState);

        nextBlocks = queryState.getNextBlocks(2);
        assertEquals(nextBlocks.size(), 2);
        assertEquals(getBlockOnlyValue(nextBlocks.get(0)), 2);
        assertEquals(getBlockOnlyValue(nextBlocks.get(1)), 3);

        assertRunning(queryState);

        nextBlocks = queryState.getNextBlocks(2);
        assertEquals(nextBlocks.size(), 1);
        assertEquals(getBlockOnlyValue(nextBlocks.get(0)), 4);

        assertRunning(queryState);

        // add one more block
        queryState.addBlock(Blocks.createLongsBlock(9, 9));

        // mark source as finished
        queryState.sourceFinished();

        assertRunning(queryState);

        // get the last block and assure the query is finished
        nextBlocks = queryState.getNextBlocks(2);
        assertEquals(nextBlocks.size(), 1);
        assertEquals(getBlockOnlyValue(nextBlocks.get(0)), 9);
        assertFinished(queryState);

        // attempt to add more blocks
        try {
            queryState.addBlock(Blocks.createLongsBlock(22, 22));
            fail("expected IllegalStateException");
        }
        catch (IllegalStateException e) {
        }
        assertFinished(queryState);

        // mark source as finished again
        queryState.sourceFinished();
        assertFinished(queryState);

        // try to fail the query and verify it doesn't work
        queryState.queryFailed(new RuntimeException());
        assertFinished(queryState);

        // try to cancel the query and verify it doesn't work
        queryState.cancel();
        assertFinished(queryState);
    }

    @Test
    public void testFailedExecution()
            throws Exception
    {
        QueryState queryState = new QueryState(1, 20);
        assertRunning(queryState);

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            queryState.addBlock(Blocks.createLongsBlock(i, i));
        }

        // verify blocks are in correct order
        assertRunning(queryState);

        List<UncompressedBlock> nextBlocks = queryState.getNextBlocks(2);
        assertEquals(nextBlocks.size(), 2);
        assertEquals(getBlockOnlyValue(nextBlocks.get(0)), 0);
        assertEquals(getBlockOnlyValue(nextBlocks.get(1)), 1);

        assertRunning(queryState);

        nextBlocks = queryState.getNextBlocks(2);
        assertEquals(nextBlocks.size(), 2);
        assertEquals(getBlockOnlyValue(nextBlocks.get(0)), 2);
        assertEquals(getBlockOnlyValue(nextBlocks.get(1)), 3);

        assertRunning(queryState);

        // Fail query with one block in the buffer
        RuntimeException exception = new RuntimeException("failed");
        queryState.queryFailed(exception);
        assertFailed(queryState, exception);

        // attempt to add more blocks
        queryState.addBlock(Blocks.createLongsBlock(22, 22));
        assertFailed(queryState, exception);

        // fail the query again
        RuntimeException anotherException = new RuntimeException("failed again");
        queryState.queryFailed(anotherException);
        assertFailed(queryState, exception, anotherException);

        // try to cancel the finished query and verify it doesn't work
        queryState.cancel();
        assertFailed(queryState, exception, anotherException);

        // try to cancel the query again and verify it doesn't work
        queryState.cancel();
        assertFailed(queryState, exception, anotherException);

        // try to finish the query and verify it doesn't work
        queryState.sourceFinished();
        assertFailed(queryState, exception, anotherException);
    }

    @Test
    public void testCanceledExecution()
            throws Exception
    {
        QueryState queryState = new QueryState(1, 20);
        assertRunning(queryState);

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            queryState.addBlock(Blocks.createLongsBlock(i, i));
        }

        // verify blocks are in correct order
        assertRunning(queryState);

        List<UncompressedBlock> nextBlocks = queryState.getNextBlocks(2);
        assertEquals(nextBlocks.size(), 2);
        assertEquals(getBlockOnlyValue(nextBlocks.get(0)), 0);
        assertEquals(getBlockOnlyValue(nextBlocks.get(1)), 1);

        assertRunning(queryState);

        nextBlocks = queryState.getNextBlocks(2);
        assertEquals(nextBlocks.size(), 2);
        assertEquals(getBlockOnlyValue(nextBlocks.get(0)), 2);
        assertEquals(getBlockOnlyValue(nextBlocks.get(1)), 3);

        assertRunning(queryState);

        // Cancel query with one block in the buffer
        queryState.cancel();
        assertCanceled(queryState);

        // attempt to add more blocks
        queryState.addBlock(Blocks.createLongsBlock(22, 22));
        assertCanceled(queryState);

        // cancel the query again
        queryState.cancel();
        assertCanceled(queryState);

        // try to fail the finished query and verify it doesn't work
        queryState.queryFailed(new RuntimeException());
        assertCanceled(queryState);

        // try to finish the query and verify it doesn't work
        queryState.sourceFinished();
        assertCanceled(queryState);
    }

    @Test
    public void testMultiSourceNormalExecution()
            throws Exception
    {
        QueryState queryState = new QueryState(3, 20);
        assertRunning(queryState);

        // add some blocks
        queryState.addBlock(Blocks.createLongsBlock(0, 0));
        queryState.addBlock(Blocks.createLongsBlock(1, 1));

        // verify blocks are in correct order
        assertRunning(queryState);

        List<UncompressedBlock> nextBlocks = queryState.getNextBlocks(2);
        assertEquals(nextBlocks.size(), 2);
        assertEquals(getBlockOnlyValue(nextBlocks.get(0)), 0);
        assertEquals(getBlockOnlyValue(nextBlocks.get(1)), 1);
        assertRunning(queryState);

        // finish first sources
        queryState.sourceFinished();
        assertRunning(queryState);

        // add one more block
        queryState.addBlock(Blocks.createLongsBlock(9, 9));

        // finish second source
        queryState.sourceFinished();
        assertRunning(queryState);

        // the block
        nextBlocks = queryState.getNextBlocks(2);
        assertEquals(nextBlocks.size(), 1);
        assertEquals(getBlockOnlyValue(nextBlocks.get(0)), 9);
        assertRunning(queryState);

        // finish last source, and verify the query is finished since there are no buffered blocks
        queryState.sourceFinished();
        assertFinished(queryState);

        // attempt to add more blocks
        try {
            queryState.addBlock(Blocks.createLongsBlock(22, 22));
            fail("expected IllegalStateException");
        }
        catch (IllegalStateException e) {
        }
        assertFinished(queryState);

        // mark source as finished again
        queryState.sourceFinished();
        assertFinished(queryState);

        // try to fail the query and verify it doesn't work
        queryState.queryFailed(new RuntimeException());
        assertFinished(queryState);

        // try to cancel the query and verify it doesn't work
        queryState.cancel();
        assertFinished(queryState);
    }

    @Test
    public void testBufferSizeNormal()
            throws Exception
    {
        QueryState queryState = new QueryState(1, 5);
        assertRunning(queryState);

        // exec thread to get two block
        GetBlocksJob getBlocksJob = new GetBlocksJob(queryState, 2, 1);
        executor.submit(getBlocksJob);
        getBlocksJob.waitForStarted();

        // "verify" thread is blocked
        getBlocksJob.assertBlockedWithCount(0);

        // add one block
        queryState.addBlock(Blocks.createLongsBlock(0, 0));

        // verify thread got one block and is blocked
        getBlocksJob.assertBlockedWithCount(1);

        // add one block
        queryState.addBlock(Blocks.createLongsBlock(1, 1));

        // verify thread is released
        getBlocksJob.waitForFinished();

        // verify thread got one block
        assertEquals(getBlocksJob.getBlocks().size(), 2);

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            queryState.addBlock(Blocks.createLongsBlock(i, i));
        }

        // exec thread to add two more block
        AddBlocksJob addBlocksJob = new AddBlocksJob(queryState, Blocks.createLongsBlock(2, 2), Blocks.createLongsBlock(3, 3));
        executor.submit(addBlocksJob);
        addBlocksJob.waitForStarted();

        // "verify" thread is blocked
        addBlocksJob.assertBlockedWithCount(2);

        // get one block
        assertEquals(queryState.getNextBlocks(1).size(), 1);

        // "verify" thread is blocked again with one remaining block
        addBlocksJob.assertBlockedWithCount(1);

        // get one block
        assertEquals(queryState.getNextBlocks(1).size(), 1);

        // verify thread is released
        addBlocksJob.waitForFinished();

        // verify thread added one block
        assertEquals(addBlocksJob.getBlocks().size(), 0);
    }

    @Test
    public void testCancelFreesReader()
            throws Exception
    {
        QueryState queryState = new QueryState(1, 5);
        assertRunning(queryState);

        ExecutorService executor = Executors.newCachedThreadPool();

        // exec thread to get two block
        GetBlocksJob getBlocksJob = new GetBlocksJob(queryState, 2, 1);
        executor.submit(getBlocksJob);
        getBlocksJob.waitForStarted();

        // "verify" thread is blocked
        getBlocksJob.assertBlockedWithCount(0);

        // add one block
        queryState.addBlock(Blocks.createLongsBlock(0, 0));

        // verify thread got one block and is blocked
        getBlocksJob.assertBlockedWithCount(1);

        // cancel the query
        queryState.cancel();
        assertCanceled(queryState);

        // verify thread is released
        getBlocksJob.waitForFinished();

        // verify thread only got one block
        assertEquals(getBlocksJob.getBlocks().size(), 1);
    }

    @Test
    public void testCancelFreesWriter()
            throws Exception
    {
        QueryState queryState = new QueryState(1, 5);
        assertRunning(queryState);

        ExecutorService executor = Executors.newCachedThreadPool();

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            queryState.addBlock(Blocks.createLongsBlock(i, i));
        }

        // exec thread to add two block
        AddBlocksJob addBlocksJob = new AddBlocksJob(queryState, Blocks.createLongsBlock(2, 2), Blocks.createLongsBlock(3, 3));
        executor.submit(addBlocksJob);
        addBlocksJob.waitForStarted();

        // "verify" thread is blocked
        addBlocksJob.assertBlockedWithCount(2);

        // get one block
        assertEquals(queryState.getNextBlocks(1).size(), 1);

        // "verify" thread is blocked again with one remaining block
        addBlocksJob.assertBlockedWithCount(1);

        // cancel the query
        queryState.cancel();
        assertCanceled(queryState);

        // verify thread is released
        addBlocksJob.waitForFinished();
    }

    @Test
    public void testFailFreesReader()
            throws Exception
    {
        QueryState queryState = new QueryState(1, 5);
        assertRunning(queryState);

        ExecutorService executor = Executors.newCachedThreadPool();

        // exec thread to get two block
        GetBlocksJob getBlocksJob = new GetBlocksJob(queryState, 2, 1);
        executor.submit(getBlocksJob);
        getBlocksJob.waitForStarted();

        // "verify" thread is blocked
        getBlocksJob.assertBlockedWithCount(0);

        // add one block
        queryState.addBlock(Blocks.createLongsBlock(0, 0));

        // verify thread got one block and is blocked
        getBlocksJob.assertBlockedWithCount(1);

        // fail the query
        RuntimeException exception = new RuntimeException("failed");
        queryState.queryFailed(exception);
        assertFailed(queryState, exception);

        // verify thread is released
        getBlocksJob.waitForFinished();

        // verify thread only got one block
        assertEquals(getBlocksJob.getBlocks().size(), 1);
        assertFailedQuery(getBlocksJob.getFailedQueryException(), exception);
    }

    @Test
    public void testFailFreesWriter()
            throws Exception
    {
        QueryState queryState = new QueryState(1, 5);
        assertRunning(queryState);

        ExecutorService executor = Executors.newCachedThreadPool();

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            queryState.addBlock(Blocks.createLongsBlock(i, i));
        }

        // exec thread to add two block
        AddBlocksJob addBlocksJob = new AddBlocksJob(queryState, Blocks.createLongsBlock(2, 2), Blocks.createLongsBlock(3, 3));
        executor.submit(addBlocksJob);
        addBlocksJob.waitForStarted();

        // "verify" thread is blocked
        addBlocksJob.assertBlockedWithCount(2);

        // get one block
        assertEquals(queryState.getNextBlocks(1).size(), 1);

        // "verify" thread is blocked again with one remaining block
        addBlocksJob.assertBlockedWithCount(1);

        // fail the query
        RuntimeException exception = new RuntimeException("failed");
        queryState.queryFailed(exception);
        assertFailed(queryState, exception);

        // verify thread is released
        addBlocksJob.waitForFinished();
    }

    private static class GetBlocksJob implements Runnable
    {
        private final QueryState queryState;
        private final int blocksToGet;
        private final int batchSize;

        private final AtomicReference<FailedQueryException> failedQueryException = new AtomicReference<>();

        private final CopyOnWriteArrayList<UncompressedBlock> blocks = new CopyOnWriteArrayList<>();
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch finished = new CountDownLatch(1);

        private GetBlocksJob(QueryState queryState, int blocksToGet, int batchSize)
        {
            this.queryState = queryState;
            this.blocksToGet = blocksToGet;
            this.batchSize = batchSize;
        }

        public List<UncompressedBlock> getBlocks()
        {
            return ImmutableList.copyOf(blocks);
        }

        public FailedQueryException getFailedQueryException()
        {
            return failedQueryException.get();
        }

        /**
         * Do our best to assure the thread is blocked.
         */
        public void assertBlockedWithCount(int expectedBlockSize)
        {
            // the best we can do is to verify the count hasn't changed in after sleeping for a bit

            assertTrue(isStarted());
            assertTrue(!isFinished());

            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

            assertEquals(blocks.size(), expectedBlockSize);
            assertTrue(isStarted());
            assertTrue(!isFinished());
        }

        private boolean isFinished()
        {
            return finished.getCount() == 0;
        }

        private boolean isStarted()
        {
            return started.getCount() == 0;
        }

        public void waitForStarted()
                throws InterruptedException
        {
            assertTrue(started.await(1, TimeUnit.SECONDS), "Job did not start with in 1 second");
        }

        public void waitForFinished()
                throws InterruptedException
        {
            assertTrue(finished.await(1, TimeUnit.SECONDS), "Job did not finish with in 1 second");
        }

        @Override
        public void run()
        {
            started.countDown();
            try {
                while (blocks.size() < blocksToGet) {
                    try {
                        List<UncompressedBlock> blocks = queryState.getNextBlocks(batchSize);
                        assertTrue(!blocks.isEmpty());
                        this.blocks.addAll(blocks);
                    }
                    catch (FailedQueryException e) {
                        failedQueryException.set(e);
                        break;
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw Throwables.propagate(e);
                    }
                }
            }
            finally {
                finished.countDown();
            }
        }
    }

    private static class AddBlocksJob implements Runnable
    {
        private final QueryState queryState;
        private final ArrayBlockingQueue<UncompressedBlock> blocks;

        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch finished = new CountDownLatch(1);

        private AddBlocksJob(QueryState queryState, UncompressedBlock... blocks)
        {
            this.queryState = queryState;
            this.blocks = new ArrayBlockingQueue<>(blocks.length);
            Collections.addAll(this.blocks, blocks);
        }

        public List<UncompressedBlock> getBlocks()
        {
            return ImmutableList.copyOf(blocks);
        }

        /**
         * Do our best to assure the thread is blocked.
         */
        public void assertBlockedWithCount(int expectedBlockSize)
        {
            // the best we can do is to verify the count hasn't changed in after sleeping for a bit

            assertTrue(isStarted());
            assertTrue(!isFinished());

            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

            assertEquals(blocks.size(), expectedBlockSize);
            assertTrue(isStarted());
            assertTrue(!isFinished());
        }

        private boolean isFinished()
        {
            return finished.getCount() == 0;
        }

        private boolean isStarted()
        {
            return started.getCount() == 0;
        }

        public void waitForStarted()
                throws InterruptedException
        {
            assertTrue(started.await(1, TimeUnit.SECONDS), "Job did not start with in 1 second");
        }

        public void waitForFinished()
                throws InterruptedException
        {
            assertTrue(finished.await(1, TimeUnit.SECONDS), "Job did not finish with in 1 second");
        }

        @Override
        public void run()
        {
            started.countDown();
            try {
                for (UncompressedBlock block = blocks.peek(); block != null; block = blocks.peek()) {
                    try {
                        queryState.addBlock(block);
                        assertNotNull(blocks.poll());
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw Throwables.propagate(e);
                    }
                }
            }
            finally {
                finished.countDown();
            }
        }
    }

    private void assertRunning(QueryState queryState)
    {
        assertFalse(queryState.isDone());
        assertFalse(queryState.isCanceled());
        assertFalse(queryState.isFailed());
    }

    private void assertFinished(QueryState queryState)
            throws Exception
    {
        assertTrue(queryState.isDone());
        assertFalse(queryState.isCanceled());
        assertFalse(queryState.isFailed());

        // getNextBlocks should return an empty list
        for (int loop = 0; loop < 5; loop++) {
            List<UncompressedBlock> nextBlocks = queryState.getNextBlocks(2);
            assertNotNull(nextBlocks);
            assertEquals(nextBlocks.size(), 0);
        }
    }

    private void assertFailed(QueryState queryState, Throwable... expectedCauses)
            throws Exception
    {
        assertTrue(queryState.isDone());
        assertFalse(queryState.isCanceled());
        assertTrue(queryState.isFailed());

        // getNextBlocks should throw an exception
        for (int loop = 0; loop < 5; loop++) {
            try {
                queryState.getNextBlocks(2);
                fail("expected FailedQueryException");
            }
            catch (FailedQueryException e) {
                assertFailedQuery(e, expectedCauses);
            }
        }
    }

    private void assertFailedQuery(FailedQueryException failedQueryException, Throwable... expectedCauses)
    {
        assertNotNull(failedQueryException);
        Throwable[] suppressed = failedQueryException.getSuppressed();
        assertEquals(suppressed.length, expectedCauses.length);
        for (int i = 0; i < suppressed.length; i++) {
            assertSame(suppressed[i], expectedCauses[i]);
        }
    }

    private void assertCanceled(QueryState queryState)
            throws Exception
    {
        assertTrue(queryState.isDone());
        assertTrue(queryState.isCanceled());
        assertFalse(queryState.isFailed());

        // getNextBlocks should return an empty list
        for (int loop = 0; loop < 5; loop++) {
            List<UncompressedBlock> nextBlocks = queryState.getNextBlocks(2);
            assertNotNull(nextBlocks);
            assertEquals(nextBlocks.size(), 0);
        }
    }

    private static long getBlockOnlyValue(UncompressedBlock block)
    {
        Cursor cursor = block.cursor();
        assertTrue(cursor.advanceNextPosition());
        long value = cursor.getLong(0);
        assertFalse(cursor.advanceNextPosition());
        return value;
    }
}
