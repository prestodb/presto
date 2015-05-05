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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.google.common.util.concurrent.SettableFuture;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestHiveSplitSource
{
    @Test
    public void testOutstandingSplitCount()
            throws Exception
    {
        HiveSplitSource hiveSplitSource = new HiveSplitSource("test", 10, new TestingHiveSplitLoader());

        // add 10 splits
        for (int i = 0; i < 10; i++) {
            hiveSplitSource.addToQueue(new TestSplit(i));
            assertEquals(hiveSplitSource.getOutstandingSplitCount(), i + 1);
        }

        // remove 1 split
        assertEquals(getFutureValue(hiveSplitSource.getNextBatch(1)).size(), 1);
        assertEquals(hiveSplitSource.getOutstandingSplitCount(), 9);

        // remove 4 splits
        assertEquals(getFutureValue(hiveSplitSource.getNextBatch(4)).size(), 4);
        assertEquals(hiveSplitSource.getOutstandingSplitCount(), 5);

        // try to remove 20 splits, and verify we only got 5
        assertEquals(getFutureValue(hiveSplitSource.getNextBatch(20)).size(), 5);
        assertEquals(hiveSplitSource.getOutstandingSplitCount(), 0);
    }

    @Test
    public void testSuspendResume()
            throws Exception
    {
        TestingHiveSplitLoader splitLoader = new TestingHiveSplitLoader();
        HiveSplitSource hiveSplitSource = new HiveSplitSource("test", 10, splitLoader);

        // almost fill the source
        for (int i = 0; i < 9; i++) {
            hiveSplitSource.addToQueue(new TestSplit(i));
            assertEquals(hiveSplitSource.getOutstandingSplitCount(), i + 1);
            assertFalse(splitLoader.isResumed());
        }

        // add one more split so the source is now full
        hiveSplitSource.addToQueue(new TestSplit(10));
        assertEquals(hiveSplitSource.getOutstandingSplitCount(), 10);
        assertFalse(splitLoader.isResumed());

        // remove one split so the source is no longer full and verify the loader is resumed
        assertEquals(getFutureValue(hiveSplitSource.getNextBatch(1)).size(), 1);
        assertEquals(hiveSplitSource.getOutstandingSplitCount(), 9);
        assertTrue(splitLoader.isResumed());
    }

    @Test
    public void testFail()
            throws Exception
    {
        HiveSplitSource hiveSplitSource = new HiveSplitSource("test", 10, new TestingHiveSplitLoader());

        // add some splits
        for (int i = 0; i < 5; i++) {
            hiveSplitSource.addToQueue(new TestSplit(i));
            assertEquals(hiveSplitSource.getOutstandingSplitCount(), i + 1);
        }

        // remove a split and verify
        assertEquals(getFutureValue(hiveSplitSource.getNextBatch(1)).size(), 1);
        assertEquals(hiveSplitSource.getOutstandingSplitCount(), 4);

        // fail source
        hiveSplitSource.fail(new RuntimeException("test"));
        assertEquals(hiveSplitSource.getOutstandingSplitCount(), 4);

        // try to remove a split and verify we got the expected exception
        try {
            getFutureValue(hiveSplitSource.getNextBatch(1));
            fail("expected RuntimeException");
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "test");
        }
        assertEquals(hiveSplitSource.getOutstandingSplitCount(), 4);

        // attempt to add another split and verify it does not work
        hiveSplitSource.addToQueue(new TestSplit(99));
        assertEquals(hiveSplitSource.getOutstandingSplitCount(), 4);

        // fail source again
        hiveSplitSource.fail(new RuntimeException("another failure"));
        assertEquals(hiveSplitSource.getOutstandingSplitCount(), 4);

        // try to remove a split and verify we got the first exception
        try {
            getFutureValue(hiveSplitSource.getNextBatch(1));
            fail("expected RuntimeException");
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "test");
        }
    }

    @Test
    public void testReaderWaitsForSplits()
            throws Exception
    {
        final HiveSplitSource hiveSplitSource = new HiveSplitSource("test", 10, new TestingHiveSplitLoader());

        final SettableFuture<ConnectorSplit> splits = SettableFuture.create();

        // create a thread that will get a split
        final CountDownLatch started = new CountDownLatch(1);
        Thread getterThread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    started.countDown();
                    List<ConnectorSplit> batch = getFutureValue(hiveSplitSource.getNextBatch(1));
                    assertEquals(batch.size(), 1);
                    splits.set(batch.get(0));
                }
                catch (Throwable e) {
                    splits.setException(e);
                }
            }
        });
        getterThread.start();

        try {
            // wait for the thread to be started
            assertTrue(started.await(1, TimeUnit.SECONDS));

            // sleep for a bit, and assure the thread is blocked
            TimeUnit.MILLISECONDS.sleep(200);
            assertTrue(!splits.isDone());

            // add a split
            hiveSplitSource.addToQueue(new TestSplit(33));

            // wait for thread to get the split
            ConnectorSplit split = splits.get(200, TimeUnit.MILLISECONDS);
            assertSame(split.getInfo(), 33);
        }
        finally {
            // make sure the thread exits
            getterThread.interrupt();
        }
    }

    private static class TestingHiveSplitLoader
            implements HiveSplitLoader
    {
        private boolean resumed;

        @Override
        public void start(HiveSplitSource splitSource)
        {
        }

        @Override
        public void resume()
        {
            resumed = true;
        }

        @Override
        public void stop()
        {
        }

        public boolean isResumed()
        {
            return resumed;
        }
    }

    private static class TestSplit
            implements ConnectorSplit
    {
        private final int id;

        private TestSplit(int id)
        {
            this.id = id;
        }

        @Override
        public boolean isRemotelyAccessible()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getInfo()
        {
            return id;
        }
    }
}
