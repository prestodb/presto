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

import com.facebook.presto.hive.InternalHiveSplit.InternalHiveBlock;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.List;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.testing.Assertions.assertContains;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestHiveSplitSource
{
    @Test
    public void testOutstandingSplitCount()
            throws Exception
    {
        HiveSplitSource hiveSplitSource = new HiveSplitSource(
                SESSION,
                "database",
                "table",
                TupleDomain.all(),
                10,
                10,
                new DataSize(32, MEGABYTE),
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat());

        // add 10 splits
        for (int i = 0; i < 10; i++) {
            hiveSplitSource.addToQueue(new TestSplit(i));
            assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), i + 1);
        }

        // remove 1 split
        assertEquals(getFutureValue(hiveSplitSource.getNextBatch(1)).size(), 1);
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 9);

        // remove 4 splits
        assertEquals(getFutureValue(hiveSplitSource.getNextBatch(4)).size(), 4);
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 5);

        // try to remove 20 splits, and verify we only got 5
        assertEquals(getFutureValue(hiveSplitSource.getNextBatch(20)).size(), 5);
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 0);
    }

    @Test
    public void testFail()
            throws Exception
    {
        HiveSplitSource hiveSplitSource = new HiveSplitSource(
                SESSION,
                "database",
                "table",
                TupleDomain.all(),
                10,
                10,
                new DataSize(32, MEGABYTE),
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat());

        // add some splits
        for (int i = 0; i < 5; i++) {
            hiveSplitSource.addToQueue(new TestSplit(i));
            assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), i + 1);
        }

        // remove a split and verify
        assertEquals(getFutureValue(hiveSplitSource.getNextBatch(1)).size(), 1);
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 4);

        // fail source
        hiveSplitSource.fail(new RuntimeException("test"));
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 4);

        // try to remove a split and verify we got the expected exception
        try {
            getFutureValue(hiveSplitSource.getNextBatch(1));
            fail("expected RuntimeException");
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "test");
        }
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 3);

        // attempt to add another split and verify it does not work
        hiveSplitSource.addToQueue(new TestSplit(99));
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 3);

        // fail source again
        hiveSplitSource.fail(new RuntimeException("another failure"));
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 3);

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
        final HiveSplitSource hiveSplitSource = new HiveSplitSource(
                SESSION,
                "database",
                "table",
                TupleDomain.all(),
                10,
                10,
                new DataSize(1, MEGABYTE),
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat());

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
            ConnectorSplit split = splits.get(800, TimeUnit.MILLISECONDS);
            assertEquals(((HiveSplit) split).getSchema().getProperty("id"), "33");
        }
        finally {
            // make sure the thread exits
            getterThread.interrupt();
        }
    }

    @Test(enabled = false)
    public void testOutstandingSplitSize()
            throws Exception
    {
        DataSize maxOutstandingSplitsSize = new DataSize(1, MEGABYTE);
        HiveSplitSource hiveSplitSource = new HiveSplitSource(
                SESSION,
                "database",
                "table",
                TupleDomain.all(),
                10,
                10000,
                maxOutstandingSplitsSize,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat());
        InternalHiveSplit testSplit = new InternalHiveSplit(
                "partition-name",
                "path",
                0,
                100,
                100,
                new Properties(),
                ImmutableList.of(new HivePartitionKey("pk_col", "pk_value")),
                ImmutableList.of(new InternalHiveBlock(0, 100, ImmutableList.of(HostAddress.fromString("localhost")))),
                OptionalInt.empty(),
                true,
                false,
                ImmutableMap.of());
        int testSplitSizeInBytes = testSplit.getEstimatedSizeInBytes();

        int maxSplitCount = toIntExact(maxOutstandingSplitsSize.toBytes()) / testSplitSizeInBytes;
        for (int i = 0; i < maxSplitCount; i++) {
            hiveSplitSource.addToQueue(testSplit);
            assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), i + 1);
        }

        assertEquals(getFutureValue(hiveSplitSource.getNextBatch(maxSplitCount)).size(), maxSplitCount);

        for (int i = 0; i < maxSplitCount; i++) {
            hiveSplitSource.addToQueue(testSplit);
            assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), i + 1);
        }
        try {
            hiveSplitSource.addToQueue(testSplit);
            fail("expect failure");
        }
        catch (PrestoException e) {
            assertContains(e.getMessage(), "Split buffering for database.table exceeded memory limit");
        }
    }

    private static class TestingHiveSplitLoader
            implements HiveSplitLoader
    {
        @Override
        public void start(HiveSplitSource splitSource)
        {
        }

        @Override
        public void stop()
        {
        }
    }

    private static class TestSplit
            extends InternalHiveSplit
    {
        private TestSplit(int id)
        {
            super(
                    "partition-name",
                    "path",
                    0,
                    100,
                    100,
                    properties("id", String.valueOf(id)),
                    ImmutableList.of(),
                    ImmutableList.of(new InternalHiveBlock(0, 100, ImmutableList.of())),
                    OptionalInt.empty(),
                    true,
                    false,
                    ImmutableMap.of());
        }

        private static Properties properties(String key, String value)
        {
            Properties properties = new Properties();
            properties.put(key, value);
            return properties;
        }
    }
}
