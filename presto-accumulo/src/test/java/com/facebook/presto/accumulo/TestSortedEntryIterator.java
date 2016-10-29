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
package com.facebook.presto.accumulo;

import com.facebook.presto.accumulo.io.SortedEntryIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.testng.annotations.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static java.lang.Math.abs;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class TestSortedEntryIterator
{
    private static final int FILL_SIZE = 100_000;

    @Test
    public void testEmpty()
            throws Exception
    {
        assertScan(ImmutableList.of());
    }

    @Test
    public void testNumEntriesIsFillSize()
            throws Exception
    {
        List<Entry<Key, Value>> entries = new ArrayList<>();
        for (int i = 0; i < FILL_SIZE; ++i) {
            entries.add(new SimpleEntry<>(new Key(
                    new Text(uuid()),
                    new Text(uuid()),
                    new Text(uuid()),
                    new Text(uuid()),
                    0L),
                    new Value(uuid().getBytes(UTF_8))));
        }

        assertScan(entries);
    }

    @Test
    public void testNumEntriesIsLessThanFillSize()
            throws Exception
    {
        List<Entry<Key, Value>> entries = new ArrayList<>();
        for (int i = 0; i < FILL_SIZE / 10; ++i) {
            entries.add(new SimpleEntry<>(new Key(
                    new Text(uuid()),
                    new Text(uuid()),
                    new Text(uuid()),
                    new Text(uuid()),
                    0L),
                    new Value(uuid().getBytes(UTF_8))));
        }

        assertScan(entries);
    }

    @Test
    public void testNumEntriesIsGreaterThanFillSize()
            throws Exception
    {
        int numTasks = 10;
        // Generate random data from ten threads
        // Each thread generates keys that are increasing by row ID,
        // simulating how a BatchScanner reads data from multiple threads
        List<Entry<Key, Value>> entries = Collections.synchronizedList(new ArrayList<>());
        ExecutorService service = MoreExecutors.getExitingExecutorService(
                new ThreadPoolExecutor(
                        numTasks,
                        numTasks,
                        0,
                        SECONDS,
                        new SynchronousQueue<>()
                ));

        for (Future<Void> task : service.invokeAll(Collections.nCopies(numTasks, new GenerateOrderedData(entries)))) {
            task.get();
        }

        assertScan(entries);
    }

    private void assertScan(List<Entry<Key, Value>> entries)
            throws Exception
    {
        SortedEntryIterator iterator = new SortedEntryIterator(FILL_SIZE, entries.iterator());

        if (entries.isEmpty()) {
            assertFalse(iterator.hasNext());
        }
        else {
            assertTrue(iterator.hasNext());
            Entry<Key, Value> previousEntry = iterator.next();
            long numEntries = 1;
            while (iterator.hasNext()) {
                Entry<Key, Value> entry = iterator.next();
                assertTrue(
                        format("Entries are not correctly ordered, %s %s", previousEntry, entry),
                        previousEntry.getKey().compareTo(entry.getKey()) < 0);
                ++numEntries;
            }

            assertEquals(entries.size(), numEntries);
        }
    }

    private static class GenerateOrderedData
            implements Callable<Void>
    {
        private final List<Entry<Key, Value>> entries;

        public GenerateOrderedData(List<Entry<Key, Value>> entries)
        {
            this.entries = requireNonNull(entries, "entries is null");
        }

        @Override
        public Void call()
                throws Exception
        {
            // Append monotonically increasing rows to the given list
            Random random = new Random();
            IntegerLexicoder lexicoder = new IntegerLexicoder();
            for (int i = 0; i < FILL_SIZE; ++i) {
                entries.add(new SimpleEntry<>(new Key(
                        new Text(lexicoder.encode(i)),
                        new Text(uuid()),
                        new Text(uuid()),
                        new Text(uuid()),
                        abs(random.nextLong())),
                        new Value(uuid().getBytes(UTF_8))));
            }
            return null;
        }
    }

    private static String uuid()
    {
        return randomUUID().toString();
    }
}
