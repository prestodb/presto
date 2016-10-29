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
package com.facebook.presto.accumulo.io;

import com.facebook.presto.spi.PrestoException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.accumulo.AccumuloErrorCode.IO_ERROR;
import static java.util.Objects.requireNonNull;

public class SortedEntryIterator
        implements Iterator<Entry<Key, Value>>
{
    public static final int FILL_SIZE = 10_000;
    private final BlockingQueue<Entry<Key, Value>> orderedEntries = new BoundedPriorityBlockingQueue<>(FILL_SIZE, new KeyValueEntryComparator(), FILL_SIZE * 2);
    private final AtomicBoolean finishedScan = new AtomicBoolean(false);

    public SortedEntryIterator(Iterator<Entry<Key, Value>> parent)
    {
        requireNonNull(parent, "parent iterator is null");

        // Begin reading from the parent iterator, adding entries to the queue
        new Thread(() ->
        {
            while (parent.hasNext()) {
                try {
                    orderedEntries.put(parent.next());
                }
                catch (InterruptedException e) {
                    Thread.interrupted();
                    throw new PrestoException(IO_ERROR, "Received InterruptedException when adding an entry");
                }
            }

            finishedScan.set(true);
        }).start();
    }

    @Override
    public boolean hasNext()
    {
        // Wait for the scan to finish or there are at least FILL_SIZE entries
        // This will guarantee ordering of the last FILL_SIZE entries that were put in the queue
        while (!finishedScan.get() && orderedEntries.size() < FILL_SIZE) {
            try {
                Thread.sleep(1);
            }
            catch (InterruptedException e) {
                Thread.interrupted();
            }
        }

        return !orderedEntries.isEmpty() || !finishedScan.get();
    }

    @Override
    public Entry<Key, Value> next()
    {
        return orderedEntries.poll();
    }

    private static class KeyValueEntryComparator
            implements Comparator<Entry<Key, Value>>
    {
        @Override
        public int compare(Entry<Key, Value> o1, Entry<Key, Value> o2)
        {
            return o1.getKey().compareTo(o2.getKey());
        }
    }
}
