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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.io.Closeable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public class SortedEntryIterator
        implements Iterator<Entry<Key, Value>>, Closeable
{
    private final BlockingQueue<Entry<Key, Value>> orderedEntries;
    private final AtomicBoolean finishedScan = new AtomicBoolean(false);
    private final int bufferSize;
    private final Thread scanThread;

    public SortedEntryIterator(int bufferSize, Iterator<Entry<Key, Value>> parent)
    {
        requireNonNull(parent, "parent iterator is null");
        this.bufferSize = bufferSize;
        orderedEntries = new BoundedPriorityBlockingQueue<>(bufferSize, new KeyValueEntryComparator(), (int) (bufferSize * 1.25));

        // Begin reading from the parent iterator, adding entries to the queue
        scanThread = new Thread(() ->
        {
            while (parent.hasNext()) {
                try {
                    orderedEntries.put(parent.next());
                }
                catch (InterruptedException e) {
                    Thread.interrupted();
                    break;
                }
            }

            finishedScan.set(true);
        });

        scanThread.start();
    }

    @Override
    public boolean hasNext()
    {
        // Wait for the scan to finish or there are at least FILL_SIZE entries
        // This will guarantee ordering of the last FILL_SIZE entries that were put in the queue
        while (!finishedScan.get() && orderedEntries.size() < bufferSize) {
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

    @Override
    public void close()
    {
        scanThread.interrupt();
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
