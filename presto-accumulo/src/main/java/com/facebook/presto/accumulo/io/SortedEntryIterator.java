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
    private final IteratorTask iteratorTask = new IteratorTask();
    private final Thread iteratorThread = new Thread(iteratorTask);
    private final Iterator<Entry<Key, Value>> parent;

    public SortedEntryIterator(int bufferSize, Iterator<Entry<Key, Value>> parent)
    {
        this.parent = requireNonNull(parent, "parent iterator is null");
        this.bufferSize = bufferSize;
        this.orderedEntries = new BoundedPriorityBlockingQueue<>(bufferSize, new KeyValueEntryComparator(), (int) (bufferSize * 1.25));

        // Begin reading from the parent iterator, adding entries to the queue
        this.iteratorThread.start();
    }

    @Override
    public boolean hasNext()
    {
        // Wait for the scan to finish or there are at least FILL_SIZE entries
        // This will guarantee ordering of the last FILL_SIZE entries that were put in the queue
        while (!finishedScan.get() && orderedEntries.size() < bufferSize) {
            try {
                // Say there are no more entries if this thread has been interrupted
                if (Thread.currentThread().isInterrupted()) {
                    return false;
                }

                Thread.sleep(1);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        return !orderedEntries.isEmpty() || !finishedScan.get();
    }

    @Override
    public Entry<Key, Value> next()
    {
        // This won't block due to the behavior of hasNext
        return orderedEntries.poll();
    }

    @Override
    public void close()
    {
        // Alert the task to stop reading entries
        iteratorTask.stop();

        // Block until thread has finished reading
        // Avoids race condition between closing the scanner and
        // reading the next entry from the parent iterator
        // (which causes Accumulo to log an error message)
        while (iteratorThread.isAlive()) {
            try {
                Thread.sleep(1);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Drop all entries in the queue
        orderedEntries.clear();
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

    private class IteratorTask
            implements Runnable
    {
        private final AtomicBoolean stop = new AtomicBoolean(false);

        @Override
        public void run()
        {
            while (!stop.get() && parent.hasNext()) {
                try {
                    orderedEntries.put(parent.next());
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            finishedScan.set(true);
        }

        public void stop()
        {
            this.stop.set(true);
        }
    }
}
