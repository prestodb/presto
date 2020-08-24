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

package com.facebook.presto.operator.aggregation.mostfrequent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.util.Objects.requireNonNull;

/**
 * copied from com.facebook.presto.execution.resourceGroups.IndexedPriorityQueue
 * A priority queue with constant time contains(E) and log time remove(E)
 * Ties are broken by insertion order
 * Simple treeset does not work: https://github.com/prestodb/presto-facebook/pull/383#discussion_r274086065
 */
public final class IndexedPriorityQueue
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(IndexedPriorityQueue.class).instanceSize();
    private final Map<Slice, Entry> index = new HashMap<>();
    private final TreeSet<Entry> queue = new TreeSet<>((entry1, entry2) -> {
        int priorityComparison = Long.compare(entry1.getPriority(), entry2.getPriority());
        if (priorityComparison != 0) {
            return priorityComparison;
        }
        priorityComparison = Long.compare(entry2.getGeneration(), entry1.getGeneration());
        if (priorityComparison != 0) {
            return priorityComparison;
        }
        return entry1.getValue().compareTo(entry2.getValue());
    });
    private long generation;
    private int estimatedInMemorySize;

    public IndexedPriorityQueue()
    {
    }

    public boolean addOrUpdate(Slice element, long priority)
    {
        Entry entry = index.get(element);
        if (entry != null) {
            queue.remove(entry);
            Entry newEntry = new Entry(element, priority, entry.getGeneration());
            queue.add(newEntry);
            index.put(element, newEntry);
            return false;
        }
        Entry newEntry = new Entry(element, priority, generation);
        generation++;
        queue.add(newEntry);
        index.put(element, newEntry);
        updateMemoryForElement(newEntry, 1);
        return true;
    }

    public boolean addOrIncrement(Slice element, long increment)
    {
        Entry entry = index.get(element);
        if (entry != null) {
            return addOrUpdate(element, entry.getPriority() + increment);
        }
        else {
            return addOrUpdate(element, increment);
        }
    }

    public boolean remove(Slice element)
    {
        Entry entry = index.remove(element);
        if (entry != null) {
            queue.remove(entry);
            updateMemoryForElement(entry, -1);
            return true;
        }
        return false;
    }

    @VisibleForTesting
    public Entry poll()
    {
        List<Entry> itemArrayList = poll(1);
        if (itemArrayList.isEmpty()) {
            return null;
        }
        return itemArrayList.get(0);
    }

    public List<Entry> poll(int count)
    {
        if (count <= 0) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<Entry> itemArrayList = ImmutableList.builder();
        Iterator<Entry> iterator = queue.iterator();
        while (iterator.hasNext()) {
            Entry entry = iterator.next();
            iterator.remove();
            itemArrayList.add(entry);
            checkState(index.remove(entry.getValue()) != null, "Failed to remove entry from index");
            updateMemoryForElement(entry, -1);
            count--;
            if (count <= 0) {
                break;
            }
        }
        return itemArrayList.build();
    }

    public void removeBelowPriority(double tillPriority)
    {
        Iterator<Entry> iterator = queue.iterator();
        while (iterator.hasNext()) {
            Entry entry = iterator.next();
            if (entry.getPriority() < tillPriority) {
                iterator.remove();
                checkState(index.remove(entry.getValue()) != null, "Failed to remove entry from index");
                updateMemoryForElement(entry, -1);
            }
            else {
                break;
            }
        }
    }

    public Entry peek()
    {
        Iterator<Entry> iterator = queue.iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        Entry entry = iterator.next();
        return entry;
    }

    public int size()
    {
        return queue.size();
    }

    public boolean isEmpty()
    {
        return queue.isEmpty();
    }

    public Iterator<Entry> iterator()
    {
        return queue.iterator();
    }

    public TreeSet<Entry> getQueue()
    {
        return queue;
    }

    public long getMinPriority()
    {
        Entry entry = peek();
        if (entry == null) {
            return -1L;
        }
        return entry.getPriority();
    }

    /**
     * When only keys are required.
     * By using this iterator java.util.ConcurrentModificationException can be avoided for just updating priority
     *
     * @return
     */
    public Iterator<Slice> keysIterator()
    {
        return index.keySet().iterator();
    }

    /**
     * @param value
     * @param itemCount if add than +1, if removed than -1
     * @return final memory after the change.
     */
    public long updateMemoryForElement(Entry value, int itemCount)
    {
        estimatedInMemorySize += itemCount * value.estimatedInMemorySize();  //itemCount can be negative
        if (estimatedInMemorySize < 0) {
            estimatedInMemorySize = 0;
        }
        return this.estimatedInMemorySize();
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE + estimatedInMemorySize + SIZE_OF_INT * (index.size() + queue.size());
    }

    public Slice serialize()
    {
        int requiredBytes = 2 * SIZE_OF_LONG + estimatedInMemorySize;   //generation, estimatedInMemorySize
        SliceOutput s = new DynamicSliceOutput(requiredBytes);

        s.writeLong(generation);
        s.writeInt(estimatedInMemorySize);
        s.writeInt(queue.size());
        for (Iterator<Entry> it = queue.iterator(); it.hasNext(); ) {
            Entry e = it.next();
            s.writeLong(e.getPriority());
            s.writeLong(e.getGeneration());
            s.writeInt(e.getValue().length());
            s.writeBytes(e.getValue());
        }
        return s.slice();
    }

    //Constructor based upon deserialization
    public IndexedPriorityQueue(Slice serialized)
    {
        SliceInput s = new BasicSliceInput(serialized);
        generation = s.readLong();
        estimatedInMemorySize = s.readInt();
        int qSize = s.readInt();
        for (int i = 0; i < qSize; i++) {
            long priority = s.readLong();
            long generation = s.readLong();
            int elemSize = s.readInt();
            Slice item = s.readSlice(elemSize);
            Entry e = new Entry(item, priority, generation);
            index.put(e.getValue(), e);
            queue.add(e);
        }
    }

    public String toString()
    {
        return queue.toString();
    }

    public static final class Entry
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(Entry.class).instanceSize();
        private final Slice value;
        private final long priority;
        private final long generation;

        private Entry(Slice value, long priority, long generation)
        {
            this.value = requireNonNull(value, "value is null");
            this.priority = priority;
            this.generation = generation;
        }

        public Slice getValue()
        {
            return value;
        }

        public long getPriority()
        {
            return priority;
        }

        public long getGeneration()
        {
            return generation;
        }

        public long estimatedInMemorySize()
        {
            return INSTANCE_SIZE + value.length();
        }
    }
}
