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
package com.facebook.presto.operator.aggregation.heavyhitters;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.*;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;

import java.util.*;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.util.Objects.requireNonNull;

/**
 * copied from com.facebook.presto.execution.resourceGroups.IndexedPriorityQueue
 * A priority queue with constant time contains(E) and log time remove(E)
 * Ties are broken by insertion order
 */
public final class IndexedPriorityQueue<E>
{
    private long generation;
    private int estimatedInMemorySize;
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(IndexedPriorityQueue.class).instanceSize();

    private final Map<E, Entry<E>> index = new HashMap<>();
    private final TreeSet<Entry<E>> queue = new TreeSet<>((entry1, entry2) -> {
        int priorityComparison = Long.compare(entry1.getPriority(), entry2.getPriority());
        if (priorityComparison != 0) {
                return priorityComparison;
        }
        return entry1.getValue().toString().compareTo(entry2.getValue().toString());
    });


    public IndexedPriorityQueue(){
    }

    public boolean addOrUpdate(E element, long priority)
    {
        Entry<E> entry = index.get(element);
        if (entry != null) {
            queue.remove(entry);
            Entry<E> newEntry = new Entry<>(element, priority, entry.getGeneration());
            queue.add(newEntry);
            index.put(element, newEntry);
            return false;
        }
        Entry<E> newEntry = new Entry<>(element, priority, generation);
        generation++;
        queue.add(newEntry);
        index.put(element, newEntry);
        updateMemoryForElement(newEntry, 1);  //Update memory usage
        return true;
    }

    public boolean contains(E element)
    {
        return index.containsKey(element);
    }

    public boolean remove(E element)
    {
        Entry<E> entry = index.remove(element);
        if (entry != null) {
            queue.remove(entry);
            updateMemoryForElement(entry, -1);  //Update memory usage
            return true;
        }
        return false;
    }

    public Entry<E> poll()
    {
        Iterator<Entry<E>> iterator = queue.iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        Entry<E> entry = iterator.next();
        iterator.remove();
        checkState(index.remove(entry.getValue()) != null, "Failed to remove entry from index");
        updateMemoryForElement(entry, -1);  //Update memory usage
        return entry;
    }

    public void removeBelowPriority(double tillPriority){
        Iterator<Entry<E>> iterator = queue.iterator();
        while (iterator.hasNext()) {
            Entry<E> entry = iterator.next();
            if (entry.getPriority() < tillPriority) {
                iterator.remove();
                checkState(index.remove(entry.getValue()) != null, "Failed to remove entry from index");
                updateMemoryForElement(entry, -1);  //Update memory usage
            } else {
                break;
            }
        }
    }

    public Entry<E> peek()
    {
        Iterator<Entry<E>> iterator = queue.iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        Entry<E> entry = iterator.next();
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

    public Iterator<Entry<E>> iterator()
    {
        return (Iterator<Entry<E>>) queue.iterator();
    }

    /**
     * When only keys are required.
     * By using this iterator java.util.ConcurrentModificationException can be avoided for just updating priority
     * @return
     */
    public Iterator<E> keysIterator()
    {
        return (Iterator<E>) index.keySet().iterator();
    }


    public long getMinPriority(){
        if(queue.size() > 0){
            return queue.first().getPriority();
        }else{
            return Long.MAX_VALUE;
        }
    }

    /**
     *
     * @param value
     * @param itemCount  if add than +1, if removed than -1
     * @return final memory after the change.
     */
    public long updateMemoryForElement(Entry<E> value, int itemCount){
        estimatedInMemorySize += itemCount*value.estimatedInMemorySize();  //itemCount can be negative
        if (estimatedInMemorySize < 0)
            estimatedInMemorySize = 0;
        return this.estimatedInMemorySize();
    }

    public long estimatedInMemorySize() {
        // TODO add size of index and queue
        return INSTANCE_SIZE + estimatedInMemorySize;
    }


    public Slice serialize() {
        int requiredBytes = 2*SIZE_OF_LONG + estimatedInMemorySize;   //generation, estimatedInMemorySize
        SliceOutput s = new DynamicSliceOutput(requiredBytes);

        s.writeLong(generation);
        s.writeInt(estimatedInMemorySize);
        s.writeInt(queue.size());
        for (Iterator<Entry<E>> it = queue.iterator(); it.hasNext(); ) {
            Entry<E> e = it.next();
            Slice slc = e.serialize();
            s.writeInt(slc.length());
            s.writeBytes(slc);
        }
        return s.slice();
    }


    //Constructor based upon deserialization
    public IndexedPriorityQueue(Slice serialized) {
        SliceInput s = new BasicSliceInput(serialized);
        generation = s.readLong();
        estimatedInMemorySize = s.readInt();
        int qSize = s.readInt();
        for(int i=0; i<qSize; i++){
            int elemSize = s.readInt();
            Entry<E> e = new Entry<>(s.readSlice(elemSize));
            index.put(e.getValue(), e);
            queue.add(e);
        }
    }

    public String toString(){
        return queue.toString();
    }

    public static final class Entry<E>
    {
        private final E value;
        private final long priority;
        private final long generation;
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(Entry.class).instanceSize();
        private static int TYPE_SLICE = 0;
        private static int TYPE_STRING = 1;

        private Entry(E value, long priority, long generation)
        {
            this.value = requireNonNull(value, "value is null");
            this.priority = priority;
            this.generation = generation;
        }

        public E getValue()
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

        //TODO is it the right way to calculate size. If String then correct for java primitive this won't work.
        public long estimatedInMemorySize(){
            return INSTANCE_SIZE + value.toString().getBytes().length;
        }

        public String toString(){
            return value.toString() + ":" + String.valueOf(priority);
        }

        public Slice serialize(){
            SliceOutput s = new DynamicSliceOutput((int)estimatedInMemorySize());
            s.writeLong(priority);
            s.writeLong(generation);

            Slice slc;
            if(value instanceof Slice){
                slc = (Slice)value;
                s.writeInt(TYPE_SLICE);
            }else {
                slc = Slices.utf8Slice(value.toString());
                s.writeInt(TYPE_STRING);
            }
            s.writeInt(slc.length());
            s.writeBytes(slc);

            return s.slice();
        }

        @VisibleForTesting
        public Entry(Slice serialized){
            SliceInput s = new BasicSliceInput(serialized);
            priority = s.readLong();
            generation = s.readLong();
            int type = s.readInt();
            int length = s.readInt();
            byte[] b = new byte[length];
            s.readBytes(b,0, length);
            if(type == TYPE_SLICE){
                value = (E) Slices.wrappedBuffer(b);
            }else {
                value = (E) new String(b);
            }
        }
    }
}
