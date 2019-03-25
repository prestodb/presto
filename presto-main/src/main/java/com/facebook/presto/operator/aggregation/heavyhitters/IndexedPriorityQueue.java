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

import java.util.*;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * copied from com.facebook.presto.execution.resourceGroups.IndexedPriorityQueue
 * A priority queue with constant time contains(E) and log time remove(E)
 * Ties are broken by insertion order
 */
public final class IndexedPriorityQueue<E>
{
    private boolean highestPriorityfirst=true;
    private final Map<E, Entry<E>> index = new HashMap<>();
    private final TreeSet<Entry<E>> queue = new TreeSet<>((entry1, entry2) -> {
        int priorityComparison = Long.compare(entry2.getPriority(), entry1.getPriority());
        if (priorityComparison != 0) {
            if(highestPriorityfirst)
                return priorityComparison;
            else
                return -1*priorityComparison;
        }
        return Long.compare(entry1.getGeneration(), entry2.getGeneration());
    });


    private long generation;

    public IndexedPriorityQueue(boolean highestPriorityfirst){
            this.highestPriorityfirst = highestPriorityfirst;
    }

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
        return entry;
    }

    public void removeBelowPriority(long tillPriority){
        Iterator<Entry<E>> iterator = queue.iterator();
        while (iterator.hasNext()) {
            Entry<E> entry = iterator.next();
            if (entry.getPriority() < tillPriority) {
                iterator.remove();
                checkState(index.remove(entry.getValue()) != null, "Failed to remove entry from index");
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

    public long getMinPriority(){
        if(queue.size() > 0){
            return queue.first().getPriority();
        }else{
            return Long.MAX_VALUE;
        }
    }

    public static final class Entry<E>
    {
        private final E value;
        private final long priority;
        private final long generation;

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
    }
}
