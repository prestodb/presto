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
package com.facebook.presto.execution.resourceGroups;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * A priority queue with constant time contains(E) and log time remove(E)
 * Ties are broken by insertion order
 */
final class IndexedPriorityQueue<E>
        implements UpdateablePriorityQueue<E>
{
    private final Map<E, Entry<E>> index = new HashMap<>();
    private final Set<Entry<E>> queue = new TreeSet<>((entry1, entry2) -> {
        int priorityComparison = Integer.compare(entry2.getPriority(), entry1.getPriority());
        if (priorityComparison != 0) {
            return priorityComparison;
        }
        return Long.compare(entry1.getGeneration(), entry2.getGeneration());
    });

    private long generation;

    @Override
    public boolean addOrUpdate(E element, int priority)
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

    @Override
    public boolean contains(E element)
    {
        return index.containsKey(element);
    }

    @Override
    public boolean remove(E element)
    {
        Entry<E> entry = index.remove(element);
        if (entry != null) {
            queue.remove(entry);
            return true;
        }
        return false;
    }

    @Override
    public E poll()
    {
        Iterator<Entry<E>> iterator = queue.iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        Entry<E> entry = iterator.next();
        iterator.remove();
        checkState(index.remove(entry.getValue()) != null, "Failed to remove entry from index");
        return entry.getValue();
    }

    @Override
    public E peek()
    {
        Iterator<Entry<E>> iterator = queue.iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        Entry<E> entry = iterator.next();
        return entry.getValue();
    }

    @Override
    public int size()
    {
        return queue.size();
    }

    @Override
    public boolean isEmpty()
    {
        return queue.isEmpty();
    }

    private static final class Entry<E>
    {
        private final E value;
        private final int priority;
        private final long generation;

        private Entry(E value, int priority, long generation)
        {
            this.value = requireNonNull(value, "value is null");
            this.priority = priority;
            this.generation = generation;
        }

        public E getValue()
        {
            return value;
        }

        public int getPriority()
        {
            return priority;
        }

        public long getGeneration()
        {
            return generation;
        }
    }
}
