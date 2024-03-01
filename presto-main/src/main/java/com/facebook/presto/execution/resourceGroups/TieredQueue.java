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

import java.util.Iterator;
import java.util.function.Supplier;

import static com.google.common.collect.Iterators.concat;
import static java.util.Objects.requireNonNull;

final class TieredQueue<E>
        implements UpdateablePriorityQueue<E>
{
    private final UpdateablePriorityQueue<E> highPriorityQueue;
    private final UpdateablePriorityQueue<E> lowPriorityQueue;

    public TieredQueue(UpdateablePriorityQueue<E> highPriorityQueue, UpdateablePriorityQueue<E> lowPriorityQueue)
    {
        this.highPriorityQueue = requireNonNull(highPriorityQueue, "highPriorityQueue is null");
        this.lowPriorityQueue = requireNonNull(lowPriorityQueue, "lowPriorityQueue is null");
    }

    public TieredQueue(Supplier<UpdateablePriorityQueue<E>> supplier)
    {
        this(supplier.get(), supplier.get());
    }

    @Override
    public boolean addOrUpdate(E element, long priority)
    {
        return lowPriorityQueue.addOrUpdate(element, priority);
    }

    public boolean prioritize(E element, long priority)
    {
        return highPriorityQueue.addOrUpdate(element, priority);
    }

    @Override
    public boolean contains(E element)
    {
        return highPriorityQueue.contains(element) || lowPriorityQueue.contains(element);
    }

    @Override
    public boolean remove(E element)
    {
        boolean highPriorityRemoved = highPriorityQueue.remove(element);
        boolean lowPriorityRemoved = lowPriorityQueue.remove(element);
        return highPriorityRemoved || lowPriorityRemoved;
    }

    @Override
    public E poll()
    {
        Iterator<E> iterator = iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        E element = iterator.next();
        iterator.remove();
        return element;
    }

    @Override
    public E peek()
    {
        Iterator<E> iterator = iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        return iterator.next();
    }

    @Override
    public int size()
    {
        return highPriorityQueue.size() + lowPriorityQueue.size();
    }

    @Override
    public boolean isEmpty()
    {
        return highPriorityQueue.isEmpty() && lowPriorityQueue.isEmpty();
    }

    @Override
    public Iterator<E> iterator()
    {
        return concat(highPriorityQueue.iterator(), lowPriorityQueue.iterator());
    }

    public UpdateablePriorityQueue<E> getLowPriorityQueue()
    {
        return lowPriorityQueue;
    }
}
