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
import java.util.LinkedHashSet;
import java.util.Set;

// A queue with constant time contains(E) and remove(E)
final class FifoQueue<E>
        implements UpdateablePriorityQueue<E>
{
    private final Set<E> delegate = new LinkedHashSet<>();

    @Override
    public boolean addOrUpdate(E element, int priority)
    {
        return delegate.add(element);
    }

    @Override
    public boolean contains(E element)
    {
        return delegate.contains(element);
    }

    @Override
    public boolean remove(E element)
    {
        return delegate.remove(element);
    }

    @Override
    public E poll()
    {
        Iterator<E> iterator = delegate.iterator();
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
        Iterator<E> iterator = delegate.iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        return iterator.next();
    }

    @Override
    public int size()
    {
        return delegate.size();
    }

    @Override
    public boolean isEmpty()
    {
        return delegate.isEmpty();
    }
}
