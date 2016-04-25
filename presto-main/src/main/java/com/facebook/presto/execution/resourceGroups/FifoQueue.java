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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;

// A queue with constant time contains(E) and remove(E)
final class FifoQueue<E>
        implements Queue<E>
{
    private final Set<E> delegate = new LinkedHashSet<>();

    @Override
    public boolean add(E element)
    {
        return delegate.add(element);
    }

    @Override
    public boolean offer(E element)
    {
        return delegate.add(element);
    }

    @Override
    public E remove()
    {
        E element = poll();
        if (element == null) {
            throw new NoSuchElementException();
        }
        return element;
    }

    @Override
    public boolean contains(Object element)
    {
        return delegate.contains(element);
    }

    @Override
    public boolean remove(Object element)
    {
        return delegate.remove(element);
    }

    @Override
    public boolean containsAll(Collection<?> elements)
    {
        return delegate.containsAll(elements);
    }

    @Override
    public boolean addAll(Collection<? extends E> elements)
    {
        return delegate.addAll(elements);
    }

    @Override
    public boolean removeAll(Collection<?> elements)
    {
        return delegate.removeAll(elements);
    }

    @Override
    public boolean retainAll(Collection<?> elements)
    {
        return delegate.retainAll(elements);
    }

    @Override
    public void clear()
    {
        delegate.clear();
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
    public E element()
    {
        E element = peek();
        if (element == null) {
            throw new NoSuchElementException();
        }
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
    public Iterator<E> iterator()
    {
        return delegate.iterator();
    }

    @Override
    public Object[] toArray()
    {
        return delegate.toArray();
    }

    @Override
    public <T> T[] toArray(T[] array)
    {
        return delegate.toArray(array);
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
