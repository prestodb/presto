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
package com.facebook.presto.util.maps;

import com.google.common.base.Equivalence;
import com.google.common.collect.Iterators;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.joining;

public class IdentityLinkedHashMap<K, V>
        implements Map<K, V>
{
    private final Map<Equivalence.Wrapper<K>, V> delegate = new LinkedHashMap<>();
    private final Equivalence<Object> equivalence = Equivalence.identity();

    public IdentityLinkedHashMap()
    {
    }

    public IdentityLinkedHashMap(IdentityLinkedHashMap<K, V> map)
    {
        putAll(map);
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

    @Override
    public boolean containsKey(Object key)
    {
        return delegate.containsKey(equivalence.wrap(key));
    }

    /**
     * @deprecated Unsupported operation.
     */
    @Deprecated
    @Override
    public boolean containsValue(Object value)
    {
        // should use identity-based comparison
        throw new UnsupportedOperationException();
    }

    @Override
    public V get(Object key)
    {
        return delegate.get(equivalence.wrap(key));
    }

    @Override
    public V put(K key, V value)
    {
        return delegate.put(equivalence.wrap(key), value);
    }

    @Override
    public V remove(Object key)
    {
        return delegate.remove(equivalence.wrap(key));
    }

    /**
     * @deprecated Unsupported operation.
     */
    @Deprecated
    @Override
    public boolean remove(Object key, Object value)
    {
        // should use identity-based comparison for value too
        throw new UnsupportedOperationException();
    }

    /**
     * @deprecated Unsupported operation.
     */
    @Deprecated
    @Override
    public boolean replace(K key, V oldValue, V newValue)
    {
        // should use identity-based comparison for value too
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map)
    {
        map.entrySet().forEach(e -> delegate.put(equivalence.wrap(e.getKey()), e.getValue()));
    }

    @Override
    public void clear()
    {
        delegate.clear();
    }

    @Override
    public IterateOnlySetView<K> keySet()
    {
        return new IterateOnlySetView<K>()
        {
            @Override
            public Iterator<K> iterator()
            {
                return delegate.keySet().stream().map(Equivalence.Wrapper::get).iterator();
            }
        };
    }

    @Override
    public IterateOnlyCollectionView<V> values()
    {
        return new IterateOnlyCollectionView<V>()
        {
            @Override
            public Iterator<V> iterator()
            {
                return Iterators.unmodifiableIterator(delegate.values().iterator());
            }
        };
    }

    @Override
    public IterateOnlySetView<Entry<K, V>> entrySet()
    {
        return new IterateOnlySetView<Entry<K, V>>()
        {
            @Override
            public Iterator<Entry<K, V>> iterator()
            {
                return delegate.entrySet().stream().map(e -> {
                    K key = e.getKey().get();
                    return (Entry<K, V>) new AbstractMap.SimpleEntry<>(key, e.getValue());
                }).iterator();
            }
        };
    }

    public abstract class IterateOnlySetView<E>
            extends IterateOnlyCollectionView<E>
            implements Set<E>
    {
        private IterateOnlySetView() {}
    }

    public abstract class IterateOnlyCollectionView<E>
            implements Collection<E>
    {
        private IterateOnlyCollectionView() {}

        @Override
        public int size()
        {
            return IdentityLinkedHashMap.this.size();
        }

        @Override
        public boolean isEmpty()
        {
            return size() == 0;
        }

        /**
         * @deprecated Unsupported operation.
         */
        @Deprecated
        @Override
        public boolean contains(Object item)
        {
            // should use identity-based comparison whenever map's keys or values are compared
            throw new UnsupportedOperationException();
        }

        @Override
        public Object[] toArray()
        {
            return Iterators.toArray(iterator(), Object.class);
        }

        /**
         * @deprecated Unsupported operation.
         */
        @Deprecated
        @Override
        public <T> T[] toArray(T[] array)
        {
            throw new UnsupportedOperationException();
        }

        /**
         * @deprecated Unsupported operation.
         */
        @Deprecated
        @Override
        public boolean add(E item)
        {
            throw new UnsupportedOperationException();
        }

        /**
         * @deprecated Unsupported operation.
         */
        @Deprecated
        @Override
        public boolean remove(Object item)
        {
            throw new UnsupportedOperationException();
        }

        /**
         * @deprecated Unsupported operation.
         */
        @Deprecated
        @Override
        public boolean containsAll(Collection<?> other)
        {
            throw new UnsupportedOperationException();
        }

        /**
         * @deprecated Unsupported operation.
         */
        @Deprecated
        @Override
        public boolean addAll(Collection<? extends E> other)
        {
            throw new UnsupportedOperationException();
        }

        /**
         * @deprecated Unsupported operation.
         */
        @Deprecated
        @Override
        public boolean retainAll(Collection<?> other)
        {
            throw new UnsupportedOperationException();
        }

        /**
         * @deprecated Unsupported operation.
         */
        @Deprecated
        @Override
        public boolean removeAll(Collection<?> other)
        {
            throw new UnsupportedOperationException();
        }

        /**
         * @deprecated Unsupported operation.
         */
        @Deprecated
        @Override
        public void clear()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return stream()
                    .map(String::valueOf)
                    .collect(joining(", ", "[", "]"));
        }

        /**
         * @deprecated Unsupported operation.
         */
        @Deprecated
        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        /**
         * @deprecated Unsupported operation.
         */
        @Deprecated
        @Override
        public boolean equals(Object obj)
        {
            throw new UnsupportedOperationException();
        }
    }
}
