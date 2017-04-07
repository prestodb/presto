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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Maps.immutableEntry;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Objects.requireNonNull;

public final class IdentityLinkedHashMap<K, V>
        implements Map<K, V>
{
    private static final Equivalence<Object> equivalence = Equivalence.identity();

    private final Map<Equivalence.Wrapper<K>, V> delegate = new LinkedHashMap<>();

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

    @Override
    public boolean containsValue(Object value)
    {
        return delegate.values().stream()
                .anyMatch(containedValue -> containedValue == value);
    }

    @Override
    public V get(Object key)
    {
        return delegate.get(equivalence.wrap(key));
    }

    @Override
    public V put(K key, V value)
    {
        requireNonNull(key, "key is null");
        return delegate.put(equivalence.wrap(key), value);
    }

    @Override
    public V remove(Object key)
    {
        return delegate.remove(equivalence.wrap(key));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map)
    {
        map.forEach(this::put);
    }

    @Override
    public void clear()
    {
        delegate.clear();
    }

    @Override
    public Set<K> keySet()
    {
        return new KeySet();
    }

    @Override
    public Collection<V> values()
    {
        return unmodifiableCollection(delegate.values());
    }

    @Override
    public Set<Entry<K, V>> entrySet()
    {
        return new EntrySet();
    }

    private class KeySet
            extends SetView<K>
    {
        @Override
        public boolean contains(Object item)
        {
            return IdentityLinkedHashMap.this.containsKey(item);
        }

        @Override
        public Iterator<K> iterator()
        {
            return Iterators.transform(delegate.keySet().iterator(), Equivalence.Wrapper::get);
        }

        @Override
        public boolean remove(Object item)
        {
            return delegate.keySet().remove(equivalence.wrap(item));
        }

        @Override
        public boolean retainAll(Collection<?> other)
        {
            return delegate.keySet().retainAll(
                    other.stream()
                            .map(equivalence::wrap)
                            .collect(toImmutableSet()));
        }
    }

    private class EntrySet
            extends SetView<Entry<K, V>>
    {
        @Override
        public boolean contains(Object item)
        {
            if (!(item instanceof Entry)) {
                return false;
            }
            Entry<?, ?> entry = (Entry<?, ?>) item;
            Equivalence.Wrapper<?> wrappedKey = equivalence.wrap(entry.getKey());
            return delegate.get(wrappedKey) == entry.getValue()
                    && (entry.getValue() != null || delegate.containsKey(wrappedKey));
        }

        @Override
        public Iterator<Entry<K, V>> iterator()
        {
            return Iterators.transform(delegate.entrySet().iterator(),
                    wrapperEntry -> immutableEntry(wrapperEntry.getKey().get(), wrapperEntry.getValue()));
        }

        @Override
        public boolean remove(Object item)
        {
            if (!contains(item)) {
                return false;
            }
            Entry<?, ?> entry = (Entry<?, ?>) item;
            delegate.remove(equivalence.wrap(entry.getKey()));
            return true;
        }

        @Override
        public boolean retainAll(Collection<?> other)
        {
            throw new UnsupportedOperationException();
        }
    }

    private abstract class SetView<E>
            implements Set<E>
    {
        @Override
        public final int size()
        {
            return IdentityLinkedHashMap.this.size();
        }

        @Override
        public final boolean isEmpty()
        {
            return IdentityLinkedHashMap.this.isEmpty();
        }

        @Override
        public final Object[] toArray()
        {
            return Iterators.toArray(iterator(), Object.class);
        }

        @Override
        public final <T> T[] toArray(T[] array)
        {
            return ImmutableList.copyOf(iterator()).toArray(array);
        }

        @Override
        public final boolean add(E item)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public final boolean addAll(Collection<? extends E> other)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public final boolean containsAll(Collection<?> other)
        {
            return other.stream()
                    .allMatch(this::contains);
        }

        @Override
        public final boolean removeAll(Collection<?> other)
        {
            boolean removed = false;
            for (Object item : other) {
                removed |= remove(item);
            }
            return removed;
        }

        @Override
        public final void clear()
        {
            IdentityLinkedHashMap.this.clear();
        }

        /**
         * Unsupported.
         * <p>
         * When comparing with other {@link Set}, we could compare snapshots as in {@code ImmutableSet.copyOf(this).equals(obj)},
         * but that would mean two sets can be equal even when they have different size.
         */
        @Override
        public final boolean equals(Object obj)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public final int hashCode()
        {
            throw new UnsupportedOperationException();
        }
    }
}
