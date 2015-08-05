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
package com.facebook.presto.metadata;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DisjointSets<T>
{
    private static class Entry<T>
    {
        T parent = null;
        int depth = 0; // depth of 1-node tree is 0
    }

    private HashMap<T, Entry> map = new HashMap<>();

    private boolean union(T root1, T root2)
    {
        Entry value1 = map.get(root1);
        Entry value2 = map.get(root2);
        if (value1 == value2) {
            return false;
        }
        if (value1.depth < value2.depth) {
            value1.parent = root2; // new parent: root2
        }
        else {
            if (value1.depth == value2.depth) {
                value1.depth++;
            }
            value2.parent = root1; // new parent: root1
        }
        return true;
    }

    public boolean findAndUnion(T node1, T node2)
    {
        return union(find(node1), find(node2));
    }

    public T find(T node)
    {
        if (!map.containsKey(node)) {
            map.put(node, new Entry());
            return node;
        }
        return findInternal(node);
    }

    public boolean inSameSet(T node1, T node2)
    {
        return find(node1).equals(find(node2));
    }

    public DisjointSets<T> clone()
    {
        DisjointSets<T> result = new DisjointSets<>();
        for (T item : map.keySet()) {
            result.findAndUnion(item, findInternal(item));
        }
        return result;
    }

    private T findInternal(T node)
    {
        Entry<T> value = map.get(node);
        if (value.parent == null) {
            return node;
        }
        else {
            value.parent = find(value.parent);
            return value.parent;
        }
    }

    public Collection<Set<T>> getEquivalentClasses()
    {
        Map<T, Set<T>> result = new HashMap<>();
        for (Map.Entry<T, Entry> entry : map.entrySet()) {
            T node = entry.getKey();
            T root = find(node);
            result.computeIfAbsent(root, unused -> new HashSet<>());
            result.get(root).add(node);
        }
        return result.values();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DisjointSets<T> that = (DisjointSets<T>) o;

        if (!map.keySet().equals(that.map.keySet())) {
            return false;
        }

        for (T t : map.keySet()) {
            if (!inSameSet(t, that.find(t))) {
                return false;
            }
        }

        for (T t : that.map.keySet()) {
            if (!that.inSameSet(t, find(t))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }
}
