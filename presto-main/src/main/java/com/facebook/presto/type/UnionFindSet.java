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
package com.facebook.presto.type;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

public class UnionFindSet<T>
{
    private static class Entry<T>
    {
        private T parent;
        private int depth; // depth of 1-node tree is 0

        public Entry()
        {
            this(null, 0);
        }

        public Entry(Entry<T> entry)
        {
            this(entry.parent, entry.depth);
        }

        private Entry(T parent, int depth)
        {
            this.parent = parent;
            this.depth = depth;
        }

        public T getParent()
        {
            return parent;
        }

        public void setParent(T parent)
        {
            this.parent = parent;
            this.depth = -1;
        }

        public int getDepth()
        {
            checkState(parent == null);
            return depth;
        }

        public void incrementDepth()
        {
            checkState(parent == null);
            depth++;
        }
    }

    private final HashMap<T, Entry<T>> map;

    public UnionFindSet()
    {
        map = new HashMap<>();
    }

    public UnionFindSet(UnionFindSet<T> unionFindSet)
    {
        map = new HashMap<>(unionFindSet.map);
        map.replaceAll((k, v) -> new Entry<>(v));
    }

    /**
     * @return <tt>true</tt> if the specified equivalence is new
     */
    public boolean findAndUnion(T node1, T node2)
    {
        return union(find(node1), find(node2));
    }

    public T find(T element)
    {
        if (!map.containsKey(element)) {
            map.put(element, new Entry<>());
            return element;
        }
        return findInternal(element);
    }

    private boolean union(T root1, T root2)
    {
        if (root1 == root2) {
            return false;
        }
        Entry<T> entry1 = map.get(root1);
        Entry<T> entry2 = map.get(root2);
        if (entry1.getDepth() < entry2.getDepth()) {
            // make root1 child of root2
            entry1.setParent(root2);
        }
        else {
            if (entry1.getDepth() == entry2.getDepth()) {
                // increment depth of root1 when both side were equally deep
                entry1.incrementDepth();
            }
            // make root2 child of root1
            entry2.setParent(root1);
        }
        return true;
    }

    private T findInternal(T element)
    {
        Entry<T> value = map.get(element);
        if (value.getParent() == null) {
            return element;
        }
        else {
            value.setParent(findInternal(value.getParent()));
            return value.getParent();
        }
    }

    public Collection<Set<T>> getEquivalentClasses()
    {
        // map from root element to all element in the tree
        Map<T, Set<T>> rootToTreeElements = new HashMap<>();
        for (Map.Entry<T, Entry<T>> entry : map.entrySet()) {
            T node = entry.getKey();
            T root = findInternal(node);
            rootToTreeElements.computeIfAbsent(root, unused -> new HashSet<>());
            rootToTreeElements.get(root).add(node);
        }
        return rootToTreeElements.values();
    }
}
