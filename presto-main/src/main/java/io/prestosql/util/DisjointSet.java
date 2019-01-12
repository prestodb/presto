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
package io.prestosql.util;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;

public class DisjointSet<T>
{
    private static class Entry<T>
    {
        private T parent;
        private int rank; // Without path compression, this would be equal to depth. Depth of 1-node tree is considered 0.

        public Entry()
        {
            this(null, 0);
        }

        public Entry(Entry<T> entry)
        {
            this(entry.parent, entry.rank);
        }

        private Entry(T parent, int rank)
        {
            this.parent = parent;
            this.rank = rank;
        }

        public T getParent()
        {
            return parent;
        }

        public void setParent(T parent)
        {
            this.parent = parent;
            this.rank = -1;
        }

        public int getRank()
        {
            checkState(parent == null);
            return rank;
        }

        public void incrementRank()
        {
            checkState(parent == null);
            rank++;
        }
    }

    private final Map<T, Entry<T>> map;

    public DisjointSet()
    {
        map = new LinkedHashMap<>();
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
        if (root1.equals(root2)) {
            return false;
        }
        Entry<T> entry1 = map.get(root1);
        Entry<T> entry2 = map.get(root2);
        int entry1Rank = entry1.getRank();
        int entry2Rank = entry2.getRank();
        verify(entry1Rank >= 0);
        verify(entry2Rank >= 0);
        if (entry1Rank < entry2Rank) {
            // make root1 child of root2
            entry1.setParent(root2);
        }
        else {
            if (entry1Rank == entry2Rank) {
                // increment rank of root1 when both side were equally deep
                entry1.incrementRank();
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
            T root = findInternal(value.getParent());
            value.setParent(root);
            return root;
        }
    }

    public Collection<Set<T>> getEquivalentClasses()
    {
        // map from root element to all element in the tree
        Map<T, Set<T>> rootToTreeElements = new LinkedHashMap<>();
        for (Map.Entry<T, Entry<T>> entry : map.entrySet()) {
            T node = entry.getKey();
            T root = findInternal(node);
            rootToTreeElements.computeIfAbsent(root, unused -> new LinkedHashSet<>());
            rootToTreeElements.get(root).add(node);
        }
        return rootToTreeElements.values();
    }
}
