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
package com.facebook.presto.spi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static java.util.Collections.unmodifiableList;

public class ConsistentHashingNodeProvider<T>
        implements NodeProvider<T>
{
    private final SortedMap<Integer, T> circle = new TreeMap<>();

    public ConsistentHashingNodeProvider(Collection<T> nodes)
    {
        if (nodes == null || nodes.isEmpty()) {
            throw new PrestoException(NO_NODES_AVAILABLE, "nodes is null or empty for ConsistentHashingNodeProvider");
        }
        for (T node : nodes) {
            add(node);
        }
    }

    /**
     * Get target nodes for a particular key
     * By first locating the node for the key and clockwise append the rest of the nodes in hash ring to the target list
     * Until we have reached the number of nodes or we have exhausted the hash ring
     *
     */
    public List<T> get(Object key, int count)
    {
        if (circle.isEmpty() || key == null) {
            return unmodifiableList(new ArrayList<>());
        }

        List<T> nodes = new ArrayList<>();
        int targetPosition = findPosition(key);
        Iterator<T> iterator = circle.tailMap(targetPosition).values().iterator();
        while (nodes.size() < count && nodes.size() < circle.size() && iterator.hasNext()) {
            nodes.add(iterator.next());
        }

        iterator = circle.values().iterator();
        while (nodes.size() < count && nodes.size() < circle.size()) {
            nodes.add(iterator.next());
        }

        return unmodifiableList(nodes);
    }

    private int findPosition(Object key)
    {
        int position = key.hashCode();
        if (!circle.containsKey(position)) {
            SortedMap<Integer, T> tailMap = circle.tailMap(position);
            position = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return position;
    }

    private void add(T node)
    {
        circle.put(node.hashCode(), node);
    }
}
