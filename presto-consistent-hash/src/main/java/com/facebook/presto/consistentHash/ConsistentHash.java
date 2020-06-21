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
package com.facebook.presto.consistentHash;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.google.common.hash.Hashing.md5;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ConsistentHash<T>
{
    private final SortedMap<Integer, T> circle = new TreeMap<>();

    public ConsistentHash(Collection<T> nodes)
    {
        for (T node : nodes) {
            add(node);
        }
    }

    /**
     * Get target nodes for a particular key
     * By first locating the node for the key and clockwise append the rest of the nodes in hash ring to the target list
     * Until we have reached the number of nodes or we have exhausted the hash ring
     * @param key
     * @param count specifies how many nodes to look for
     * @return
     */
    public Optional<List<T>> get(Object key, int count)
    {
        if (circle.isEmpty() || key == null) {
            return Optional.empty();
        }

        List<T> nodes = new ArrayList<>();
        nodes.add(get(key).get());
        while (count > 1 && nodes.size() < circle.size()) {
            nodes.add(get(key.hashCode() + 1).get());
            count--;
        }
        return Optional.of(ImmutableList.copyOf(nodes));
    }

    /**
     * Get the target node for a particular key
     * @param key
     * @return
     */
    public Optional<T> get(Object key)
    {
        if (circle.isEmpty() || key == null) {
            return Optional.empty();
        }
        int hash = md5().hashString(key.toString(), UTF_8).asInt();
        if (!circle.containsKey(hash)) {
            SortedMap<Integer, T> tailMap = circle.tailMap(hash);
            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return Optional.of(circle.get(hash));
    }

    private void add(T node)
    {
        circle.put(md5().hashString(node.toString(), UTF_8).asInt(), node);
    }
}
