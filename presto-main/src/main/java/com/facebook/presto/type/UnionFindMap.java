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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UnionFindMap<K, V>
{
    private final UnionFindSet<K> sets;
    private final Map<K, Set<V>> map;

    public UnionFindMap()
    {
        this.sets = new UnionFindSet<>();
        this.map = new HashMap<>();
    }

    public UnionFindMap(UnionFindMap<K, V> unionFindMap)
    {
        this.sets = new UnionFindSet<>(unionFindMap.sets);
        this.map = new HashMap<>(unionFindMap.map);
    }

    /**
     * @return <tt>true</tt> if the specified equivalence is new
     */
    public boolean union(K left, K right)
    {
        K leftRoot = sets.find(left);
        K rightRoot = sets.find(right);
        if (leftRoot.equals(rightRoot)) {
            return false;
        }
        sets.findAndUnion(leftRoot, rightRoot);
        Set<V> leftData = map.get(leftRoot);
        Set<V> rightData = map.get(rightRoot);
        if (leftData == null) {
            leftData = rightData;
        }
        else if (rightData != null) {
            leftData.addAll(rightData);
        }
        map.remove(rightRoot);
        map.put(sets.find(leftRoot), leftData);
        return true;
    }

    /**
     * @return <tt>true</tt> if the specified entry is new
     */
    public boolean put(K key, V value)
    {
        K keyRoot = sets.find(key);
        Set<V> set = map.computeIfAbsent(keyRoot, unused -> new HashSet<>());
        return set.add(value);
    }

    public Set<V> get(K key)
    {
        return map.computeIfAbsent(sets.find(key), unused -> new HashSet<V>());
    }

    public Map<K, Set<V>> values()
    {
        return map;
    }

    public Map<Set<K>, Set<V>> toMap()
    {
        ImmutableMap.Builder<Set<K>, Set<V>> result = ImmutableMap.builder();
        for (Set<K> ks : sets.getEquivalentClasses()) {
            Set<V> vs = get(Iterables.getFirst(ks, null));
            result.put(ks, vs);
        }
        return result.build();
    }
}
