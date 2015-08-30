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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class DisjointSetsWithData<K, V>
{
    private final DisjointSets<K> sets;
    private final Map<K, Set<V>> map;

    public DisjointSetsWithData()
    {
        this.sets = new DisjointSets<>();
        this.map = new HashMap<>();
    }

    public DisjointSetsWithData(DisjointSets<K> sets, Map<K, Set<V>> map)
    {
        this.sets = sets;
        this.map = map;
    }

    /**
     * @return <tt>true</tt> if this structure did not already contain the specified equivalence
     */
    public boolean union(K left, K right)
    {
        K leftRoot = sets.find(left);
        K rightRoot = sets.find(right);
        if (leftRoot.equals(rightRoot)) {
            return false;
        }
        sets.findAndUnion(leftRoot, rightRoot);
        Set<V> leftVs = map.get(leftRoot);
        Set<V> rightVs = map.get(rightRoot);
        if (leftVs == null) {
            leftVs = rightVs;
        }
        else if (rightVs != null) {
            leftVs.addAll(rightVs);
            map.remove(leftVs);
        }
        map.remove(rightRoot);
        map.put(sets.find(leftRoot), leftVs);
        return true;
    }

    /**
     * @return <tt>true</tt> if this structure did not already contain the specified equivalence
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
            Set<V> vs = get(requireNonNull(Iterables.getFirst(ks, null)));
            result.put(ks, vs);
        }
        return result.build();
    }

    public DisjointSetsWithData<K, V> clone()
    {
        return new DisjointSetsWithData<>(sets.clone(), new HashMap<>(map));
    }
}
