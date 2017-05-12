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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.util.maps.IdentityLinkedHashMap;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Collections.newSetFromMap;

/**
 * Temporary class facilitating conversions from/to identity-based Maps and Sets
 */
public class NodeRefCollections
{
    public static <K extends Node, V> IdentityLinkedHashMap<K, V> toIdentityMap(Map<NodeRef<K>, V> map)
    {
        IdentityLinkedHashMap<K, V> result = new IdentityLinkedHashMap<>();
        for (Entry<NodeRef<K>, V> entry : map.entrySet()) {
            result.put(entry.getKey().getNode(), entry.getValue());
        }
        return result;
    }

    public static <K extends Node, V> Map<NodeRef<K>, V> fromIdentityMap(IdentityLinkedHashMap<K, V> map)
    {
        Map<NodeRef<K>, V> result = new LinkedHashMap<>();
        for (Entry<K, V> entry : map.entrySet()) {
            result.put(NodeRef.of(entry.getKey()), entry.getValue());
        }
        return result;
    }

    public static <T extends Node> Set<T> toIdentitySet(Set<NodeRef<T>> set)
    {
        Set<T> result = newSetFromMap(new IdentityLinkedHashMap<>());
        set.stream()
                .map(NodeRef::getNode)
                .forEach(result::add);
        return result;
    }

    public static <T extends Node> Set<NodeRef<T>> fromIdentitySet(Set<T> set)
    {
        return set.stream()
                .map(NodeRef::of)
                .collect(toImmutableSet());
    }

    private NodeRefCollections()
    {
    }
}
