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
package com.facebook.presto.spi.hashing;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.google.common.hash.Hashing.md5;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ConsistentHashSelector<T>
{
    private final SortedMap<Long, T> circle = new TreeMap();
    private static final HashFunction hashFunction = key -> md5().hashString(key, UTF_8).asLong();
    private final ConsistentHashNodeKeySupplier<T, String> keySupplier;
    private final int replicas;

    public ConsistentHashSelector(List<T> nodes, int replicas, ConsistentHashNodeKeySupplier<T, String> keySupplier)
    {
        this.keySupplier = keySupplier;
        this.replicas = replicas;
        nodes.stream().forEach(node -> add(node));
    }

    public void add(T node)
    {
        for (int i = 0; i < replicas; i++) {
            this.circle.put(hashFunction.hash(keySupplier.getKey(node) + "_" + i), node);
        }
    }

    public void remove(T node)
    {
        for (int i = 0; i < replicas; i++) {
            this.circle.remove(hashFunction.hash(keySupplier.getKey(node) + "_" + i));
        }
    }

    public List<T> getN(String data, int n)
    {
        List<T> results = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            results.add(get(data + "_" + i));
        }
        return results;
    }

    public T get(String data)
    {
        if (this.circle.isEmpty()) {
            return null;
        }
        else {
            long hash = hashFunction.hash(data);
            if (!circle.containsKey(hash)) {
                SortedMap<Long, T> tailMap = this.circle.tailMap(hash);
                hash = tailMap.isEmpty() ? this.circle.firstKey() : tailMap.firstKey();
            }
            return circle.get(hash);
        }
    }

    private interface HashFunction
    {
        long hash(String key);
    }
}
