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
package com.facebook.presto.execution.scheduler;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.hash.Hashing.murmur3_128;
import static java.lang.Math.addExact;

/**
 * Reconstructible, bounded-load placement over rendezvous-hash preferences.
 * Placement depends only on the complete key/weight set and worker identities,
 * never on enumeration order, query identity, or live execution load.
 *
 * Prefer the primary owner first; move only its deterministic overflow through
 * the remaining ranked workers. This is not a persistent cache directory and
 * does not promise unchanged overflow owners when the input key set changes.
 */
public final class DeterministicBoundedAssignment
{
    private static final HashFunction HASH = murmur3_128();

    private DeterministicBoundedAssignment() {}

    public static Map<String, String> assign(Map<String, Long> weights, Collection<String> workerIds)
    {
        List<String> workers = workerIds.stream().sorted().collect(Collectors.toList());
        checkArgument(!workers.isEmpty(), "No workers for deterministic placement");
        checkArgument(workers.stream().distinct().count() == workers.size(), "Duplicate worker identity");
        if (weights.isEmpty()) {
            return ImmutableMap.of();
        }

        long capacity = capacity(weights.values(), workers.size());
        // Fixed priority determines both primary retention and overflow order.
        List<String> keys = new ArrayList<>(weights.keySet());
        keys.sort(Comparator.<String>comparingLong(key -> score(key, "")).thenComparing(Comparator.naturalOrder()));
        Map<String, List<String>> preferences = new HashMap<>();
        for (String key : keys) {
            List<String> ranked = new ArrayList<>(workers);
            ranked.sort((left, right) -> {
                int comparison = Long.compareUnsigned(score(key, right), score(key, left));
                return comparison != 0 ? comparison : left.compareTo(right);
            });
            preferences.put(key, ranked);
        }

        Map<String, Long> loads = new HashMap<>();
        workers.forEach(worker -> loads.put(worker, 0L));
        Map<String, String> owners = new HashMap<>();
        List<String> overflow = new ArrayList<>();
        for (String key : keys) {
            String primary = preferences.get(key).get(0);
            long weight = weights.get(key);
            if (weight <= capacity - loads.get(primary)) {
                owners.put(key, primary);
                loads.put(primary, loads.get(primary) + weight);
            }
            else {
                overflow.add(key);
            }
        }
        for (String key : overflow) {
            long weight = weights.get(key);
            for (String worker : preferences.get(key)) {
                if (weight <= capacity - loads.get(worker)) {
                    owners.put(key, worker);
                    loads.put(worker, loads.get(worker) + weight);
                    break;
                }
            }
            verify(owners.containsKey(key), "No capacity for key %s", key);
        }
        return ImmutableMap.copyOf(owners);
    }

    /**
     * Indivisible weighted keys need slack. Average rounded up plus the largest
     * key minus one guarantees room for every overflow key: otherwise every
     * worker would already have at least the rounded average assigned.
     * For equal unit weights this is exactly ceil(keys / workers).
     */
    public static long capacity(Collection<Long> weights, int workers)
    {
        checkArgument(workers > 0, "workers must be positive");
        long total = 0;
        long largest = 0;
        for (long weight : weights) {
            checkArgument(weight > 0, "weights must be positive");
            total = addExact(total, weight);
            largest = Math.max(largest, weight);
        }
        if (total == 0) {
            return 0;
        }
        long average = total / workers + (total % workers == 0 ? 0 : 1);
        return largest - 1 > Long.MAX_VALUE - average ? Long.MAX_VALUE : average + largest - 1;
    }

    private static long score(String key, String worker)
    {
        return HASH.newHasher()
                .putInt(key.length())
                .putUnencodedChars(key)
                .putUnencodedChars(worker)
                .hash()
                .asLong();
    }

    public static String fingerprint(Map<String, String> owners)
    {
        Hasher hasher = HASH.newHasher();
        owners.keySet().stream().sorted().forEach(key -> hasher
                .putInt(key.length()).putUnencodedChars(key)
                .putInt(owners.get(key).length()).putUnencodedChars(owners.get(key)));
        return hasher.hash().toString();
    }
}
