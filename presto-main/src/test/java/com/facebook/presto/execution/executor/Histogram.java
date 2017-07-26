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
package com.facebook.presto.execution.executor;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;

class Histogram<K extends Comparable<K>>
{
    private final List<K> buckets;
    private final boolean discrete;

    private Histogram(Collection<K> buckets, boolean discrete)
    {
        this.buckets = new ArrayList<>(buckets);
        this.discrete = discrete;
        Collections.sort(this.buckets);
    }

    public static <K extends Comparable<K>> Histogram<K> fromDiscrete(Collection<K> buckets)
    {
        return new Histogram<>(buckets, true);
    }

    public static <K extends Comparable<K>> Histogram<K> fromContinuous(Collection<K> buckets)
    {
        return new Histogram<>(buckets, false);
    }

    public static <D> Histogram<Long> fromContinuous(Collection<D> initialData, Function<D, Long> keyFunction)
    {
        if (initialData.isEmpty()) {
            return new Histogram<>(ImmutableList.<Long>of(), false);
        }

        int numBuckets = Math.min(10, (int) Math.sqrt(initialData.size()));
        long min = initialData.stream()
                .mapToLong(keyFunction::apply)
                .min()
                .getAsLong();
        long max = initialData.stream()
                .mapToLong(keyFunction::apply)
                .max()
                .getAsLong();

        checkArgument(max > min);

        long bucketSize = (max - min) / numBuckets;
        long bucketRemainder = (max - min) % numBuckets;

        List<Long> minimums = new ArrayList<>();

        long currentMin = min;
        for (int i = 0; i < numBuckets; i++) {
            minimums.add(currentMin);
            long currentMax = currentMin + bucketSize;
            if (bucketRemainder > 0) {
                currentMax++;
                bucketRemainder--;
            }
            currentMin = currentMax + 1;
        }

        minimums.add(numBuckets, currentMin);

        return new Histogram<>(minimums, false);
    }

    public <D, F> void printDistribution(
            Collection<D> data,
            Function<D, K> keyFunction,
            Function<K, F> keyFormatter)
    {
        if (buckets.isEmpty()) {
            System.out.println("No buckets");
            return;
        }

        if (data.isEmpty()) {
            System.out.println("No data");
            return;
        }

        long[] bucketData = new long[buckets.size()];

        for (D datum : data) {
            K key = keyFunction.apply(datum);

            for (int i = 0; i < buckets.size(); i++) {
                if (key.compareTo(buckets.get(i)) >= 0 && (i == (buckets.size() - 1) || key.compareTo(buckets.get(i + 1)) < 0)) {
                    bucketData[i]++;
                    break;
                }
            }
        }

        if (!discrete) {
            for (int i = 0; i < bucketData.length - 1; i++) {
                System.out.printf("%8s - %8s : (%5s values)\n",
                        keyFormatter.apply(buckets.get(i)),
                        keyFormatter.apply(buckets.get(i + 1)),
                        bucketData[i]);
            }
        }
        else {
            for (int i = 0; i < bucketData.length; i++) {
                System.out.printf("%8s : (%5s values)\n",
                        keyFormatter.apply(buckets.get(i)),
                        bucketData[i]);
            }
        }
    }

    public <D, V, F, G> void printDistribution(
            Collection<D> data,
            Function<D, K> keyFunction,
            Function<D, V> valueFunction,
            Function<K, F> keyFormatter,
            Function<List<V>, G> valueFormatter)
    {
        if (buckets.isEmpty()) {
            System.out.println("No buckets");
            return;
        }

        if (data.isEmpty()) {
            System.out.println("No data");
            return;
        }

        SortedMap<Integer, List<V>> bucketData = new TreeMap<>();
        for (int i = 0; i < buckets.size(); i++) {
            bucketData.put(i, new ArrayList<>());
        }

        for (D datum : data) {
            K key = keyFunction.apply(datum);
            V value = valueFunction.apply(datum);

            for (int i = 0; i < buckets.size(); i++) {
                if (key.compareTo(buckets.get(i)) >= 0 && (i == (buckets.size() - 1) || key.compareTo(buckets.get(i + 1)) < 0)) {
                    bucketData.get(i).add(value);
                    break;
                }
            }
        }

        if (!discrete) {
            for (int i = 0; i < bucketData.size() - 1; i++) {
                System.out.printf("%8s - %8s : (%5s values) %s\n",
                        keyFormatter.apply(buckets.get(i)),
                        keyFormatter.apply(buckets.get(i + 1)),
                        bucketData.get(i).size(),
                        valueFormatter.apply(bucketData.get(i)));
            }
        }
        else {
            for (int i = 0; i < bucketData.size(); i++) {
                System.out.printf("%19s : (%5s values) %s\n",
                        keyFormatter.apply(buckets.get(i)),
                        bucketData.get(i).size(),
                        valueFormatter.apply(bucketData.get(i)));
            }
        }
    }
}
