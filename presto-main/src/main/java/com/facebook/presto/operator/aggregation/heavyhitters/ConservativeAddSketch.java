/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.operator.aggregation.heavyhitters;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

/**
 * Copied from: https://github.com/addthis/stream-lib
 * Initially this class was used but due to it's non-deterministic results it was dropped.
 * Count estimation changes based upon the sequence in which the elements are added.
 * Class is still kept as a reminder that this algorithm does not work.
 *
 * A more accurate (by some large, but ill-defined amount), but slower (by some
 * small, but equally ill-defined amount) count min sketch. It seemed like a
 * simple optimization and later internet searching suggested it might be
 * called something like a conservative adding variant.
 */
public class ConservativeAddSketch extends CountMinSketch {

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ConservativeAddSketch.class).instanceSize();

    public ConservativeAddSketch(double epsOfTotalCount, double confidence, int seed) {
        super(epsOfTotalCount, confidence, seed);
    }

    //TODO any way to collapse all the add methods? bring out the common code into a different function
    @Override
    public long add(long item, long count) {
        if (count < 0) {
            // Negative values are not implemented in the regular version, and do not
            // play nicely with this algorithm anyway
            throw new IllegalArgumentException("Negative increments not implemented");
        }
        int[] buckets = new int[depth];
        for (int i = 0; i < depth; ++i) {
            buckets[i] = hash(item, i);
        }
        long min = table[0][buckets[0]];
        for (int i = 1; i < depth; ++i) {
            min = Math.min(min, table[i][buckets[i]]);
        }
        for (int i = 0; i < depth; ++i) {
            long newVal = Math.max(table[i][buckets[i]], min + count);
            table[i][buckets[i]] = newVal;
        }
        size += count;
        return min + count;
    }

    @Override
    public long add(String item, long count) {
        if (count < 0) {
            // Negative values are not implemented in the regular version, and do not
            // play nicely with this algorithm anyway
            throw new IllegalArgumentException("Negative increments not implemented");
        }
        int[] buckets = Filter.getHashBuckets(item, depth, width);
        long min = table[0][buckets[0]];
        for (int i = 1; i < depth; ++i) {
            min = Math.min(min, table[i][buckets[i]]);
        }
        for (int i = 0; i < depth; ++i) {
            long newVal = Math.max(table[i][buckets[i]], min + count);
            table[i][buckets[i]] = newVal;
        }
        size += count;
        return min + count;
    }

    @Override
    public long add(Slice item, long count) {
        if (count < 0) {
            // Negative values are not implemented in the regular version, and do not
            // play nicely with this algorithm anyway
            throw new IllegalArgumentException("Negative increments not implemented");
        }
        int[] buckets = Filter.getHashBuckets(item, depth, width);
        long min = table[0][buckets[0]];
        for (int i = 1; i < depth; ++i) {
            min = Math.min(min, table[i][buckets[i]]);
        }
        for (int i = 0; i < depth; ++i) {
            long newVal = Math.max(table[i][buckets[i]], min + count);
            table[i][buckets[i]] = newVal;
        }
        size += count;
        return min + count;
    }

    @VisibleForTesting
    public ConservativeAddSketch(Slice serialized) {
        super(serialized);
    }
}