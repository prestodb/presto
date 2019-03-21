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

/**
 * Copied from: https://github.com/addthis/stream-lib
 * A more accurate (by some large, but ill-defined amount), but slower (by some
 * small, but equally ill-defined amount) count min sketch. It seemed like a
 * simple optimization and later internet searching suggested it might be
 * called something like a conservative adding variant.
 */
public class ConservativeAddSketch extends CountMinSketch {

//    ConservativeAddSketch() {
//        super();
//    }
//
//    public ConservativeAddSketch(int depth, int width, int seed) {
//        super(depth, width, seed);
//    }

    public ConservativeAddSketch(double epsOfTotalCount, double confidence, int seed) {
        super(epsOfTotalCount, confidence, seed);
    }

//    ConservativeAddSketch(int depth, int width, long size, long[] hashA, long[][] table) {
//        super(depth, width, size, hashA, table);
//    }

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
        long newVal=min;
        for (int i = 0; i < depth; ++i) {
            newVal = Math.max(table[i][buckets[i]], min + count);
            table[i][buckets[i]] = newVal;
        }
        size += count;
        return newVal;
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
        long newVal=min;
        for (int i = 0; i < depth; ++i) {
            newVal = Math.max(table[i][buckets[i]], min + count);
            table[i][buckets[i]] = newVal;
        }
        size += count;
        return newVal;
    }
}
