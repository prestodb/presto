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

package com.facebook.presto.operator.aggregation.mostfrequent;

import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/**
 * Copied from: https://github.com/addthis/stream-lib
 */
public class ConservativeAddSketch
        extends CountMinSketch
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ConservativeAddSketch.class).instanceSize();

    public ConservativeAddSketch(double epsOfTotalCount, double confidence, int seed)
    {
        super(epsOfTotalCount, confidence, seed);
    }

    @Override
    public long add(int[] buckets, long count)
    {
        if (count < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Negative increments not implemented");
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

    public ConservativeAddSketch(Slice serialized)
    {
        super(serialized);
    }
}
