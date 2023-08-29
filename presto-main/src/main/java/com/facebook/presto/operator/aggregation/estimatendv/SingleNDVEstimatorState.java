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
package com.facebook.presto.operator.aggregation.estimatendv;

import com.facebook.presto.common.array.LongBigArray;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.histogram.SingleHistogramState;
import com.facebook.presto.operator.aggregation.histogram.SingleTypedHistogram;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.LongStream;

public class SingleNDVEstimatorState
        extends SingleHistogramState
        implements NDVEstimatorState
{
    public SingleNDVEstimatorState(Type keyType, int expectedEntriesCount)
    {
        super(keyType, expectedEntriesCount);
    }

    private static Map<Long, Long> computeFreqDict(SingleTypedHistogram histogram)
    {
        LongBigArray counts = histogram.getCounts();
        Map<Long, Long> freqDict = new HashMap<>();
        int ndv = histogram.getDistinctValueCount();
        for (int i = 0; i < ndv; i++) {
            Long count = counts.get(i);
            freqDict.put(count, freqDict.getOrDefault(count, 0L) + 1);
        }
        return freqDict;
    }

    private long computeBirthdayProblemProbability(long d)
    {
        long i = d;
        long lowerBound;
        while (true) {
            lowerBound = d + i;
            long finalLowerBound = lowerBound;
            double multiplied = LongStream.range(0, d).mapToDouble(j -> (double) (finalLowerBound - j) / finalLowerBound).reduce(1, (x, y) -> x * y);
            if (multiplied > LOWER_BOUND_PROBABILITY) {
                break;
            }
            i += 1;
        }
        return lowerBound;
    }

    /**
     * Use Chao estimator to estimate NDV
     *
     * @return Estimate of the NDV in the specific column in the original table
     */
    public long estimate()
    {
        Map<Long, Long> freqDict = computeFreqDict((SingleTypedHistogram) this.get());
        // get frequency map and compute freq dict
        long distinctValues = freqDict.values().stream().mapToLong(i -> i).sum();
        long totalValues = 0;
        for (long key : freqDict.keySet()) {
            totalValues += key * freqDict.get(key);
        }
        if (distinctValues == totalValues) {
            return computeBirthdayProblemProbability(distinctValues);
        }
        // if 1 not in freqDict
        if (!freqDict.containsKey(1L)) {
            return distinctValues;
        }
        long f1 = freqDict.get(1L);
        if (!freqDict.containsKey(2L)) {
            return computeBirthdayProblemProbability(distinctValues);
        }
        long f2 = freqDict.get(2L);
        return distinctValues + (f1 ^ 2) / (2L * f2);
    }
}
