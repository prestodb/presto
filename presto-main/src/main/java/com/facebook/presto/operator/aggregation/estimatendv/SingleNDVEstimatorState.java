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

    /**
     * Estimates the lower bound for the number of distinct values in a population
     * using a probability-based approach inspired by the Chao estimator implementation
     * in the Python package Pydistinct.
     *
     * The Chao estimator helps us approximate the number of distinct values in a population
     * when we draw N samples, and all of them are unique values. It calculates the probability
     * of this event happening for a specific number (R) of distinct values within the population,
     * allowing us to place a lower bound on the estimated number of distinct values.
     *
     * This method estimates the number of distinct values as the population size at which
     * there is a 10% chance that drawing N distinct values could happen due to random chance alone.
     *
     * @param d The number of distinct values observed in the sample.
     * @return An estimated lower bound for the number of distinct values in the population.
     * @see <a href="https://github.com/chanedwin/pydistinct">Pydistinct on GitHub</a>
     */
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
     * Use Chao estimator to estimate NDV.
     * Also compute error bound with 95% CI:
     * Error Bound (E) = Z * sqrt((f_1 * (n - 1) * (n - 2)) / (2 * (f_2 * n - 1)))
     * Z is the critical value from the standard normal distribution corresponding to the desired level of confidence (e.g., 1.96 for 95% confidence).
     * f_1 is the number of singletons (items observed only once) in the sample.
     * f_2 is the number of doubletons (items observed exactly twice) in the sample.
     * n is the size of the sample.
     * If R_1 or R_2 is 0, then only return estimated NDV, else return estimated NDV with the error bound
     *
     * @return Estimate of the NDV in the specific column in the original table with error bound
     */
    public Object[] estimate()
    {
        Map<Long, Long> freqDict = computeFreqDict((SingleTypedHistogram) this.get());
        // get frequency map and compute freq dict
        long distinctValues = freqDict.values().stream().mapToLong(i -> i).sum();
        long totalValues = 0;
        for (long key : freqDict.keySet()) {
            totalValues += key * freqDict.get(key);
        }
        if (distinctValues == totalValues) {
            return new Object[] {computeBirthdayProblemProbability(distinctValues)};
        }
        // if 1 not in freqDict
        if (!freqDict.containsKey(1L)) {
            return new Object[] {distinctValues};
        }
        long f1 = freqDict.get(1L);
        // if 2 not in freqDict
        if (!freqDict.containsKey(2L)) {
            return new Object[] {computeBirthdayProblemProbability(distinctValues)};
        }
        long f2 = freqDict.get(2L);
        Object[] ndvWithErrorBound = new Object[2];
        ndvWithErrorBound[0] = distinctValues + (f1 * f1) / (2L * f2);
        ndvWithErrorBound[1] = 1.96 * Math.sqrt((f1 * (totalValues - 1) * (totalValues - 2)) / (2.0 * (f2 * totalValues - 1)));
        return ndvWithErrorBound;
    }
}
