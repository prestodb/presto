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

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.operator.aggregation.reservoirsample.ReservoirSampleState;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;
import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.LongStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;

/**
 * Implementation of Chao's estimator from Chao 1984, using counts of values that appear exactly once and twice
 * Before running NDV estimator, first run a select count(*) group by col query to get the frequency of each
 * distinct value in col.
 * Collect the counts of values with each frequency and store in the Map field freqDict
 * Use the counts of values with frequency 1 and 2 to do the computation:
 * d_chao = d + (f_1)^2/(2*(f_2))
 * Return birthday problem solution if there are no values observed of frequency 2
 * Also make insane bets (10x) when every point observed is almost unique, which could be good or bad
 */
@AccumulatorStateMetadata(
        stateSerializerClass = NDVEstimatorStateSerializer.class,
        stateFactoryClass = NDVEstimatorStateFactory.class)
public class NDVEstimatorState
        implements AccumulatorState
{
    private static final double LOWER_BOUND_PROBABILITY = 0.1;
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(ReservoirSampleState.class).instanceSize();
    /**
     * Counts of values with each frequency in the column
     */
    private Map<Long, Long> freqDict = new HashMap<>();

    public NDVEstimatorState() {}

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + 2L * SizeOf.SIZE_OF_LONG * freqDict.keySet().size();
    }

    public Map<Long, Long> getFreqDict()
    {
        return freqDict;
    }

    public void setFreqDict(Map<Long, Long> frequencyDict)
    {
        this.freqDict = frequencyDict;
    }

    /**
     * @param freq Frequency of a distinct value in original column . The frequency comes from a count(*) group by col query.
     */
    public void add(Long freq)
    {
        freqDict.put(freq, freqDict.getOrDefault(freq, 0L) + 1);
    }

    public void merge(NDVEstimatorState otherState)
    {
        Map<Long, Long> otherFreqDict = otherState.getFreqDict();
        for (Long value : otherFreqDict.keySet()) {
            freqDict.put(value, freqDict.getOrDefault(value, 0L) + otherFreqDict.get(value));
        }
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

    // serialize a long array
    public void serialize(BlockBuilder out)
    {
        for (long key : freqDict.keySet()) {
            BIGINT.writeLong(out, key);
            BIGINT.writeLong(out, freqDict.get(key));
        }
    }
}
