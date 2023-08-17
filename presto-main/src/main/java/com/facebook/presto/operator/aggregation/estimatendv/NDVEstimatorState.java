package com.facebook.presto.operator.aggregation.estimatendv;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;

@AccumulatorStateMetadata(
        stateSerializerClass = NDVEstimatorStateSerializer.class,
        stateFactoryClass = NDVEstimatorStateFactory.class)
public class NDVEstimatorState
        implements AccumulatorState
{
    private Map<Long, Long> freqDict = new HashMap<>();

    public NDVEstimatorState() {}

    @Override
    public long getEstimatedSize()
    {
        return 0;
    }

    public Map<Long, Long> getFreqDict()
    {
        return freqDict;
    }

    public void setFreqDict(Map<Long, Long> frequencyDict)
    {
        this.freqDict = frequencyDict;
    }

    public void add(Long freq)
    {
        if (freqDict.containsKey(freq)) {
            freqDict.put(freq, freqDict.get(freq) + 1);
        }
        else {
            freqDict.put(freq, 1L);
        }
    }

    public void merge(NDVEstimatorState otherState)
    {
        Map<Long, Long> otherFreqDict = otherState.getFreqDict();
        for (Long value : otherFreqDict.keySet()) {
            if (freqDict.containsKey(value)) {
                freqDict.put(value, freqDict.get(value) + otherFreqDict.get(value));
            }
            else {
                freqDict.put(value, otherFreqDict.get(value));
            }
        }
    }

    private long computeBirthdayProblemProbability(long d)
    {
        double lowerBoundProbability = 0.1;
        long i = d;
        long lowerBound;
        while (true) {
            lowerBound = d + i;
            double multiplied = 1;
            for (int j : IntStream.range(0, (int) d).toArray()) {
                multiplied *= (double) (lowerBound - j) / lowerBound;
            }
            if (multiplied > lowerBoundProbability) {
                break;
            }
            i += 1;
        }
        return lowerBound;
    }

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
