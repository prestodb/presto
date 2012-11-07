package com.facebook.presto.benchmark;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkNotNull;

public class AverageBenchmarkResults
        implements BenchmarkResultHook
{
    private final Map<String, Long> resultsSum = new LinkedHashMap<>();
    private int resultsCount;

    @Override
    public BenchmarkResultHook addResults(Map<String, Long> results)
    {
        checkNotNull(results, "results is null");
        for (Entry<String, Long> entry : results.entrySet()) {
            Long currentSum = resultsSum.get(entry.getKey());
            if (currentSum == null) {
                currentSum = 0L;
            }
            resultsSum.put(entry.getKey(), currentSum + entry.getValue());
        }
        resultsCount++;

        return this;
    }

    public Map<String, Double> getAverageResultsValues()
    {
        return Maps.transformValues(resultsSum, new Function<Long, Double>()
        {
            @Override
            public Double apply(@Nullable Long input)
            {
                return 1.0 * input / resultsCount;
            }
        });
    }

    public Map<String, String> getAverageResultsStrings()
    {
        return Maps.transformValues(resultsSum, new Function<Long, String>()
        {
            @Override
            public String apply(@Nullable Long input)
            {
                return String.format("%,3.2f", 1.0 * input / resultsCount);
            }
        });
    }

    @Override
    public void finished()
    {
    }
}
