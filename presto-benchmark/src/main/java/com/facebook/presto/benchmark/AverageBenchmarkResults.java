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
package com.facebook.presto.benchmark;

import com.google.common.collect.Maps;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import static java.util.Objects.requireNonNull;

public class AverageBenchmarkResults
        implements BenchmarkResultHook
{
    private final Map<String, Long> resultsSum = new LinkedHashMap<>();
    private int resultsCount;

    @Override
    public BenchmarkResultHook addResults(Map<String, Long> results)
    {
        requireNonNull(results, "results is null");
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
        return Maps.transformValues(resultsSum, input -> 1.0 * input / resultsCount);
    }

    public Map<String, String> getAverageResultsStrings()
    {
        return Maps.transformValues(resultsSum, input -> String.format("%,3.2f", 1.0 * input / resultsCount));
    }

    @Override
    public void finished()
    {
    }
}
