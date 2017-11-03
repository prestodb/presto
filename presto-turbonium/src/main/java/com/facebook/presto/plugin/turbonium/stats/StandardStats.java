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
package com.facebook.presto.plugin.turbonium.stats;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StandardStats<T>
        implements Stats<T>
{
    private final int size;
    private final int nullCount;
    private final int nonNullCount;
    private final Optional<T> min;
    private final Optional<T> max;
    private final Optional<T> delta;
    private final Optional<T> singleValue;
    private final Optional<Map<T, List<Integer>>> distinctValues;

    public StandardStats(
            int size,
            int nullCount,
            int nonNullCount,
            Optional<T> min,
            Optional<T> max,
            Optional<T> delta,
            Optional<T> singleValue,
            Optional<Map<T, List<Integer>>> distinctValues)
    {
        this.size = size;
        this.nullCount = nullCount;
        this.nonNullCount = nonNullCount;
        this.min = min;
        this.max = max;
        this.delta = delta;
        this.singleValue = singleValue;
        this.distinctValues = distinctValues;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public int getNullCount()
    {
        return nullCount;
    }

    @Override
    public int getNonNullCount()
    {
        return nonNullCount;
    }

    @Override
    public Optional<T> getMin()
    {
        return min;
    }

    @Override
    public Optional<T> getMax()
    {
        return max;
    }

    @Override
    public Optional<T> getDelta()
    {
        return delta;
    }

    @Override
    public Optional<T> getSingleValue()
    {
        return singleValue;
    }

    @Override
    public Optional<Map<T, List<Integer>>> getDistinctValues()
    {
        return distinctValues;
    }
}
