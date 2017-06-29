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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractStatsBuilder<T>
    implements StatsBuilder<T>
{
    protected static final int ADD_DISTINCT_LIMIT = 0x100;

    private int nullCount;
    private int nonNullCount;
    private int distinctValueCount = 0;
    private Optional<Map<T, List<Integer>>> distinctValues = Optional.of(new HashMap<>());

    protected abstract void setIfMin(T value);
    protected abstract void setIfMax(T value);
    protected abstract Optional<T> getDelta();
    protected abstract Optional<T> getMin();
    protected abstract Optional<T> getMax();

    private Optional<T> getSingleValue()
    {
        if (distinctValues.isPresent() && distinctValues.get().size() == 1) {
            return Optional.of(distinctValues.get().keySet().iterator().next());
        }
        else {
            return Optional.empty();
        }
    }

    private final void addDistinct(T value, int position)
    {
        if (canAddDistinctValue()) {
            List<Integer> positions = distinctValues.get().computeIfAbsent(value, k -> new ArrayList<>());
            if (positions.isEmpty()) {
                distinctValueCount++;
                if (distinctValueCount > ADD_DISTINCT_LIMIT) {
                    distinctValues = Optional.empty();
                    return;
                }
            }
            positions.add(position);
        }
    }

    protected boolean canAddDistinctValue()
    {
        return distinctValues.isPresent();
    }

    @Override
    public void add(T value, int position)
    {
        if (value == null) {
            nullCount++;
        }
        else {
            setIfMin(value);
            setIfMax(value);
            addDistinct(value, position);
            nonNullCount++;
        }
    }

    @Override
    public Stats<T> build()
    {
        return new StandardStats<>(
                nullCount + nonNullCount,
                nullCount,
                nonNullCount,
                getMin(),
                getMax(),
                getDelta(),
                getSingleValue(),
                distinctValues);
    }
}
