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
package com.facebook.presto.tpch.statistics;

import java.util.Optional;
import java.util.TreeSet;

class ColumnStatisticsRecorder
{
    private final TreeSet<Object> nonNullValues = new TreeSet<>();

    void record(Comparable<?> value)
    {
        if (value != null) {
            nonNullValues.add(value);
        }
    }

    ColumnStatisticsData getRecording()
    {
        return new ColumnStatisticsData(
                Optional.of(getUniqueValuesCount()),
                getLowestValue(),
                getHighestValue());
    }

    private long getUniqueValuesCount()
    {
        return nonNullValues.size();
    }

    private Optional<Object> getLowestValue()
    {
        return nonNullValues.size() > 0 ? Optional.of(nonNullValues.first()) : Optional.empty();
    }

    private Optional<Object> getHighestValue()
    {
        return nonNullValues.size() > 0 ? Optional.of(nonNullValues.last()) : Optional.empty();
    }
}
