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

package io.prestosql.plugin.tpcds.statistics;

import com.teradata.tpcds.column.ColumnType;

import java.util.Optional;
import java.util.TreeSet;

import static java.util.Objects.requireNonNull;

class ColumnStatisticsRecorder
{
    private final TreeSet<Object> nonNullValues = new TreeSet<>();
    private final ColumnType type;
    private long nullsCount;

    public ColumnStatisticsRecorder(ColumnType type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    public void record(Comparable<?> value)
    {
        if (value != null) {
            nonNullValues.add(value);
        }
        else {
            nullsCount++;
        }
    }

    public ColumnStatisticsData getRecording()
    {
        return new ColumnStatisticsData(
                getDistinctValuesCount(),
                getNullsCount(),
                getLowestValue(),
                getHighestValue(),
                getDataSize());
    }

    private long getDistinctValuesCount()
    {
        return nonNullValues.size();
    }

    private long getNullsCount()
    {
        return nullsCount;
    }

    private Optional<Object> getLowestValue()
    {
        return nonNullValues.size() > 0 ? Optional.of(nonNullValues.first()) : Optional.empty();
    }

    private Optional<Object> getHighestValue()
    {
        return nonNullValues.size() > 0 ? Optional.of(nonNullValues.last()) : Optional.empty();
    }

    public Optional<Long> getDataSize()
    {
        if (type.getBase() == ColumnType.Base.VARCHAR || type.getBase() == ColumnType.Base.CHAR) {
            return Optional.of(nonNullValues.stream()
                    .map(String.class::cast)
                    .map(value -> {
                        if (type.getBase() == ColumnType.Base.CHAR) {
                            return value.replaceFirst(" +$", "");
                        }
                        return value;
                    })
                    .mapToLong(String::length)
                    .sum());
        }
        return Optional.empty();
    }
}
