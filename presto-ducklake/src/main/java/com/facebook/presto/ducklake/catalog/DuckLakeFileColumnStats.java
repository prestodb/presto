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
package com.facebook.presto.ducklake.catalog;

import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * A row of {@code ducklake_file_column_stats} for one column of one data file. {@code
 * column_size_bytes} and {@code extra_stats} are not modeled; nothing in this connector uses them
 * yet.
 */
public class DuckLakeFileColumnStats
{
    private final long columnId;
    private final OptionalLong columnSizeBytes;
    private final OptionalLong valueCount;
    private final OptionalLong nullCount;
    private final Optional<String> minValue;
    private final Optional<String> maxValue;
    private final Optional<Boolean> containsNan;

    public DuckLakeFileColumnStats(
            long columnId,
            OptionalLong columnSizeBytes,
            OptionalLong valueCount,
            OptionalLong nullCount,
            Optional<String> minValue,
            Optional<String> maxValue,
            Optional<Boolean> containsNan)
    {
        this.columnId = columnId;
        this.columnSizeBytes = requireNonNull(columnSizeBytes, "columnSizeBytes is null");
        this.valueCount = requireNonNull(valueCount, "valueCount is null");
        this.nullCount = requireNonNull(nullCount, "nullCount is null");
        this.minValue = requireNonNull(minValue, "minValue is null");
        this.maxValue = requireNonNull(maxValue, "maxValue is null");
        this.containsNan = requireNonNull(containsNan, "containsNan is null");
    }

    public long getColumnId()
    {
        return columnId;
    }

    public OptionalLong getColumnSizeBytes()
    {
        return columnSizeBytes;
    }

    public OptionalLong getValueCount()
    {
        return valueCount;
    }

    public OptionalLong getNullCount()
    {
        return nullCount;
    }

    public Optional<String> getMinValue()
    {
        return minValue;
    }

    public Optional<String> getMaxValue()
    {
        return maxValue;
    }

    public Optional<Boolean> getContainsNan()
    {
        return containsNan;
    }
}
