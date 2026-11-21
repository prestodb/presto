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

import static java.util.Objects.requireNonNull;

/**
 * A row of {@code ducklake_table_column_stats}, the table-level (as opposed to per-file) column
 * statistics. {@code extra_stats} is not modeled; nothing in this connector uses it yet.
 */
public class DuckLakeTableColumnStats
{
    private final long columnId;
    private final Optional<Boolean> containsNull;
    private final Optional<Boolean> containsNan;
    private final Optional<String> minValue;
    private final Optional<String> maxValue;

    public DuckLakeTableColumnStats(
            long columnId,
            Optional<Boolean> containsNull,
            Optional<Boolean> containsNan,
            Optional<String> minValue,
            Optional<String> maxValue)
    {
        this.columnId = columnId;
        this.containsNull = requireNonNull(containsNull, "containsNull is null");
        this.containsNan = requireNonNull(containsNan, "containsNan is null");
        this.minValue = requireNonNull(minValue, "minValue is null");
        this.maxValue = requireNonNull(maxValue, "maxValue is null");
    }

    public long getColumnId()
    {
        return columnId;
    }

    public Optional<Boolean> getContainsNull()
    {
        return containsNull;
    }

    public Optional<Boolean> getContainsNan()
    {
        return containsNan;
    }

    public Optional<String> getMinValue()
    {
        return minValue;
    }

    public Optional<String> getMaxValue()
    {
        return maxValue;
    }
}
