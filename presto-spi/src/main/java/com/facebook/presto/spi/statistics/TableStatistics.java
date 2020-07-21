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

package com.facebook.presto.spi.statistics;

import com.facebook.presto.spi.ColumnHandle;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public final class TableStatistics
{
    private static final TableStatistics EMPTY = TableStatistics.builder().build();

    private final Estimate rowCount;
    private final Map<ColumnHandle, ColumnStatistics> columnStatistics;

    public static TableStatistics empty()
    {
        return EMPTY;
    }

    private TableStatistics(Estimate rowCount, Map<ColumnHandle, ColumnStatistics> columnStatistics)
    {
        this.rowCount = requireNonNull(rowCount, "rowCount can not be null");
        if (!rowCount.isUnknown() && rowCount.getValue() < 0) {
            throw new IllegalArgumentException(format("rowCount must be greater than or equal to 0: %s", rowCount.getValue()));
        }
        this.columnStatistics = unmodifiableMap(requireNonNull(columnStatistics, "columnStatistics can not be null"));
    }

    @JsonProperty
    public Estimate getRowCount()
    {
        return rowCount;
    }

    @JsonProperty
    public Map<ColumnHandle, ColumnStatistics> getColumnStatistics()
    {
        return columnStatistics;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableStatistics that = (TableStatistics) o;
        return Objects.equals(rowCount, that.rowCount) &&
                Objects.equals(columnStatistics, that.columnStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rowCount, columnStatistics);
    }

    @Override
    public String toString()
    {
        return "TableStatistics{" +
                "rowCount=" + rowCount +
                ", columnStatistics=" + columnStatistics +
                '}';
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Estimate rowCount = Estimate.unknown();
        private Map<ColumnHandle, ColumnStatistics> columnStatisticsMap = new LinkedHashMap<>();

        public Builder setRowCount(Estimate rowCount)
        {
            this.rowCount = requireNonNull(rowCount, "rowCount can not be null");
            return this;
        }

        public Builder setColumnStatistics(ColumnHandle columnHandle, ColumnStatistics columnStatistics)
        {
            requireNonNull(columnHandle, "columnHandle can not be null");
            requireNonNull(columnStatistics, "columnStatistics can not be null");
            this.columnStatisticsMap.put(columnHandle, columnStatistics);
            return this;
        }

        public TableStatistics build()
        {
            return new TableStatistics(rowCount, columnStatisticsMap);
        }
    }
}
