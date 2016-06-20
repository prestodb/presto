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

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.spi.statistics.Estimate.unknownValue;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public final class TableStatistics
{
    public static final TableStatistics EMPTY_STATISTICS = TableStatistics.builder().build();

    private final Estimate rowCount;
    private final Map<ColumnHandle, ColumnStatistics> columnStatistics;

    public TableStatistics(Estimate rowCount, Map<ColumnHandle, ColumnStatistics> columnStatistics)
    {
        this.rowCount = requireNonNull(rowCount, "rowCount can not be null");
        this.columnStatistics = unmodifiableMap(new HashMap<>(requireNonNull(columnStatistics, "columnStatistics can not be null")));
    }

    public Estimate getRowCount()
    {
        return rowCount;
    }

    public Map<ColumnHandle, ColumnStatistics> getColumnStatistics()
    {
        return columnStatistics;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Estimate rowCount = unknownValue();
        private Map<ColumnHandle, ColumnStatistics> columnStatisticsMap = new HashMap<>();

        public Builder setRowCount(Estimate rowCount)
        {
            this.rowCount = requireNonNull(rowCount, "rowCount can not be null");
            return this;
        }

        public Builder setColumnStatistics(ColumnHandle columnName, ColumnStatistics columnStatistics)
        {
            requireNonNull(columnName, "columnName can not be null");
            requireNonNull(columnStatistics, "columnStatistics can not be null");
            this.columnStatisticsMap.put(columnName, columnStatistics);
            return this;
        }

        public TableStatistics build()
        {
            return new TableStatistics(rowCount, columnStatisticsMap);
        }
    }
}
