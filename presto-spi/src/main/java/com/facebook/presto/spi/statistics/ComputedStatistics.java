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

import com.facebook.presto.spi.block.Block;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class ComputedStatistics
{
    private final List<String> groupingColumns;
    private final List<Block> gropingValues;
    private final Map<TableStatisticType, Block> tableStatistics;
    private final Map<ColumnStatisticMetadata, Block> columnStatistics;

    public ComputedStatistics(
            List<String> groupingColumns,
            List<Block> gropingValues,
            Map<TableStatisticType, Block> tableStatistics,
            Map<ColumnStatisticMetadata, Block> columnStatistics)
    {
        this.groupingColumns = unmodifiableList(requireNonNull(groupingColumns, "groupingColumns is null"));
        this.gropingValues = unmodifiableList(requireNonNull(gropingValues, "gropingValues is null"));
        this.tableStatistics = unmodifiableMap(requireNonNull(tableStatistics, "tableStatistics is null"));
        this.columnStatistics = unmodifiableMap(requireNonNull(columnStatistics, "columnStatistics is null"));
    }

    public List<String> getGroupingColumns()
    {
        return groupingColumns;
    }

    public List<Block> getGropingValues()
    {
        return gropingValues;
    }

    public Map<TableStatisticType, Block> getTableStatistics()
    {
        return tableStatistics;
    }

    public Map<ColumnStatisticMetadata, Block> getColumnStatistics()
    {
        return columnStatistics;
    }

    public static Builder builder(List<String> groupingColumns, List<Block> gropingValues)
    {
        return new Builder(groupingColumns, gropingValues);
    }

    public static class Builder
    {
        private final List<String> groupingColumns;
        private final List<Block> gropingValues;
        private final Map<TableStatisticType, Block> tableStatistics = new HashMap<>();
        private final Map<ColumnStatisticMetadata, Block> columnStatistics = new HashMap<>();

        public Builder(List<String> groupingColumns, List<Block> gropingValues)
        {
            this.groupingColumns = requireNonNull(groupingColumns, "groupingColumns is null");
            this.gropingValues = requireNonNull(gropingValues, "gropingValues is null");
        }

        public void addTableStatistic(TableStatisticType type, Block value)
        {
            tableStatistics.put(type, value);
        }

        public void addColumnStatistic(ColumnStatisticMetadata columnStatisticMetadata, Block value)
        {
            columnStatistics.put(columnStatisticMetadata, value);
        }

        public ComputedStatistics build()
        {
            return new ComputedStatistics(
                    unmodifiableList(groupingColumns),
                    unmodifiableList(gropingValues),
                    unmodifiableMap(tableStatistics),
                    unmodifiableMap(columnStatistics));
        }
    }
}
