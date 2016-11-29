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
package com.facebook.presto.sql.rewrite;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.sql.QueryUtil;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

class ShowColumnStatsRewriteResultBuilder
{
    private ShowColumnStatsRewriteResultBuilder() {}

    static List<Expression> buildStatisticsRows(TableStatistics tableStatistics, Map<ColumnHandle, String> columnNames, List<String> statisticsNames)
    {
        ImmutableList.Builder<Expression> rowsBuilder = ImmutableList.builder();

        // Stats for columns
        for (Map.Entry<ColumnHandle, ColumnStatistics> columnStats : tableStatistics.getColumnStatistics().entrySet()) {
            Map<String, Estimate> columnStatisticsValues = columnStats.getValue().getStatistics();
            rowsBuilder.add(createStatsRow(Optional.of(columnNames.get(columnStats.getKey())), statisticsNames, columnStatisticsValues));
        }

        // Stats for whole table
        rowsBuilder.add(createStatsRow(Optional.empty(), statisticsNames, tableStatistics.getTableStatistics()));

        return rowsBuilder.build();
    }

    static SelectItem[] buildSelectItems(List<String> columnNames)
    {
        return columnNames.stream().map(QueryUtil::unaliasedName).toArray(SelectItem[]::new);
    }

    static List<String> buildColumnsNames(List<String> statisticsNames)
    {
        ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();
        columnNamesBuilder.add("column_name");
        columnNamesBuilder.addAll(statisticsNames);
        return columnNamesBuilder.build();
    }

    private static Row createStatsRow(Optional<String> columnName, List<String> statisticsNames, Map<String, Estimate> columnStatisticsValues)
    {
        ImmutableList.Builder<Expression> rowValues = ImmutableList.builder();
        Expression columnNameExpression = columnName.map(name -> (Expression) new StringLiteral(name)).orElse(new NullLiteral());

        rowValues.add(columnNameExpression);
        for (String statName : statisticsNames) {
            rowValues.add(createStatisticValueOrNull(columnStatisticsValues, statName));
        }
        return new Row(rowValues.build());
    }

    private static Expression createStatisticValueOrNull(Map<String, Estimate> columnStatisticsValues, String statName)
    {
        if (columnStatisticsValues.containsKey(statName) && !columnStatisticsValues.get(statName).isValueUnknown()) {
            return new DoubleLiteral(Double.toString(columnStatisticsValues.get(statName).getValue()));
        }
        else {
            return new NullLiteral();
        }
    }
}
