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
package com.facebook.presto.orc.metadata.statistics;

import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.metadata.OrcType;
import com.google.common.collect.ImmutableMap;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StatisticsBuilders
{
    private StatisticsBuilders() {}

    public static Supplier<StatisticsBuilder> createStatisticsBuilderSupplier(OrcType orcType, ColumnWriterOptions columnWriterOptions)
    {
        requireNonNull(orcType, "orcType is null");
        requireNonNull(columnWriterOptions, "columnWriterOptions is null");

        switch (orcType.getOrcTypeKind()) {
            case BOOLEAN:
                return BooleanStatisticsBuilder::new;
            case BYTE:
                // TODO: sdruzkin - byte should use IntegerStatisticsBuilder
                return CountStatisticsBuilder::new;
            case SHORT:
            case INT:
            case LONG:
                return IntegerStatisticsBuilder::new;
            case FLOAT:
            case DOUBLE:
                return DoubleStatisticsBuilder::new;
            case BINARY:
                return BinaryStatisticsBuilder::new;
            case VARCHAR:
            case STRING:
                int stringStatisticsLimit = columnWriterOptions.getStringStatisticsLimit();
                return () -> new StringStatisticsBuilder(stringStatisticsLimit);
            case TIMESTAMP:
            case TIMESTAMP_MICROSECONDS:
            case LIST:
            case MAP:
            case STRUCT:
                return CountStatisticsBuilder::new;
            default:
                throw new IllegalArgumentException("Unsupported type: " + orcType);
        }
    }

    public static Map<Integer, ColumnStatistics> createEmptyColumnStatistics(List<OrcType> orcTypes, int nodeIndex, ColumnWriterOptions columnWriterOptions)
    {
        requireNonNull(orcTypes, "orcTypes is null");
        checkArgument(nodeIndex >= 0, "Invalid nodeIndex value: %s", nodeIndex);

        ImmutableMap.Builder<Integer, ColumnStatistics> columnStatistics = ImmutableMap.builder();
        LinkedList<Integer> stack = new LinkedList<>();
        stack.add(nodeIndex);

        while (!stack.isEmpty()) {
            int node = stack.removeLast();
            OrcType orcType = orcTypes.get(node);
            stack.addAll(orcType.getFieldTypeIndexes());

            StatisticsBuilder statisticsBuilder = createStatisticsBuilderSupplier(orcType, columnWriterOptions).get();
            ColumnStatistics emptyStatistics = statisticsBuilder.buildColumnStatistics();
            columnStatistics.put(node, emptyStatistics);
        }

        return columnStatistics.build();
    }
}
