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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.aggregation.MaxDataSizeForStats;
import com.facebook.presto.operator.aggregation.SumDataSizeForStats;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.statistics.TableStatisticType;
import com.facebook.presto.spi.statistics.TableStatisticsMetadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.StatisticAggregations;
import com.facebook.presto.sql.planner.plan.StatisticAggregationsDescriptor;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.statistics.TableStatisticType.ROW_COUNT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class StatisticsAggregationPlanner
{
    private final Session session;
    private final SymbolAllocator symbolAllocator;
    private final Metadata metadata;

    public StatisticsAggregationPlanner(Session session, SymbolAllocator symbolAllocator, Metadata metadata)
    {
        this.session = requireNonNull(session, "session is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public TableStatisticAggregation createStatisticsAggregation(TableStatisticsMetadata statisticsMetadata, Map<String, Symbol> columnToSymbolMap)
    {
        StatisticAggregationsDescriptor.Builder<Symbol> descriptor = StatisticAggregationsDescriptor.builder();

        List<String> groupingColumns = statisticsMetadata.getGroupingColumns();
        List<Symbol> groupingSymbols = groupingColumns.stream()
                .map(columnToSymbolMap::get)
                .collect(toImmutableList());

        for (int i = 0; i < groupingSymbols.size(); i++) {
            descriptor.addGrouping(groupingColumns.get(i), groupingSymbols.get(i));
        }

        ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> aggregations = ImmutableMap.builder();
        FunctionManager functionManager = metadata.getFunctionManager();
        for (TableStatisticType type : statisticsMetadata.getTableStatistics()) {
            if (type != ROW_COUNT) {
                throw new PrestoException(NOT_SUPPORTED, "Table-wide statistic type not supported: " + type);
            }
            QualifiedName count = QualifiedName.of("count");
            AggregationNode.Aggregation aggregation = new AggregationNode.Aggregation(
                    new FunctionCall(count, ImmutableList.of()),
                    functionManager.lookupFunction(count, ImmutableList.of()),
                    Optional.empty());
            Symbol symbol = symbolAllocator.newSymbol("rowCount", BIGINT);
            aggregations.put(symbol, aggregation);
            descriptor.addTableStatistic(ROW_COUNT, symbol);
        }

        for (ColumnStatisticMetadata columnStatisticMetadata : statisticsMetadata.getColumnStatistics()) {
            String columnName = columnStatisticMetadata.getColumnName();
            ColumnStatisticType statisticType = columnStatisticMetadata.getStatisticType();
            Symbol inputSymbol = columnToSymbolMap.get(columnName);
            verify(inputSymbol != null, "inputSymbol is null");
            Type inputType = symbolAllocator.getTypes().get(inputSymbol);
            verify(inputType != null, "inputType is null for symbol: %s", inputSymbol);
            ColumnStatisticsAggregation aggregation = createColumnAggregation(statisticType, inputSymbol, inputType);
            Symbol symbol = symbolAllocator.newSymbol(statisticType + ":" + columnName, aggregation.getOutputType());
            aggregations.put(symbol, aggregation.getAggregation());
            descriptor.addColumnStatistic(columnStatisticMetadata, symbol);
        }

        StatisticAggregations aggregation = new StatisticAggregations(aggregations.build(), groupingSymbols);
        return new TableStatisticAggregation(aggregation, descriptor.build());
    }

    private ColumnStatisticsAggregation createColumnAggregation(ColumnStatisticType statisticType, Symbol input, Type inputType)
    {
        switch (statisticType) {
            case MIN_VALUE:
                return createAggregation(QualifiedName.of("min"), input.toSymbolReference(), inputType, inputType);
            case MAX_VALUE:
                return createAggregation(QualifiedName.of("max"), input.toSymbolReference(), inputType, inputType);
            case NUMBER_OF_DISTINCT_VALUES:
                return createAggregation(QualifiedName.of("approx_distinct"), input.toSymbolReference(), inputType, BIGINT);
            case NUMBER_OF_NON_NULL_VALUES:
                return createAggregation(QualifiedName.of("count"), input.toSymbolReference(), inputType, BIGINT);
            case NUMBER_OF_TRUE_VALUES:
                return createAggregation(QualifiedName.of("count_if"), input.toSymbolReference(), BOOLEAN, BIGINT);
            case TOTAL_SIZE_IN_BYTES:
                return createAggregation(QualifiedName.of(SumDataSizeForStats.NAME), input.toSymbolReference(), inputType, BIGINT);
            case MAX_VALUE_SIZE_IN_BYTES:
                return createAggregation(QualifiedName.of(MaxDataSizeForStats.NAME), input.toSymbolReference(), inputType, BIGINT);
            default:
                throw new IllegalArgumentException("Unsupported statistic type: " + statisticType);
        }
    }

    private ColumnStatisticsAggregation createAggregation(QualifiedName functionName, SymbolReference input, Type inputType, Type outputType)
    {
        FunctionHandle functionHandle = metadata.getFunctionManager().resolveFunction(session, functionName, TypeSignatureProvider.fromTypes(ImmutableList.of(inputType)));
        Type resolvedType = metadata.getType(getOnlyElement(functionHandle.getSignature().getArgumentTypes()));
        verify(resolvedType.equals(inputType), "resolved function input type does not match the input type: %s != %s", resolvedType, inputType);
        return new ColumnStatisticsAggregation(
                new AggregationNode.Aggregation(
                        new FunctionCall(functionName, ImmutableList.of(input)),
                        functionHandle,
                        Optional.empty()),
                outputType);
    }

    public static class TableStatisticAggregation
    {
        private final StatisticAggregations aggregations;
        private final StatisticAggregationsDescriptor<Symbol> descriptor;

        private TableStatisticAggregation(
                StatisticAggregations aggregations,
                StatisticAggregationsDescriptor<Symbol> descriptor)
        {
            this.aggregations = requireNonNull(aggregations, "statisticAggregations is null");
            this.descriptor = requireNonNull(descriptor, "descriptor is null");
        }

        public StatisticAggregations getAggregations()
        {
            return aggregations;
        }

        public StatisticAggregationsDescriptor<Symbol> getDescriptor()
        {
            return descriptor;
        }
    }

    public static class ColumnStatisticsAggregation
    {
        private final AggregationNode.Aggregation aggregation;
        private final Type outputType;

        private ColumnStatisticsAggregation(AggregationNode.Aggregation aggregation, Type outputType)
        {
            this.aggregation = requireNonNull(aggregation, "aggregation is null");
            this.outputType = requireNonNull(outputType, "outputType is null");
        }

        public AggregationNode.Aggregation getAggregation()
        {
            return aggregation;
        }

        public Type getOutputType()
        {
            return outputType;
        }
    }
}
