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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.statistics.TableStatisticType;
import com.facebook.presto.spi.statistics.TableStatisticsMetadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.StatisticAggregations;
import com.facebook.presto.sql.planner.plan.StatisticAggregationsDescriptor;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.statistics.TableStatisticType.ROW_COUNT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class StatisticsAggregationPlanner
{
    private final SymbolAllocator symbolAllocator;
    private final Metadata metadata;

    public StatisticsAggregationPlanner(SymbolAllocator symbolAllocator, Metadata metadata)
    {
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public CreateStatisticAggregationsResult createStatisticsAggregation(TableStatisticsMetadata statisticsMetadata, Map<String, Symbol> columnToSymbolMap)
    {
        StatisticAggregationsDescriptor.Builder<Symbol> descriptor = StatisticAggregationsDescriptor.builder();

        List<String> groupingColumns = statisticsMetadata.getGroupingColumns();
        List<Symbol> groupingSymbols = groupingColumns.stream()
                .map(columnToSymbolMap::get)
                .collect(toImmutableList());

        for (int i = 0; i < groupingSymbols.size(); i++) {
            descriptor.addGrouping(groupingSymbols.get(i), groupingColumns.get(i));
        }

        ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> aggregations = ImmutableMap.builder();

        for (TableStatisticType type : statisticsMetadata.getTableStatistics()) {
            if (type != ROW_COUNT) {
                throw new PrestoException(NOT_SUPPORTED, "Table-wide statistic type not supported: " + type);
            }
        }

        FunctionRegistry functionRegistry = metadata.getFunctionRegistry();
        if (!statisticsMetadata.getTableStatistics().isEmpty()) {
            QualifiedName count = QualifiedName.of("count");
            AggregationNode.Aggregation aggregation = new AggregationNode.Aggregation(
                    new FunctionCall(count, ImmutableList.of()),
                    functionRegistry.resolveFunction(count, ImmutableList.of()),
                    Optional.empty());
            Symbol symbol = symbolAllocator.newSymbol("rowCount", BIGINT);
            aggregations.put(symbol, aggregation);
            descriptor.addTableStatistic(symbol, ROW_COUNT);
        }

        ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();
        for (ColumnStatisticMetadata columnStatisticMetadata : statisticsMetadata.getColumnStatistics()) {
            String columnName = columnStatisticMetadata.getColumnName();
            ColumnStatisticType statisticType = columnStatisticMetadata.getStatisticType();
            Symbol inputSymbol = columnToSymbolMap.get(columnName);
            verify(inputSymbol != null, "inputSymbol is null");
            Type inputType = requireNonNull(symbolAllocator.getTypes().get(inputSymbol), "inputType is null");
            SingleColumnStatisticAggregation aggregation = createColumnAggregation(statisticType, inputSymbol, inputType);
            Symbol symbol = symbolAllocator.newSymbol(statisticType + ":" + columnName, aggregation.getOutputType());
            aggregations.put(symbol, aggregation.getAggregation());
            descriptor.addColumnStatistic(symbol, columnStatisticMetadata);
            aggregation.getInputProjection().ifPresent(projections::put);
        }

        StatisticAggregations aggregation = new StatisticAggregations(aggregations.build(), groupingSymbols);
        return new CreateStatisticAggregationsResult(aggregation, descriptor.build(), projections.build());
    }

    private SingleColumnStatisticAggregation createColumnAggregation(ColumnStatisticType statisticType, Symbol input, Type inputType)
    {
        switch (statisticType) {
            case MIN: {
                checkArgument(inputType.isOrderable(), "Input type is not orderable: %s", inputType);
                return createAggregation(QualifiedName.of("min"), input.toSymbolReference(), inputType, inputType);
            }
            case MAX: {
                checkArgument(inputType.isOrderable(), "Input type is not orderable: %s", inputType);
                return createAggregation(QualifiedName.of("max"), input.toSymbolReference(), inputType, inputType);
            }
            case NUMBER_OF_DISTINCT_VALUES: {
                checkArgument(inputType.isComparable(), "Input type is not comparable: %s", inputType);
                // TODO: IMPLEMENT GENERIC APPROX DISTINCT
                if (!isApproxDistinctAvailable(inputType)) {
                    FunctionCall hashCode = new FunctionCall(QualifiedName.of(mangleOperatorName(HASH_CODE)), ImmutableList.of(input.toSymbolReference()));
                    return createAggregation(QualifiedName.of("approx_distinct"), hashCode, BIGINT, BIGINT);
                }
                return createAggregation(QualifiedName.of("approx_distinct"), input.toSymbolReference(), inputType, BIGINT);
            }
            case NUMBER_OF_NON_NULL_VALUES: {
                return createAggregation(QualifiedName.of("count"), input.toSymbolReference(), inputType, BIGINT);
            }
            case MAX_VALUE_SIZE_IN_BYTES: {
                if (!inputType.equals(VARBINARY) && !isVarcharType(inputType)) {
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported statistic %s for type: %s", statisticType, inputType));
                }
                Expression expression = new FunctionCall(QualifiedName.of("length"), ImmutableList.of(inputType.equals(VARBINARY) ? input.toSymbolReference() : new Cast(input.toSymbolReference(), "VARBINARY")));
                return createAggregation(QualifiedName.of("max"), expression, BIGINT, BIGINT);
            }
            case AVERAGE_VALUE_SIZE_IN_BYTES: {
                if (!inputType.equals(VARBINARY) && !isVarcharType(inputType)) {
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported statistic %s for type: %s", statisticType, inputType));
                }
                Expression expression = new FunctionCall(QualifiedName.of("length"), ImmutableList.of(inputType.equals(VARBINARY) ? input.toSymbolReference() : new Cast(input.toSymbolReference(), "VARBINARY")));
                return createAggregation(QualifiedName.of("avg"), expression, BIGINT, BIGINT);
            }
            case NUMBER_OF_TRUE_VALUES: {
                checkArgument(BOOLEAN.equals(inputType), "invalid input type %s for statistic type %s", inputType, statisticType);
                return createAggregation(QualifiedName.of("count_if"), input.toSymbolReference(), BOOLEAN, BIGINT);
            }
            default:
                throw new IllegalArgumentException("Unsupported statistic type: " + statisticType);
        }
    }

    private static boolean isApproxDistinctAvailable(Type inputType)
    {
        return inputType.equals(BIGINT) || inputType.equals(DOUBLE) || isVarcharType(inputType) || inputType.equals(VARBINARY);
    }

    private SingleColumnStatisticAggregation createAggregation(QualifiedName functionName, Expression expression, Type inputType, Type outputType)
    {
        Signature signature = metadata.getFunctionRegistry().resolveFunction(functionName, TypeSignatureProvider.fromTypes(ImmutableList.of(inputType)));
        Type resolvedType = metadata.getType(getOnlyElement(signature.getArgumentTypes()));
        Expression inputExpression = expression;
        if (!resolvedType.equals(inputType)) {
            inputExpression = new Cast(
                    expression,
                    resolvedType.getTypeSignature().toString(),
                    false,
                    metadata.getTypeManager().isTypeOnlyCoercion(inputType, resolvedType));
        }

        SymbolReference inputSymbolReference;
        Optional<Map.Entry<Symbol, Expression>> inputProjection;
        if (inputExpression instanceof SymbolReference) {
            inputProjection = Optional.empty();
            inputSymbolReference = (SymbolReference) inputExpression;
        }
        else {
            Symbol inputSymbol = symbolAllocator.newSymbol(inputExpression, resolvedType);
            inputProjection = Optional.of(new AbstractMap.SimpleImmutableEntry<>(inputSymbol, inputExpression));
            inputSymbolReference = inputSymbol.toSymbolReference();
        }
        return new SingleColumnStatisticAggregation(
                new AggregationNode.Aggregation(
                        new FunctionCall(functionName, ImmutableList.of(inputSymbolReference)),
                        signature,
                        Optional.empty()),
                outputType,
                inputProjection);
    }

    public static class CreateStatisticAggregationsResult
    {
        private final StatisticAggregations aggregations;
        private final StatisticAggregationsDescriptor<Symbol> descriptor;
        private final Map<Symbol, Expression> projections;

        private CreateStatisticAggregationsResult(
                StatisticAggregations aggregations,
                StatisticAggregationsDescriptor<Symbol> descriptor,
                Map<Symbol, Expression> projections)
        {
            this.aggregations = requireNonNull(aggregations, "statisticAggregations is null");
            this.descriptor = requireNonNull(descriptor, "descriptor is null");
            this.projections = ImmutableMap.copyOf(requireNonNull(projections, "projections is null"));
        }

        public StatisticAggregations getAggregations()
        {
            return aggregations;
        }

        public StatisticAggregationsDescriptor<Symbol> getDescriptor()
        {
            return descriptor;
        }

        public Map<Symbol, Expression> getProjections()
        {
            return projections;
        }
    }

    public static class SingleColumnStatisticAggregation
    {
        private final AggregationNode.Aggregation aggregation;
        private final Type outputType;
        private final Optional<Map.Entry<Symbol, Expression>> inputProjection;

        private SingleColumnStatisticAggregation(AggregationNode.Aggregation aggregation, Type outputType, Optional<Map.Entry<Symbol, Expression>> inputProjection)
        {
            this.aggregation = requireNonNull(aggregation, "aggregation is null");
            this.outputType = requireNonNull(outputType, "outputType is null");
            this.inputProjection = requireNonNull(inputProjection, "inputProjection is null").map(AbstractMap.SimpleImmutableEntry::new);
        }

        public AggregationNode.Aggregation getAggregation()
        {
            return aggregation;
        }

        public Type getOutputType()
        {
            return outputType;
        }

        public Optional<Map.Entry<Symbol, Expression>> getInputProjection()
        {
            return inputProjection;
        }
    }
}
