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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.StatisticAggregations;
import com.facebook.presto.spi.plan.StatisticAggregationsDescriptor;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.statistics.TableStatisticType;
import com.facebook.presto.spi.statistics.TableStatisticsMetadata;
import com.facebook.presto.sql.analyzer.FunctionAndTypeResolver;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.statistics.TableStatisticType.ROW_COUNT;
import static com.facebook.presto.sql.relational.SqlFunctionUtils.sqlFunctionToRowExpression;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class StatisticsAggregationPlanner
{
    private final VariableAllocator variableAllocator;
    private final FunctionAndTypeResolver functionAndTypeResolver;
    private final Session session;
    private final FunctionAndTypeManager functionAndTypeManager;

    public StatisticsAggregationPlanner(VariableAllocator variableAllocator, FunctionAndTypeManager functionAndTypeManager, Session session)
    {
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        this.session = requireNonNull(session, "session is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionAndTypeResolver = functionAndTypeManager.getFunctionAndTypeResolver();
    }

    public TableStatisticAggregation createStatisticsAggregation(TableStatisticsMetadata statisticsMetadata, Map<String, VariableReferenceExpression> columnToVariableMap)
    {
        StatisticAggregationsDescriptor.Builder<VariableReferenceExpression> descriptor = StatisticAggregationsDescriptor.builder();

        List<String> groupingColumns = statisticsMetadata.getGroupingColumns();
        List<VariableReferenceExpression> groupingVariables = groupingColumns.stream()
                .map(columnToVariableMap::get)
                .collect(toImmutableList());

        for (int i = 0; i < groupingVariables.size(); i++) {
            descriptor.addGrouping(groupingColumns.get(i), groupingVariables.get(i));
        }
        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> additionalVariables = ImmutableMap.builder();

        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = ImmutableMap.builder();
        StandardFunctionResolution functionResolution = new FunctionResolution(functionAndTypeResolver);
        for (TableStatisticType type : statisticsMetadata.getTableStatistics()) {
            if (type != ROW_COUNT) {
                throw new PrestoException(NOT_SUPPORTED, "Table-wide statistic type not supported: " + type);
            }
            AggregationNode.Aggregation aggregation = new AggregationNode.Aggregation(
                    new CallExpression(
                            "count",
                            functionResolution.countFunction(),
                            BIGINT,
                            ImmutableList.of()),
                    Optional.empty(),
                    Optional.empty(),
                    false,
                    Optional.empty());
            VariableReferenceExpression variable = variableAllocator.newVariable("rowCount", BIGINT);
            aggregations.put(variable, aggregation);
            descriptor.addTableStatistic(ROW_COUNT, variable);
        }

        for (ColumnStatisticMetadata columnStatisticMetadata : statisticsMetadata.getColumnStatistics()) {
            String columnName = columnStatisticMetadata.getColumnName();
            ColumnStatisticType statisticType = columnStatisticMetadata.getStatisticType();
            VariableReferenceExpression inputVariable = columnToVariableMap.get(columnName);
            verify(inputVariable != null, "inputVariable is null");
            ColumnStatisticsAggregation aggregation = createColumnAggregation(columnStatisticMetadata, inputVariable,
                    ImmutableMap.of(columnName, inputVariable.getName()));
            additionalVariables.putAll(aggregation.getInputProjections());
            VariableReferenceExpression variable = variableAllocator.newVariable(statisticType + ":" + columnName, aggregation.getOutputType());
            aggregations.put(variable, aggregation.getAggregation());
            descriptor.addColumnStatistic(columnStatisticMetadata, variable);
        }

        StatisticAggregations aggregation = new StatisticAggregations(aggregations.build(), groupingVariables);
        return new TableStatisticAggregation(aggregation, descriptor.build(), additionalVariables.build());
    }

    private ColumnStatisticsAggregation createColumnAggregationFromSqlFunction(
            String sqlFunction,
            VariableReferenceExpression input,
            Map<String, String> columnNameToInputVariableNameMap)
    {
        RowExpression expression = sqlFunctionToRowExpression(
                sqlFunction,
                ImmutableSet.of(input),
                functionAndTypeManager,
                session,
                columnNameToInputVariableNameMap);
        verify(expression instanceof CallExpression, "column statistic SQL expressions must represent a function call");
        CallExpression call = (CallExpression) expression;
        FunctionMetadata functionMeta = functionAndTypeResolver.getFunctionMetadata(call.getFunctionHandle());
        verify(functionMeta.getFunctionKind().equals(AGGREGATE), "column statistic function must be aggregates");
        // Aggregations input arguments are required to be variable reference expressions.
        // For each one that isn't, allocate a new variable to reference
        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> inputProjections = ImmutableMap.builder();
        List<RowExpression> callVariableArguments = call.getArguments()
                .stream()
                .map(argument -> {
                    if (argument instanceof VariableReferenceExpression) {
                        return argument;
                    }
                    VariableReferenceExpression newArgument = variableAllocator.newVariable(argument);
                    inputProjections.put(newArgument, argument);
                    return newArgument;
                })
                .collect(Collectors.toList());
        CallExpression callWithVariables = new CallExpression(
                call.getSourceLocation(),
                call.getDisplayName(),
                call.getFunctionHandle(),
                call.getType(),
                callVariableArguments);
        return new ColumnStatisticsAggregation(
                new AggregationNode.Aggregation(callWithVariables,
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty()),
                functionAndTypeResolver.getType(functionMeta.getReturnType()),
                inputProjections.build());
    }

    private ColumnStatisticsAggregation createColumnAggregationFromFunctionName(ColumnStatisticMetadata columnStatisticMetadata, VariableReferenceExpression input)
    {
        FunctionHandle functionHandle = functionAndTypeResolver.lookupFunction(columnStatisticMetadata.getFunction(), TypeSignatureProvider.fromTypes(ImmutableList.<Type>builder()
                .add(input.getType())
                .build()));
        FunctionMetadata functionMeta = functionAndTypeResolver.getFunctionMetadata(functionHandle);
        Type inputType = functionAndTypeResolver.getType(getOnlyElement(functionMeta.getArgumentTypes()));
        Type outputType = functionAndTypeResolver.getType(functionMeta.getReturnType());
        verify(inputType.equals(input.getType()) || input.getType().equals(UNKNOWN), "resolved function input type does not match the input type: %s != %s", inputType, input.getType());
        return new ColumnStatisticsAggregation(
                new AggregationNode.Aggregation(
                        new CallExpression(
                                input.getSourceLocation(),
                                columnStatisticMetadata.getFunction(),
                                functionHandle,
                                outputType,
                                ImmutableList.of(input)),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty()),
                outputType,
                ImmutableMap.of());
    }

    private ColumnStatisticsAggregation createColumnAggregation(ColumnStatisticMetadata columnStatisticMetadata, VariableReferenceExpression input,
            Map<String, String> columnNameToInputVariableNameMap)
    {
        if (columnStatisticMetadata.isSqlExpression()) {
            return createColumnAggregationFromSqlFunction(columnStatisticMetadata.getFunction(), input, columnNameToInputVariableNameMap);
        }

        return createColumnAggregationFromFunctionName(columnStatisticMetadata, input);
    }

    public static class TableStatisticAggregation
    {
        private final StatisticAggregations aggregations;
        private final StatisticAggregationsDescriptor<VariableReferenceExpression> descriptor;
        private final Map<VariableReferenceExpression, RowExpression> additionalVariables;

        private TableStatisticAggregation(
                StatisticAggregations aggregations,
                StatisticAggregationsDescriptor<VariableReferenceExpression> descriptor,
                Map<VariableReferenceExpression, RowExpression> additionalVariables)
        {
            this.aggregations = requireNonNull(aggregations, "statisticAggregations is null");
            this.descriptor = requireNonNull(descriptor, "descriptor is null");
            this.additionalVariables = requireNonNull(additionalVariables, "additionalVariables is null");
        }

        public StatisticAggregations getAggregations()
        {
            return aggregations;
        }

        public StatisticAggregationsDescriptor<VariableReferenceExpression> getDescriptor()
        {
            return descriptor;
        }

        public Map<VariableReferenceExpression, RowExpression> getAdditionalVariables()
        {
            return additionalVariables;
        }
    }

    public static class ColumnStatisticsAggregation
    {
        private final AggregationNode.Aggregation aggregation;
        private final Type outputType;
        private final Map<VariableReferenceExpression, RowExpression> inputProjections;

        private ColumnStatisticsAggregation(AggregationNode.Aggregation aggregation, Type outputType, Map<VariableReferenceExpression, RowExpression> inputProjections)
        {
            this.aggregation = requireNonNull(aggregation, "aggregation is null");
            this.outputType = requireNonNull(outputType, "outputType is null");
            this.inputProjections = requireNonNull(inputProjections, "additionalVariable is null");
        }

        public AggregationNode.Aggregation getAggregation()
        {
            return aggregation;
        }

        public Type getOutputType()
        {
            return outputType;
        }

        public Map<VariableReferenceExpression, RowExpression> getInputProjections()
        {
            return inputProjections;
        }
    }
}
