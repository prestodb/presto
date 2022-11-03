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
package com.facebook.presto.plugin.clickhouse.optimization;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.plugin.clickhouse.ClickHouseColumnHandle;
import com.facebook.presto.plugin.clickhouse.ClickHouseTableHandle;
import com.facebook.presto.plugin.clickhouse.optimization.ClickHouseAggregationColumnNode.AggregationFunctionColumnNode;
import com.facebook.presto.plugin.clickhouse.optimization.ClickHouseAggregationColumnNode.GroupByColumnNode;
import com.facebook.presto.plugin.clickhouse.optimization.ClickHouseQueryGeneratorContext.Selection;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.facebook.presto.plugin.clickhouse.ClickHouseErrorCode.CLICKHOUSE_PUSHDOWN_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.plugin.clickhouse.optimization.ClickHousePushdownUtils.computeAggregationNodes;
import static com.facebook.presto.plugin.clickhouse.optimization.ClickHouseQueryGeneratorContext.Origin.DERIVED;
import static com.facebook.presto.plugin.clickhouse.optimization.ClickHouseQueryGeneratorContext.Origin.TABLE_COLUMN;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ClickHouseQueryGenerator
{
    private static final Logger log = Logger.get(ClickHouseQueryGenerator.class);
    private static final Map<String, String> UNARY_AGGREGATION_MAP = ImmutableMap.of(
            "min", "min",
            "max", "max",
            "avg", "avg",
            "sum", "sum",
            "count", "count");

    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;
    private final ClickHouseProjectExpressionConverter clickhouseProjectExpressionConverter;

    @Inject
    public ClickHouseQueryGenerator(
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution)
    {
        this.typeManager = requireNonNull(typeManager, "type manager is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "function metadata manager is null");
        this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standardFunctionResolution is null");
        this.clickhouseProjectExpressionConverter = new ClickHouseProjectExpressionConverter(typeManager, standardFunctionResolution);
    }

    public static class ClickHouseQueryGeneratorResult
    {
        private final GeneratedCkql generatedckql;
        private final ClickHouseQueryGeneratorContext context;

        public ClickHouseQueryGeneratorResult(
                GeneratedCkql generatedckql,
                ClickHouseQueryGeneratorContext context)
        {
            this.generatedckql = requireNonNull(generatedckql, "generatedckql is null");
            this.context = requireNonNull(context, "context is null");
        }

        public GeneratedCkql getGeneratedCkql()
        {
            return generatedckql;
        }

        public ClickHouseQueryGeneratorContext getContext()
        {
            return context;
        }
    }

    public Optional<ClickHouseQueryGeneratorResult> generate(PlanNode plan, ConnectorSession session)
    {
        try {
            ClickHouseQueryGeneratorContext context = requireNonNull(plan.accept(
                            new ClickHouseQueryPlanVisitor(session),
                            new ClickHouseQueryGeneratorContext()),
                    "Resulting context is null");

            return Optional.of(new ClickHouseQueryGeneratorResult(context.toQuery(), context));
        }
        catch (PrestoException e) {
            log.debug(e, "Possibly benign error when pushing plan into scan node %s", plan);
            return Optional.empty();
        }
    }

    public static class GeneratedCkql
    {
        final String table;
        final String ckql;
        final boolean pushdown;

        @JsonCreator
        public GeneratedCkql(
                @JsonProperty("table") String table,
                @JsonProperty("ckql") String ckql,
                @JsonProperty("pushdown") boolean pushdown)
        {
            this.table = table;
            this.ckql = ckql;
            this.pushdown = pushdown;
        }

        @JsonProperty("ckql")
        public String getCkql()
        {
            return ckql;
        }

        @JsonProperty("table")
        public String getTable()
        {
            return table;
        }

        @JsonProperty("pushdown")
        public boolean getPushdown()
        {
            return pushdown;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("ckql", ckql)
                    .add("table", table)
                    .add("pushdown", pushdown)
                    .toString();
        }
    }

    private class ClickHouseQueryPlanVisitor
            extends PlanVisitor<ClickHouseQueryGeneratorContext, ClickHouseQueryGeneratorContext>
    {
        private final ConnectorSession session;

        protected ClickHouseQueryPlanVisitor(ConnectorSession session)
        {
            this.session = session;
        }

        @Override
        public ClickHouseQueryGeneratorContext visitPlan(PlanNode node, ClickHouseQueryGeneratorContext context)
        {
            throw new PrestoException(CLICKHOUSE_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Unsupported pushdown for ClickHouse connector with plan node of type " + node);
        }

        protected VariableReferenceExpression getVariableReference(RowExpression expression)
        {
            if (expression instanceof VariableReferenceExpression) {
                return ((VariableReferenceExpression) expression);
            }
            throw new PrestoException(CLICKHOUSE_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Unsupported pushdown for ClickHouse connector. Expect variable reference, but get: " + expression);
        }

        @Override
        public ClickHouseQueryGeneratorContext visitMarkDistinct(MarkDistinctNode node, ClickHouseQueryGeneratorContext context)
        {
            requireNonNull(context, "context is null");
            return node.getSource().accept(this, context);
        }

        @Override
        public ClickHouseQueryGeneratorContext visitFilter(FilterNode node, ClickHouseQueryGeneratorContext context)
        {
            context = node.getSource().accept(this, context);
            requireNonNull(context, "context is null");
            Map<VariableReferenceExpression, Selection> selections = context.getSelections();
            ClickHouseFilterExpressionConverter clickhouseFilterExpressionConverter = new ClickHouseFilterExpressionConverter(typeManager, functionMetadataManager, standardFunctionResolution, session);
            String filter = node.getPredicate().accept(clickhouseFilterExpressionConverter, selections::get).getDefinition();
            return context.withFilter(filter).withOutputColumns(node.getOutputVariables());
        }

        @Override
        public ClickHouseQueryGeneratorContext visitProject(ProjectNode node, ClickHouseQueryGeneratorContext contextIn)
        {
            ClickHouseQueryGeneratorContext context = node.getSource().accept(this, contextIn);
            requireNonNull(context, "context is null");
            Map<VariableReferenceExpression, Selection> newSelections = new LinkedHashMap<>();

            node.getOutputVariables().forEach(variable -> {
                RowExpression expression = node.getAssignments().get(variable);
                ClickHouseProjectExpressionConverter projectExpressionConverter = clickhouseProjectExpressionConverter;
                ClickHouseColumnExpression clickhouseExpression = expression.accept(
                        projectExpressionConverter,
                        context.getSelections());
                newSelections.put(
                        variable,
                        new Selection(clickhouseExpression.getDefinition(), clickhouseExpression.getOrigin()));
            });
            return context.withProject(newSelections);
        }

        @Override
        public ClickHouseQueryGeneratorContext visitLimit(LimitNode node, ClickHouseQueryGeneratorContext context)
        {
            context = node.getSource().accept(this, context);
            requireNonNull(context, "context is null");
            return context.withLimit(node.getCount()).withOutputColumns(node.getOutputVariables());
        }

        @Override
        public ClickHouseQueryGeneratorContext visitTableScan(TableScanNode node, ClickHouseQueryGeneratorContext contextIn)
        {
            ClickHouseTableHandle tableHandle = (ClickHouseTableHandle) node.getTable().getConnectorHandle();
            //checkArgument(!tableHandle.getCkql().isPresent(), "ClickHouse tableHandle should not have ckql before pushdown");
            Map<VariableReferenceExpression, Selection> selections = new LinkedHashMap<>();
            node.getOutputVariables().forEach(outputColumn -> {
                ClickHouseColumnHandle clickHouseColumn = (ClickHouseColumnHandle) (node.getAssignments().get(outputColumn));
                checkArgument(clickHouseColumn.getType().equals(ClickHouseColumnHandle.ClickHouseColumnType.REGULAR), "Unexpected clickhouse column handle that is not regular: " + clickHouseColumn);
                selections.put(outputColumn, new Selection(clickHouseColumn.getColumnName(), TABLE_COLUMN));
            });
            //return new ClickHouseQueryGeneratorContext(selections, tableHandle.getSchemaName() + "." + tableHandle.getTableName(), node.getId());
            return new ClickHouseQueryGeneratorContext(selections, tableHandle.getTableName(), tableHandle.getSchemaName(), node.getId());
        }

        @Override
        public ClickHouseQueryGeneratorContext visitAggregation(AggregationNode node, ClickHouseQueryGeneratorContext contextIn)
        {
            List<ClickHouseAggregationColumnNode> aggregationColumnNodes = computeAggregationNodes(node);

            // 1st pass
            Set<VariableReferenceExpression> variablesInAggregation = new HashSet<>();
            for (ClickHouseAggregationColumnNode expression : aggregationColumnNodes) {
                switch (expression.getExpressionType()) {
                    case GROUP_BY: {
                        GroupByColumnNode groupByColumn = (GroupByColumnNode) expression;
                        VariableReferenceExpression groupByInputColumn = getVariableReference(groupByColumn.getInputColumn());
                        variablesInAggregation.add(groupByInputColumn);
                        break;
                    }
                    case AGGREGATE: {
                        AggregationFunctionColumnNode aggregationNode = (AggregationFunctionColumnNode) expression;
                        variablesInAggregation.addAll(
                                aggregationNode.getCallExpression().getArguments().stream()
                                        .filter(argument -> argument instanceof VariableReferenceExpression)
                                        .map(argument -> (VariableReferenceExpression) argument)
                                        .collect(Collectors.toList()));
                        break;
                    }
                    default:
                        throw new PrestoException(CLICKHOUSE_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Unsupported pushdown for clickhouse connector. Unknown aggregation expression:" + expression.getExpressionType());
                }
            }

            // now visit the child project node
            ClickHouseQueryGeneratorContext context = node.getSource().accept(this, contextIn.withVariablesInAggregation(variablesInAggregation));
            requireNonNull(context, "context is null");

            // 2nd pass
            Map<VariableReferenceExpression, Selection> newSelections = new LinkedHashMap<>();
            Map<VariableReferenceExpression, Selection> groupByColumns = new LinkedHashMap<>();
            Set<VariableReferenceExpression> hiddenColumnSet = new HashSet<>(context.getHiddenColumnSet());
            int aggregations = 0;
            boolean groupByExists = false;

            for (ClickHouseAggregationColumnNode expression : aggregationColumnNodes) {
                switch (expression.getExpressionType()) {
                    case GROUP_BY: {
                        GroupByColumnNode groupByColumn = (GroupByColumnNode) expression;
                        VariableReferenceExpression groupByInputColumn = getVariableReference(groupByColumn.getInputColumn());
                        VariableReferenceExpression outputColumn = getVariableReference(groupByColumn.getOutputColumn());
                        Selection clickhouseColumn = requireNonNull(context.getSelections().get(groupByInputColumn), "Group By column " + groupByInputColumn + " doesn't exist in input " + context.getSelections());

                        newSelections.put(outputColumn, new Selection(clickhouseColumn.getDefinition(), clickhouseColumn.getOrigin()));
                        groupByColumns.put(outputColumn, new Selection(clickhouseColumn.getDefinition(), clickhouseColumn.getOrigin()));
                        groupByExists = true;
                        break;
                    }
                    case AGGREGATE: {
                        AggregationFunctionColumnNode aggregationNode = (AggregationFunctionColumnNode) expression;
                        String clickhouseAggregationFunction = handleAggregationFunction(aggregationNode.getCallExpression(), context.getSelections());
//                        if (aggregationNode.getCallExpression().getDisplayName().equals("avg")) {
//                            newSelections.put(new VariableReferenceExpression(aggregationNode.getOutputColumn().getSourceLocation(), aggregationNode.getOutputColumn().getName(), DoubleType.DOUBLE),
//                                    new Selection(clickhouseAggregationFunction, DERIVED));
//                        }
//                        else {
//                            newSelections.put(getVariableReference(aggregationNode.getOutputColumn()),
//                                    new Selection(clickhouseAggregationFunction, DERIVED));
//                        }
                        newSelections.put(getVariableReference(aggregationNode.getOutputColumn()),
                                    new Selection(clickhouseAggregationFunction, DERIVED));
                        aggregations++;
                        break;
                    }
                    default:
                        throw new PrestoException(CLICKHOUSE_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Unsupported pushdown for ClickHouse connector. Unknown aggregation expression:" + expression.getExpressionType());
                }
            }

            // Handling non-aggregated group by
            if (groupByExists && aggregations == 0) {
                VariableReferenceExpression hidden = new VariableReferenceExpression(Optional.empty(), UUID.randomUUID().toString(), BigintType.BIGINT);
                newSelections.put(hidden, new Selection("count(*)", DERIVED));
                hiddenColumnSet.add(hidden);
                aggregations++;
            }
            return context.withAggregation(newSelections, groupByColumns, aggregations, hiddenColumnSet);
        }

        private String handleAggregationFunction(CallExpression aggregation, Map<VariableReferenceExpression, Selection> inputSelections)
        {
            String prestoAggregation = aggregation.getDisplayName().toLowerCase(ENGLISH);
            List<RowExpression> parameters = aggregation.getArguments();
            if (prestoAggregation.equals("count")) {
                if (parameters.size() <= 1) {
                    return format("count(%s)", parameters.isEmpty() ? "*" : inputSelections.get(getVariableReference(parameters.get(0))));
                }
            }
            else if (UNARY_AGGREGATION_MAP.containsKey(prestoAggregation) && aggregation.getArguments().size() == 1) {
                return format("%s(%s)", UNARY_AGGREGATION_MAP.get(prestoAggregation), inputSelections.get(getVariableReference(parameters.get(0))));
            }
            throw new PrestoException(CLICKHOUSE_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Unsupported pushdown for ClickHouse connector. Aggregation function: " + aggregation + " not supported");
        }
    }
}
