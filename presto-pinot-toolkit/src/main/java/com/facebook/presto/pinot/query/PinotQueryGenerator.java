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
package com.facebook.presto.pinot.query;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.pinot.PinotColumnHandle;
import com.facebook.presto.pinot.PinotConfig;
import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.pinot.PinotPushdownUtils.AggregationColumnNode;
import com.facebook.presto.pinot.PinotPushdownUtils.AggregationFunctionColumnNode;
import com.facebook.presto.pinot.PinotPushdownUtils.GroupByColumnNode;
import com.facebook.presto.pinot.PinotSessionProperties;
import com.facebook.presto.pinot.PinotTableHandle;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Selection;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.pinot.PinotPushdownUtils.checkSupported;
import static com.facebook.presto.pinot.PinotPushdownUtils.computeAggregationNodes;
import static com.facebook.presto.pinot.PinotPushdownUtils.getLiteralAsString;
import static com.facebook.presto.pinot.PinotPushdownUtils.getOrderingScheme;
import static com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin.DERIVED;
import static com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin.LITERAL;
import static com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin.TABLE_COLUMN;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PinotQueryGenerator
{
    private static final Logger log = Logger.get(PinotQueryGenerator.class);
    private static final double LOWEST_APPROX_DISTINCT_MAX_STANDARD_ERROR = 0.0040625;
    private static final double HIGHEST_APPROX_DISTINCT_MAX_STANDARD_ERROR = 0.26000;
    private static final Map<String, String> UNARY_AGGREGATION_MAP =
            ImmutableMap.<String, String>builder()
                    .put("min", "min")
                    .put("max", "max")
                    .put("avg", "avg")
                    .put("sum", "sum")
                    .put("distinctcount", "DISTINCTCOUNT")
                    .build();

    private final PinotConfig pinotConfig;
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;
    private final PinotFilterExpressionConverter pinotFilterExpressionConverter;
    private final PinotProjectExpressionConverter pinotProjectExpressionConverter;

    @Inject
    public PinotQueryGenerator(
            PinotConfig pinotConfig,
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution)
    {
        this.pinotConfig = requireNonNull(pinotConfig, "pinot config is null");
        this.typeManager = requireNonNull(typeManager, "type manager is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "function metadata manager is null");
        this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standardFunctionResolution is null");
        this.pinotFilterExpressionConverter = new PinotFilterExpressionConverter(this.typeManager, this.functionMetadataManager, standardFunctionResolution);
        this.pinotProjectExpressionConverter = new PinotProjectExpressionConverter(typeManager, standardFunctionResolution);
    }

    public static class PinotQueryGeneratorResult
    {
        private final GeneratedPinotQuery generatedPinotQuery;
        private final PinotQueryGeneratorContext context;

        public PinotQueryGeneratorResult(GeneratedPinotQuery generatedPinotQuery, PinotQueryGeneratorContext context)
        {
            this.generatedPinotQuery = requireNonNull(generatedPinotQuery, "generatedPinotQuery is null");
            this.context = requireNonNull(context, "context is null");
        }

        public GeneratedPinotQuery getGeneratedPinotQuery()
        {
            return generatedPinotQuery;
        }

        public PinotQueryGeneratorContext getContext()
        {
            return context;
        }
    }

    public Optional<PinotQueryGeneratorResult> generate(PlanNode plan, ConnectorSession session)
    {
        try {
            boolean usePinotSqlSyntax = PinotSessionProperties.isUsePinotSqlForBrokerQueries(session);
            PinotQueryGeneratorContext context = requireNonNull(plan.accept(
                    new PinotQueryPlanVisitor(session),
                    new PinotQueryGeneratorContext(usePinotSqlSyntax)),
                    "Resulting context is null");
            return Optional.of(new PinotQueryGeneratorResult(context.toQuery(pinotConfig, session), context));
        }
        catch (PinotException e) {
            log.debug(e, "Possibly benign error when pushing plan into scan node %s", plan);
            return Optional.empty();
        }
    }

    public enum PinotQueryFormat {
        PQL,
        SQL
    }

    public static class GeneratedPinotQuery
    {
        final String table;
        final String query;
        final PinotQueryFormat format;
        final List<Integer> expectedColumnIndices;
        final int groupByClauses;
        final boolean haveFilter;
        final boolean isQueryShort;

        @JsonCreator
        public GeneratedPinotQuery(
                @JsonProperty("table") String table,
                @JsonProperty("query") String query,
                @JsonProperty("format") PinotQueryFormat format,
                @JsonProperty("expectedColumnIndices") List<Integer> expectedColumnIndices,
                @JsonProperty("groupByClauses") int groupByClauses,
                @JsonProperty("haveFilter") boolean haveFilter,
                @JsonProperty("isQueryShort") boolean isQueryShort)
        {
            this.table = table;
            this.query = query;
            this.format = format;
            checkState((query != null), "Expected only one of query to be present");
            this.expectedColumnIndices = expectedColumnIndices;
            this.groupByClauses = groupByClauses;
            this.haveFilter = haveFilter;
            this.isQueryShort = isQueryShort;
        }

        @JsonProperty("table")
        public String getTable()
        {
            return table;
        }

        @JsonProperty("query")
        public String getQuery()
        {
            return query;
        }

        @JsonProperty("format")
        public PinotQueryFormat getFormat()
        {
            return format;
        }

        @JsonProperty("expectedColumnIndices")
        public List<Integer> getExpectedColumnIndices()
        {
            return expectedColumnIndices;
        }

        @JsonProperty("groupByClauses")
        public int getGroupByClauses()
        {
            return groupByClauses;
        }

        @JsonProperty("haveFilter")
        public boolean isHaveFilter()
        {
            return haveFilter;
        }

        @JsonProperty("isQueryShort")
        public boolean isQueryShort()
        {
            return isQueryShort;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                .add("query", query)
                .add("format", format)
                .add("table", table)
                .add("expectedColumnIndices", expectedColumnIndices)
                .add("groupByClauses", groupByClauses)
                .add("haveFilter", haveFilter)
                .add("isQueryShort", isQueryShort)
                .toString();
        }
    }

    class PinotQueryPlanVisitor
            extends PlanVisitor<PinotQueryGeneratorContext, PinotQueryGeneratorContext>
    {
        private final ConnectorSession session;
        private final boolean forbidBrokerQueries;
        private final boolean useSqlSyntax;
        private final boolean pushdownTopnBrokerQueries;

        protected PinotQueryPlanVisitor(ConnectorSession session)
        {
            this.session = session;
            this.forbidBrokerQueries = PinotSessionProperties.isForbidBrokerQueries(session);
            this.useSqlSyntax = PinotSessionProperties.isUsePinotSqlForBrokerQueries(session);
            this.pushdownTopnBrokerQueries = PinotSessionProperties.getPushdownTopnBrokerQueries(session);
        }

        @Override
        public PinotQueryGeneratorContext visitPlan(PlanNode node, PinotQueryGeneratorContext context)
        {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Don't know how to handle plan node of type " + node);
        }

        protected VariableReferenceExpression getVariableReference(RowExpression expression)
        {
            if (expression instanceof VariableReferenceExpression) {
                return ((VariableReferenceExpression) expression);
            }
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Expected a variable reference but got " + expression);
        }

        @Override
        public PinotQueryGeneratorContext visitMarkDistinct(MarkDistinctNode node, PinotQueryGeneratorContext context)
        {
            requireNonNull(context, "context is null");
            return node.getSource().accept(this, context);
        }

        @Override
        public PinotQueryGeneratorContext visitFilter(FilterNode node, PinotQueryGeneratorContext context)
        {
            context = node.getSource().accept(this, context);
            requireNonNull(context, "context is null");
            Map<VariableReferenceExpression, Selection> selections = context.getSelections();
            String filter = node.getPredicate().accept(pinotFilterExpressionConverter, selections::get).getDefinition();
            return context.withFilter(filter).withOutputColumns(node.getOutputVariables());
        }

        @Override
        public PinotQueryGeneratorContext visitProject(ProjectNode node, PinotQueryGeneratorContext contextIn)
        {
            PinotQueryGeneratorContext context = node.getSource().accept(this, contextIn);
            requireNonNull(context, "context is null");
            Map<VariableReferenceExpression, Selection> newSelections = new HashMap<>();
            LinkedHashSet<VariableReferenceExpression> newOutputs = new LinkedHashSet<>();
            node.getOutputVariables().forEach(variable -> {
                RowExpression expression = node.getAssignments().get(variable);
                PinotExpression pinotExpression = expression.accept(
                        contextIn.getVariablesInAggregation().contains(variable) ?
                                new PinotAggregationProjectConverter(typeManager, functionMetadataManager, standardFunctionResolution, session, variable) :
                                pinotProjectExpressionConverter,
                        context.getSelections());
                newSelections.put(
                        variable,
                        new Selection(pinotExpression.getDefinition(), pinotExpression.getOrigin()));
                newOutputs.add(variable);
            });
            if (useSqlSyntax) {
                // For PinotQueryGeneratorContext, selections should contain the mapping from varRef to rowExpression,
                // and output, groupBy, orderBy objects only hold varRefs.
                //
                // When we try to generate Pinot query, the varRef in groupBy may not be in output.
                // E.g. a sample Presto query: `select count(*) group by A`.
                // - To generate PQL, we expect column `A` is always in selections.
                // - To generate SQL, we need to hold all the mappings somewhere, which is in selections, then generate
                //   SQL based on output object.
                newSelections.putAll(context.getSelections());
            }
            return context.withProject(newSelections, newOutputs);
        }

        @Override
        public PinotQueryGeneratorContext visitTableScan(TableScanNode node, PinotQueryGeneratorContext contextIn)
        {
            PinotTableHandle tableHandle = (PinotTableHandle) node.getTable().getConnectorHandle();
            checkSupported(!tableHandle.getPinotQuery().isPresent(), "Expect to see no existing pql");
            checkSupported(!tableHandle.getIsQueryShort().isPresent(), "Expect to see no existing pql");
            Map<VariableReferenceExpression, Selection> selections = new HashMap<>();
            LinkedHashSet<VariableReferenceExpression> outputs = new LinkedHashSet<>();
            node.getOutputVariables().forEach(outputColumn -> {
                PinotColumnHandle pinotColumn = (PinotColumnHandle) (node.getAssignments().get(outputColumn));
                checkSupported(pinotColumn.getType().equals(PinotColumnHandle.PinotColumnType.REGULAR), "Unexpected pinot column handle that is not regular: %s", pinotColumn);
                selections.put(outputColumn, new Selection(pinotColumn.getColumnName(), TABLE_COLUMN));
                outputs.add(outputColumn);
            });
            return new PinotQueryGeneratorContext(selections, outputs, tableHandle.getTableName(), PinotSessionProperties.isUsePinotSqlForBrokerQueries(session));
        }

        private String handleAggregationFunction(CallExpression aggregation, Map<VariableReferenceExpression, Selection> inputSelections)
        {
            String prestoAggregation = aggregation.getDisplayName().toLowerCase(ENGLISH);
            List<RowExpression> parameters = aggregation.getArguments();
            switch (prestoAggregation) {
                case "count":
                    if (parameters.size() <= 1) {
                        return format("count(%s)", parameters.isEmpty() ? "*" : inputSelections.get(getVariableReference(parameters.get(0))));
                    }
                    break;
                case "approx_percentile":
                    return handleApproxPercentile(aggregation, inputSelections);
                case "approx_distinct":
                    return handleApproxDistinct(aggregation, inputSelections);
                default:
                    if (UNARY_AGGREGATION_MAP.containsKey(prestoAggregation) && aggregation.getArguments().size() == 1) {
                        return format("%s(%s)", UNARY_AGGREGATION_MAP.get(prestoAggregation), inputSelections.get(getVariableReference(parameters.get(0))));
                    }
            }

            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("aggregation function '%s' not supported yet", aggregation));
        }

        private String handleApproxPercentile(CallExpression aggregation, Map<VariableReferenceExpression, Selection> inputSelections)
        {
            List<RowExpression> inputs = aggregation.getArguments();
            if (inputs.size() != 2) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Cannot handle approx_percentile function " + aggregation);
            }

            String fractionString;
            RowExpression fractionInput = inputs.get(1);

            if (fractionInput instanceof ConstantExpression) {
                fractionString = getLiteralAsString((ConstantExpression) fractionInput);
            }
            else if (fractionInput instanceof VariableReferenceExpression) {
                Selection fraction = inputSelections.get(fractionInput);
                if (fraction.getOrigin() != LITERAL) {
                    throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(),
                            "Cannot handle approx_percentile percentage argument be a non literal " + aggregation);
                }
                fractionString = fraction.getDefinition();
            }
            else {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Expected the fraction to be a constant or a variable " + fractionInput);
            }

            int percentile = getValidPercentile(fractionString);
            if (percentile < 0) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(),
                        format("Cannot handle approx_percentile parsed as %d from input %s (function %s)", percentile, fractionString, aggregation));
            }
            return format("PERCENTILEEST%d(%s)", percentile, inputSelections.get(getVariableReference(inputs.get(0))));
        }

        private String handleApproxDistinct(CallExpression aggregation, Map<VariableReferenceExpression, Selection> inputSelections)
        {
            List<RowExpression> inputs = aggregation.getArguments();
            if (inputs.isEmpty() || inputs.size() > 2) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Cannot handle approx_distinct function " + aggregation);
            }
            Selection selection = inputSelections.get(getVariableReference(inputs.get(0)));
            if (inputs.size() == 1) {
                return format("DISTINCTCOUNTHLL(%s)", selection);
            }
            RowExpression standardErrorInput = inputs.get(1);
            String standardErrorString;
            if (standardErrorInput instanceof ConstantExpression) {
                standardErrorString = getLiteralAsString((ConstantExpression) standardErrorInput);
            }
            else if (standardErrorInput instanceof VariableReferenceExpression) {
                Selection fraction = inputSelections.get(standardErrorInput);
                if (fraction.getOrigin() != LITERAL) {
                    throw new PinotException(
                            PINOT_UNSUPPORTED_EXPRESSION,
                            Optional.empty(),
                            "Cannot handle approx_distinct standard error argument be a non literal " + aggregation);
                }
                standardErrorString = fraction.getDefinition();
            }
            else {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Expected the standard error to be a constant or a variable " + standardErrorInput);
            }

            double standardError;
            try {
                standardError = Double.parseDouble(standardErrorString);
                if (standardError <= LOWEST_APPROX_DISTINCT_MAX_STANDARD_ERROR || standardError >= HIGHEST_APPROX_DISTINCT_MAX_STANDARD_ERROR) {
                    throw new PinotException(
                            PINOT_UNSUPPORTED_EXPRESSION,
                            Optional.empty(),
                            format("Cannot handle approx_distinct parsed as %f from input %s (function %s)", standardError, standardErrorString, aggregation));
                }
            }
            catch (Exception e) {
                throw new PinotException(
                        PINOT_UNSUPPORTED_EXPRESSION,
                        Optional.empty(),
                        format("Cannot handle approx_distinct parsing to numerical value from input %s (function %s)", standardErrorString, aggregation));
            }
            // Pinot uses DISTINCTCOUNTHLL to do distinct count estimation, with hyperloglog algorithm.
            //
            // The HyperLogLog (HLL) data structure is a probabilistic data structure used to estimate the cardinality
            // of a data set.
            // In order to construct HLL data structure, the parameter log2m is used which represents the number of
            // registers used internally by HLL.
            //
            // If we want a higher accuracy, we need to set these to higher values. Such a configuration
            // will have additional overhead because our HLL will occupy more memory. If we're fine with lower accuracy,
            // we can lower those parameters, and our HLL will occupy less memory.
            //
            // The relative standard deviation of HyperLoglog is:
            //     rsd = 1.106 / sqrt(2^(log2m))
            // So:
            //     log2m = 2 * log(1.106 / rsd) / log(2)
            int log2m = (int) (2 * Math.log(1.106 / standardError) / Math.log(2));
            if (log2m < 1) {
                throw new PinotException(
                        PINOT_UNSUPPORTED_EXPRESSION,
                        Optional.empty(),
                        format("Cannot handle approx_distinct, the log2m generated from error is %d from input %s (function %s)", log2m, standardErrorString, aggregation));
            }
            return format("DISTINCTCOUNTHLL(%s, %d)", selection, log2m);
        }

        private int getValidPercentile(String fraction)
        {
            try {
                double percent = Double.parseDouble(fraction);
                if (percent < 0 || percent > 1) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
                }
                percent = percent * 100.0;
                if (percent == Math.floor(percent)) {
                    return (int) percent;
                }
            }
            catch (NumberFormatException ne) {
                // Skip
            }
            return -1;
        }

        @Override
        public PinotQueryGeneratorContext visitAggregation(AggregationNode node, PinotQueryGeneratorContext contextIn)
        {
            List<AggregationColumnNode> aggregationColumnNodes = computeAggregationNodes(node);

            // Make two passes over the aggregatinColumnNodes: In the first pass identify all the variables that will be used
            // Then pass that context to the source
            // And finally, in the second pass actually generate the PQL

            // 1st pass
            Set<VariableReferenceExpression> variablesInAggregation = new HashSet<>();
            for (AggregationColumnNode expression : aggregationColumnNodes) {
                switch (expression.getExpressionType()) {
                    case GROUP_BY: {
                        GroupByColumnNode groupByColumn = (GroupByColumnNode) expression;
                        VariableReferenceExpression groupByInputColumn = getVariableReference(groupByColumn.getInputColumn());
                        checkState(groupByInputColumn.getType() instanceof FixedWidthType || groupByInputColumn.getType() instanceof VarcharType);
                        variablesInAggregation.add(groupByInputColumn);
                        break;
                    }
                    case AGGREGATE: {
                        AggregationFunctionColumnNode aggregationNode = (AggregationFunctionColumnNode) expression;
                        variablesInAggregation.addAll(
                                aggregationNode
                                        .getCallExpression()
                                        .getArguments()
                                        .stream()
                                        .filter(argument -> argument instanceof VariableReferenceExpression)
                                        .map(argument -> (VariableReferenceExpression) argument)
                                        .collect(Collectors.toList()));
                        break;
                    }
                    default:
                        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "unknown aggregation expression: " + expression.getExpressionType());
                }
            }

            // now visit the child project node
            PinotQueryGeneratorContext context = node.getSource().accept(this, contextIn.withVariablesInAggregation(variablesInAggregation));
            requireNonNull(context, "context is null");
            checkSupported(!node.getStep().isOutputPartial(), "partial aggregations are not supported in Pinot pushdown framework");
            checkSupported(!forbidBrokerQueries, "Cannot push aggregation in segment mode");

            // 2nd pass
            Map<VariableReferenceExpression, Selection> newSelections = new HashMap<>();
            LinkedHashSet<VariableReferenceExpression> outputs = new LinkedHashSet<>();
            LinkedHashSet<VariableReferenceExpression> groupByColumns = new LinkedHashSet<>();
            Set<VariableReferenceExpression> hiddenColumnSet = new HashSet<>(context.getHiddenColumnSet());
            int aggregations = 0;
            boolean groupByExists = false;

            for (AggregationColumnNode expression : aggregationColumnNodes) {
                switch (expression.getExpressionType()) {
                    case GROUP_BY: {
                        GroupByColumnNode groupByColumn = (GroupByColumnNode) expression;
                        VariableReferenceExpression groupByInputColumn = getVariableReference(groupByColumn.getInputColumn());
                        VariableReferenceExpression outputColumn = getVariableReference(groupByColumn.getOutputColumn());
                        Selection pinotColumn = requireNonNull(context.getSelections().get(groupByInputColumn), "Group By column " + groupByInputColumn + " doesn't exist in input " + context.getSelections());

                        newSelections.put(outputColumn, new Selection(pinotColumn.getDefinition(), pinotColumn.getOrigin()));
                        groupByColumns.add(outputColumn);
                        outputs.add(outputColumn);
                        groupByExists = true;
                        break;
                    }
                    case AGGREGATE: {
                        AggregationFunctionColumnNode aggregationNode = (AggregationFunctionColumnNode) expression;
                        String pinotAggFunction = handleAggregationFunction(aggregationNode.getCallExpression(), context.getSelections());
                        VariableReferenceExpression aggregationVarRef = getVariableReference(aggregationNode.getOutputColumn());
                        newSelections.put(aggregationVarRef, new Selection(pinotAggFunction, DERIVED));
                        outputs.add(aggregationVarRef);
                        aggregations++;
                        break;
                    }
                    default:
                        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "unknown aggregation expression: " + expression.getExpressionType());
                }
            }

            // Handling non-aggregated group by
            // E.g. `SELECT A, B FROM myTable GROUP BY A, B`
            // In Pql mode, the generated pql is `SELECT count(*) FROM myTable GROUP BY A, B`;
            // In Sql mode, the generated sql is still `SELECT A, B FROM myTable GROUP BY A, B`.
            if (!useSqlSyntax && groupByExists && aggregations == 0) {
                setHiddenField(newSelections, outputs, hiddenColumnSet);
                aggregations++;
            }
            return context.withAggregation(newSelections, outputs, groupByColumns, aggregations, hiddenColumnSet);
        }

        @Override
        public PinotQueryGeneratorContext visitLimit(LimitNode node, PinotQueryGeneratorContext context)
        {
            checkSupported(!node.isPartial(), String.format("pinot query generator cannot handle partial limit"));
            checkSupported(!forbidBrokerQueries, "Cannot push limit in segment mode");
            context = node.getSource().accept(this, context);
            requireNonNull(context, "context is null");
            return context.withLimit(node.getCount()).withOutputColumns(node.getOutputVariables());
        }

        @Override
        public PinotQueryGeneratorContext visitTopN(TopNNode node, PinotQueryGeneratorContext context)
        {
            context = node.getSource().accept(this, context);
            requireNonNull(context, "context is null");
            checkSupported(!forbidBrokerQueries, "Cannot push topn in segment mode");
            checkSupported(node.getStep().equals(TopNNode.Step.SINGLE), "Can only push single logical topn in");

            if (pushdownTopnBrokerQueries) {
                return context.withTopN(getOrderingScheme(node), node.getCount()).withOutputColumns(node.getOutputVariables());
            }
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "TopN query is not allowed to push down. Please refer to config: 'pinot.pushdown-topn-broker-queries'");
        }

        @Override
        public PinotQueryGeneratorContext visitDistinctLimit(DistinctLimitNode node, PinotQueryGeneratorContext context)
        {
            context = node.getSource().accept(this, context);
            requireNonNull(context, "context is null");
            checkSupported(!forbidBrokerQueries, "Cannot push distinctLimit in segment mode");
            LinkedHashSet<VariableReferenceExpression> groupByColumns = new LinkedHashSet<>(node.getDistinctVariables());
            if (!useSqlSyntax) {
                // Handling PQL by adding hidden expression: count(*)
                // E.g. `SELECT DISTINCT A, B FROM myTable LIMIT 10`
                // In Pql mode, the generated pql is `SELECT count(*) FROM myTable GROUP BY A, B LIMIT 10`.
                checkSupported(!context.hasAggregation(), "Aggregation already exists. Pinot doesn't support DistinctLimit with existing Aggregation");
                checkSupported(!context.hasGroupBy(), "GroupBy already exists. Pinot doesn't support DistinctLimit with existing GroupBy");
                Map<VariableReferenceExpression, Selection> newSelections = new HashMap<>(context.getSelections());
                LinkedHashSet<VariableReferenceExpression> newOutputs = new LinkedHashSet<>(groupByColumns);
                Set<VariableReferenceExpression> hiddenColumnSet = new HashSet<>();
                setHiddenField(newSelections, newOutputs, hiddenColumnSet);
                return context.withAggregation(newSelections, newOutputs, groupByColumns, 1, hiddenColumnSet).withLimit(node.getLimit());
            }
            // Handle SQL by setting the groupBy columns and output columns.
            // E.g. `SELECT DISTINCT A, B FROM myTable LIMIT 10`
            // In Sql mode, the generated sql is still `SELECT A, B FROM myTable GROUP BY A, B LIMIT 10`.
            return context.withDistinctLimit(groupByColumns, node.getLimit()).withOutputColumns(node.getOutputVariables());
        }

        private void setHiddenField(Map<VariableReferenceExpression, Selection> selections,
                LinkedHashSet<VariableReferenceExpression> outputs,
                Set<VariableReferenceExpression> hiddenColumnSet)
        {
            VariableReferenceExpression hidden = new VariableReferenceExpression(UUID.randomUUID().toString(), BigintType.BIGINT);
            selections.put(hidden, new Selection("count(*)", DERIVED));
            outputs.add(hidden);
            hiddenColumnSet.add(hidden);
        }
    }
}
