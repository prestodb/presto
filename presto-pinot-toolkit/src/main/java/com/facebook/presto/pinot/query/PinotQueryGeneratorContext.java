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

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.pinot.PinotColumnHandle;
import com.facebook.presto.pinot.PinotConfig;
import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.pinot.PinotQueryOptionsUtils;
import com.facebook.presto.pinot.PinotSessionProperties;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_QUERY_GENERATOR_FAILURE;
import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.pinot.PinotPushdownUtils.PINOT_DISTINCT_COUNT_FUNCTION_NAME;
import static com.facebook.presto.pinot.PinotPushdownUtils.checkSupported;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.StrictMath.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Encapsulates the components needed to construct a SQL query and provides methods to update the current context with new operations.
 */
public class PinotQueryGeneratorContext
{
    public static final String TIME_BOUNDARY_FILTER_TEMPLATE = "__TIME_BOUNDARY_FILTER_TEMPLATE__";
    public static final String TABLE_NAME_SUFFIX_TEMPLATE = "__TABLE_NAME_SUFFIX_TEMPLATE__";
    // Fields defining the query
    // A map that maps the column definition in terms of input relation column(s)
    private final Map<VariableReferenceExpression, Selection> selections;
    // Outputs are the fields defined in Presto for what's expected from Pinot query.
    // Outputs/groupByColumns/topNColumnOrderingMap should be part of the keys of selections.
    private final LinkedHashSet<VariableReferenceExpression> outputs;
    private final LinkedHashSet<VariableReferenceExpression> groupByColumns;
    private final LinkedHashMap<VariableReferenceExpression, OrderingColumnInformation> topNColumnInformationMap;
    private final Set<VariableReferenceExpression> hiddenColumnSet;
    private final Set<VariableReferenceExpression> variablesInAggregation;
    private final Optional<String> from;
    private final Optional<String> filter;
    private final OptionalInt limit;
    private final int aggregations;

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("selections", selections)
                .add("outputs", outputs)
                .add("groupByColumns", groupByColumns)
                .add("topNColumnInformationMap", topNColumnInformationMap)
                .add("hiddenColumnSet", hiddenColumnSet)
                .add("variablesInAggregation", variablesInAggregation)
                .add("from", from)
                .add("filter", filter)
                .add("limit", limit)
                .add("aggregations", aggregations)
                .toString();
    }

    public PinotQueryGeneratorContext()
    {
        this(new HashMap<>(), new LinkedHashSet<>(), null);
    }

    PinotQueryGeneratorContext(
            Map<VariableReferenceExpression, Selection> selections,
            LinkedHashSet<VariableReferenceExpression> outputs,
            String from)
    {
        this(
                selections,
                outputs,
                Optional.ofNullable(from),
                Optional.empty(),
                0,
                new LinkedHashSet<>(),
                new LinkedHashMap<>(),
                OptionalInt.empty(),
                new HashSet<>(),
                new HashSet<>());
    }

    private PinotQueryGeneratorContext(
            Map<VariableReferenceExpression, Selection> selections,
            LinkedHashSet<VariableReferenceExpression> outputs,
            Optional<String> from,
            Optional<String> filter,
            int aggregations,
            LinkedHashSet<VariableReferenceExpression> groupByColumns,
            LinkedHashMap<VariableReferenceExpression, OrderingColumnInformation> topNColumnInformationMap,
            OptionalInt limit,
            Set<VariableReferenceExpression> variablesInAggregation,
            Set<VariableReferenceExpression> hiddenColumnSet)
    {
        this.selections = new HashMap<>(requireNonNull(selections, "selections can't be null"));
        this.outputs = new LinkedHashSet<>(requireNonNull(outputs, "outputs can't be null."));
        this.from = requireNonNull(from, "source can't be null");
        this.aggregations = aggregations;
        this.groupByColumns = new LinkedHashSet<>(requireNonNull(groupByColumns, "groupByColumns can't be null. It could be empty if not available"));
        this.topNColumnInformationMap = new LinkedHashMap<>(requireNonNull(topNColumnInformationMap, "topNColumnInformationMap can't be null. It could be empty if not available"));
        this.filter = requireNonNull(filter, "filter is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.hiddenColumnSet = requireNonNull(hiddenColumnSet, "hidden column set is null");
        this.variablesInAggregation = requireNonNull(variablesInAggregation, "variables in aggregation is null");
    }

    /**
     * Apply the given filter to current context and return the updated context. Throws error for invalid operations.
     */
    public PinotQueryGeneratorContext withFilter(String filter)
    {
        checkSupported(!hasFilter(), "There already exists a filter. Pinot doesn't support filters at multiple levels");
        checkSupported(!hasAggregation(), "Pinot doesn't support filtering the results of aggregation");
        checkSupported(!hasLimit(), "Pinot doesn't support filtering on top of the limit");
        return new PinotQueryGeneratorContext(
                selections,
                outputs,
                from,
                Optional.of(filter),
                aggregations,
                groupByColumns,
                topNColumnInformationMap,
                limit,
                variablesInAggregation,
                hiddenColumnSet);
    }

    /**
     * Apply the aggregation to current context and return the updated context. Throws error for invalid operations.
     */
    public PinotQueryGeneratorContext withAggregation(
            Map<VariableReferenceExpression, Selection> newSelections,
            LinkedHashSet<VariableReferenceExpression> outputs,
            LinkedHashSet<VariableReferenceExpression> groupByColumns,
            int aggregations,
            Set<VariableReferenceExpression> hiddenColumnSet)
    {
        // there is only one aggregation supported unless distinct is used.
        AtomicBoolean pushDownDistinctCount = new AtomicBoolean(false);
        LinkedHashSet<VariableReferenceExpression> newOutputs = new LinkedHashSet<>(outputs);
        newSelections.values().forEach(selection -> {
            if (selection.getDefinition().startsWith(PINOT_DISTINCT_COUNT_FUNCTION_NAME.toUpperCase(Locale.ENGLISH))) {
                pushDownDistinctCount.set(true);
            }
        });
        if (pushDownDistinctCount.get()) {
            // Push down count distinct query to Pinot, clean up hidden column set by the non-aggregation groupBy Plan.
            hiddenColumnSet = ImmutableSet.of();
        }
        else {
            checkSupported(!hasAggregation(), "Pinot doesn't support aggregation on top of the aggregated data");
        }
        checkSupported(!hasLimit(), "Pinot doesn't support aggregation on top of the limit");

        return new PinotQueryGeneratorContext(newSelections, newOutputs, from, filter, aggregations, groupByColumns, topNColumnInformationMap, limit, variablesInAggregation, hiddenColumnSet);
    }

    /**
     * Apply new selections/project to current context and return the updated context. Throws error for invalid operations.
     */
    public PinotQueryGeneratorContext withProject(Map<VariableReferenceExpression, Selection> newSelections, LinkedHashSet<VariableReferenceExpression> newOutputs)
    {
        checkSupported(!newOutputs.isEmpty(), "Missing output expression in Pinot query context");
        return new PinotQueryGeneratorContext(
                newSelections,
                newOutputs,
                from,
                filter,
                aggregations,
                groupByColumns,
                topNColumnInformationMap,
                limit,
                variablesInAggregation,
                hiddenColumnSet);
    }

    private static int checkForValidLimit(long limit)
    {
        if (limit <= 0 || limit > Integer.MAX_VALUE) {
            throw new PinotException(PINOT_QUERY_GENERATOR_FAILURE, Optional.empty(), "Limit " + limit + " not supported: Limit is not being pushed down");
        }
        return toIntExact(limit);
    }

    /**
     * Apply limit to current context and return the updated context. Throws error for invalid operations.
     */
    public PinotQueryGeneratorContext withLimit(long limit)
    {
        int intLimit = checkForValidLimit(limit);
        checkSupported(!hasLimit(), "Limit already exists. Pinot doesn't support limit on top of another limit");
        return new PinotQueryGeneratorContext(
                selections,
                outputs,
                from,
                filter,
                aggregations,
                groupByColumns,
                topNColumnInformationMap,
                OptionalInt.of(intLimit),
                variablesInAggregation,
                hiddenColumnSet);
    }

    /**
     * Apply order by to current context and return the updated context. Throws error for invalid operations.
     */
    public PinotQueryGeneratorContext withTopN(LinkedHashMap<VariableReferenceExpression, SortOrder> orderByColumnOrderingMap, long limit)
    {
        checkSupported(!hasLimit(), "Limit already exists. Pinot doesn't support order by limit on top of another limit");
        int intLimit = checkForValidLimit(limit);

        LinkedHashMap<VariableReferenceExpression, OrderingColumnInformation> orderByColumnInformation = new LinkedHashMap<>();
        orderByColumnOrderingMap.entrySet().stream().forEach(entry -> orderByColumnInformation.put(entry.getKey(), new OrderingColumnInformation(entry.getValue(), selections.get(entry.getKey()))));

        return new PinotQueryGeneratorContext(
                selections,
                outputs,
                from,
                filter,
                aggregations,
                groupByColumns,
                orderByColumnInformation,
                OptionalInt.of(intLimit),
                variablesInAggregation,
                hiddenColumnSet);
    }

    private boolean hasFilter()
    {
        return filter.isPresent();
    }

    private boolean hasLimit()
    {
        return limit.isPresent();
    }

    public boolean hasGroupBy()
    {
        return !groupByColumns.isEmpty();
    }

    public boolean hasAggregation()
    {
        return aggregations > 0;
    }

    private boolean hasOrderBy()
    {
        return !topNColumnInformationMap.isEmpty();
    }

    public Map<VariableReferenceExpression, Selection> getSelections()
    {
        return selections;
    }

    public Set<VariableReferenceExpression> getHiddenColumnSet()
    {
        return hiddenColumnSet;
    }

    Set<VariableReferenceExpression> getVariablesInAggregation()
    {
        return variablesInAggregation;
    }

    public PinotQueryGenerator.GeneratedPinotQuery toQuery(PinotConfig pinotConfig, ConnectorSession session)
    {
        return toSqlQuery(pinotConfig, session);
    }

    // Generate Pinot query:
    // - takes arguments of expressions/table name/limit clause;
    // - handles the common logic to generate where/groupBy/orderBy clauses.
    private String generatePinotQueryHelper(boolean forBroker, String expressions, String tableName, String limitClause, String queryOptions)
    {
        String query = "SELECT " + expressions + " FROM " + tableName + (forBroker ? "" : TABLE_NAME_SUFFIX_TEMPLATE);
        if (filter.isPresent()) {
            String filterString = filter.get();
            // this is hack!!!. Ideally we want to clone the scan pipeline and create/update the filter in the scan pipeline to contain this filter and
            // at the same time add the time column to scan so that the query generator doesn't fail when it looks up the time column in scan output columns
            query += format(" WHERE %s%s", filterString, forBroker ? "" : TIME_BOUNDARY_FILTER_TEMPLATE);
        }
        else if (!forBroker) {
            query += TIME_BOUNDARY_FILTER_TEMPLATE;
        }

        if (hasGroupBy()) {
            String groupByExpr = groupByColumns.stream().map(x -> selections.get(x).getDefinition()).collect(Collectors.joining(", "));
            query = query + " GROUP BY " + groupByExpr;
        }

        if (hasOrderBy()) {
            String orderByExpressions = topNColumnInformationMap.entrySet().stream().map(entry -> (entry.getValue().getSelection().getDefinition()) + (entry.getValue().getSortOrder().isAscending() ? "" : " DESC")).collect(Collectors.joining(", "));
            query = query + " ORDER BY " + orderByExpressions;
        }
        query = query + limitClause;
        query = query + queryOptions;
        return query;
    }

    /**
     * Convert the current context to a Pinot SQL
     */
    public PinotQueryGenerator.GeneratedPinotQuery toSqlQuery(PinotConfig pinotConfig, ConnectorSession session)
    {
        int nonAggregateShortQueryLimit = PinotSessionProperties.getNonAggregateLimitForBrokerQueries(session);
        boolean isQueryShort = (hasAggregation() || hasGroupBy()) || limit.orElse(Integer.MAX_VALUE) < nonAggregateShortQueryLimit;
        boolean attemptBrokerQueries = PinotSessionProperties.isAttemptBrokerQueries(session) || isQueryShort;
        boolean forBroker = !PinotSessionProperties.isForbidBrokerQueries(session) && attemptBrokerQueries;
        String groupByExpressions = groupByColumns.stream()
                .map(x -> selections.get(x).getDefinition())
                .collect(Collectors.joining(", "));

        checkSupported(!outputs.isEmpty(), "Unable to generate Pinot query without output expression");
        String selectExpressions = outputs.stream()
                .filter(o -> !groupByColumns.contains(o))
                .map(o -> updateSelection(selections.get(o).getDefinition(), session))
                .collect(Collectors.joining(", "));
        String expressions = (groupByExpressions.isEmpty()) ?
                selectExpressions :
                (selectExpressions.isEmpty()) ?
                        groupByExpressions :
                        groupByExpressions + ", " + selectExpressions;

        String tableName = from.orElseThrow(() -> new PinotException(PINOT_QUERY_GENERATOR_FAILURE, Optional.empty(), "Table name not encountered yet"));

        // Rules for limit:
        // - If its a selection query:
        //      + given limit or configured limit
        // - Else if has group by:
        //      + default limit or configured top limit
        // - Aggregation only query limit is ignored.
        // - Fail if limit is invalid
        int queryLimit = -1;
        if (!hasAggregation() && !hasGroupBy()) {
            if (!limit.isPresent() && forBroker && !attemptBrokerQueries) {
                throw new PinotException(PINOT_QUERY_GENERATOR_FAILURE, Optional.empty(), "Broker non aggregate queries have to have a limit");
            }
            else {
                queryLimit = limit.orElseGet(() -> PinotSessionProperties.getLimitLargerForSegment(session));
            }
        }
        else if (hasGroupBy()) {
            if (limit.isPresent()) {
                queryLimit = limit.getAsInt();
            }
            else {
                queryLimit = PinotSessionProperties.getTopNLarge(session);
            }
        }
        String limitClause = "";
        if (queryLimit > 0) {
            limitClause = " LIMIT " + queryLimit;
        }
        String queryOptionsProperty = PinotSessionProperties.getQueryOptions(session);
        String queryOptions = PinotQueryOptionsUtils.getQueryOptionsAsString(queryOptionsProperty);
        String query = generatePinotQueryHelper(forBroker, expressions, tableName, limitClause, queryOptions);
        LinkedHashMap<VariableReferenceExpression, PinotColumnHandle> assignments = getAssignments();
        List<Integer> indices = getIndicesMappingFromPinotSchemaToPrestoSchema(query, assignments);
        return new PinotQueryGenerator.GeneratedPinotQuery(tableName, query, indices, filter.isPresent(), forBroker);
    }

    private String updateSelection(String definition, ConnectorSession session)
    {
        final String overrideDistinctCountFunction = PinotSessionProperties.getOverrideDistinctCountFunction(session);
        if (!PINOT_DISTINCT_COUNT_FUNCTION_NAME.equalsIgnoreCase(overrideDistinctCountFunction)) {
            return definition.replaceFirst(PINOT_DISTINCT_COUNT_FUNCTION_NAME.toUpperCase() + "\\(", overrideDistinctCountFunction.toUpperCase() + "\\(");
        }
        return definition;
    }

    private List<Integer> getIndicesMappingFromPinotSchemaToPrestoSchema(String query, Map<VariableReferenceExpression, PinotColumnHandle> assignments)
    {
        LinkedHashMap<VariableReferenceExpression, Selection> expressionsInPinotOrder = new LinkedHashMap<>();
        for (VariableReferenceExpression groupByColumn : groupByColumns) {
            Selection groupByColumnDefinition = selections.get(groupByColumn);
            if (groupByColumnDefinition == null) {
                throw new IllegalStateException(format(
                        "Group By column (%s) definition not found in input selections: %s",
                        groupByColumn,
                        Joiner.on(",").withKeyValueSeparator(":").join(selections)));
            }
            expressionsInPinotOrder.put(groupByColumn, groupByColumnDefinition);
        }
        for (VariableReferenceExpression outputColumn : outputs) {
            Selection outputColumnDefinition = selections.get(outputColumn);
            if (outputColumnDefinition == null) {
                throw new IllegalStateException(format(
                        "Output column (%s) definition not found in input selections: %s",
                        outputColumn,
                        Joiner.on(",").withKeyValueSeparator(":").join(selections)));
            }
            expressionsInPinotOrder.put(outputColumn, outputColumnDefinition);
        }

        checkSupported(
                assignments.size() <= expressionsInPinotOrder.keySet().stream().filter(key -> !hiddenColumnSet.contains(key)).count(),
                "Expected returned expressions %s is a superset of selections %s",
                Joiner.on(",").withKeyValueSeparator(":").join(expressionsInPinotOrder),
                Joiner.on(",").withKeyValueSeparator("=").join(assignments));

        Map<VariableReferenceExpression, Integer> assignmentToIndex = new HashMap<>();
        Iterator<Map.Entry<VariableReferenceExpression, PinotColumnHandle>> assignmentsIterator = assignments.entrySet().iterator();
        for (int i = 0; i < assignments.size(); i++) {
            VariableReferenceExpression key = assignmentsIterator.next().getKey();
            Integer previous = assignmentToIndex.put(key, i);
            if (previous != null) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.of(query), format("Expected Pinot column handle %s to occur only once, but we have: %s", key, Joiner.on(",").withKeyValueSeparator("=").join(assignments)));
            }
        }

        ImmutableList.Builder<Integer> outputIndices = ImmutableList.builder();
        for (Map.Entry<VariableReferenceExpression, Selection> expression : expressionsInPinotOrder.entrySet()) {
            Integer index;
            if (hiddenColumnSet.contains(expression.getKey())) {
                index = -1; // negative output index means to skip this value returned by pinot at query time
            }
            else {
                index = assignmentToIndex.getOrDefault(expression.getKey(), -1); // negative output index means to skip this value returned by pinot at query time
            }
            outputIndices.add(index);
        }
        return outputIndices.build();
    }

    public LinkedHashMap<VariableReferenceExpression, PinotColumnHandle> getAssignments()
    {
        LinkedHashMap<VariableReferenceExpression, PinotColumnHandle> result = new LinkedHashMap<>();
        LinkedHashSet<VariableReferenceExpression> outputFields = new LinkedHashSet<>();
        outputFields.addAll(outputs.stream().filter(variable -> !hiddenColumnSet.contains(variable)).collect(Collectors.toList()));
        outputFields.stream().forEach(variable -> {
            Selection selection = selections.get(variable);
            PinotColumnHandle handle = selection.getOrigin() == Origin.TABLE_COLUMN ? new PinotColumnHandle(selection.getDefinition(), variable.getType(), PinotColumnHandle.PinotColumnType.REGULAR) : new PinotColumnHandle(variable, PinotColumnHandle.PinotColumnType.DERIVED);
            result.put(variable, handle);
        });
        return result;
    }

    public PinotQueryGeneratorContext withOutputColumns(List<VariableReferenceExpression> outputColumns)
    {
        LinkedHashSet<VariableReferenceExpression> newOutputs = new LinkedHashSet<>(outputColumns);
        outputColumns.forEach(o -> requireNonNull(selections.get(o), String.format("Cannot find the selection %s in the original context %s", o, this)));
        // Hidden columns flow as is from the previous
        selections.entrySet().stream().filter(e -> hiddenColumnSet.contains(e.getKey())).forEach(e -> newOutputs.add(e.getKey()));
        return new PinotQueryGeneratorContext(selections, newOutputs, from, filter, aggregations, groupByColumns, topNColumnInformationMap, limit, variablesInAggregation, hiddenColumnSet);
    }

    public PinotQueryGeneratorContext withVariablesInAggregation(Set<VariableReferenceExpression> newVariablesInAggregation)
    {
        return new PinotQueryGeneratorContext(
                selections,
                outputs,
                from,
                filter,
                aggregations,
                groupByColumns,
                topNColumnInformationMap,
                limit,
                newVariablesInAggregation,
                hiddenColumnSet);
    }

    public PinotQueryGeneratorContext withDistinctLimit(LinkedHashSet<VariableReferenceExpression> newGroupByColumns, long limit)
    {
        int intLimit = checkForValidLimit(limit);
        checkSupported(!hasLimit(), "Limit already exists. Pinot doesn't support limit on top of another limit");
        checkSupported(!hasGroupBy(), "GroupBy already exists. Pinot doesn't support Distinct on top of another Group By");
        checkSupported(!hasAggregation(), "Aggregation already exists. Pinot doesn't support Distinct Limit on top of Aggregation");
        return new PinotQueryGeneratorContext(
                selections,
                outputs,
                from,
                filter,
                aggregations,
                newGroupByColumns,
                topNColumnInformationMap,
                OptionalInt.of(intLimit),
                variablesInAggregation,
                hiddenColumnSet);
    }

    /**
     * Where is the selection/projection originated from
     */
    public enum Origin
    {
        TABLE_COLUMN, // refers to direct column in table
        DERIVED, // expression is derived from one or more input columns or a combination of input columns and literals
        LITERAL, // derived from literal
    }

    // Projected/selected column definition in query
    public static class Selection
    {
        private final String definition;
        private final Origin origin;

        public Selection(String definition, Origin origin)
        {
            this.origin = origin;
            this.definition = (origin == Origin.TABLE_COLUMN) ? (definition.startsWith("\"") ? definition : String.format("\"%s\"", definition)) : definition;
        }

        public String getDefinition()
        {
            return definition;
        }

        public Origin getOrigin()
        {
            return origin;
        }

        @Override
        public String toString()
        {
            return definition;
        }
    }

    // Columns definitions and ordering information for OrderBy Columns
    private static class OrderingColumnInformation
    {
        private final SortOrder sortOrder;
        private final Selection selection;

        public OrderingColumnInformation(SortOrder sortOrder, Selection selection)
        {
            this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
            this.selection = requireNonNull(selection, "selection is null");
        }

        public SortOrder getSortOrder()
        {
            return sortOrder;
        }

        public Selection getSelection()
        {
            return selection;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("sortOrder", sortOrder.isAscending() ? "ASC" : "DESC")
                    .add("selection", selection)
                    .toString();
        }
    }
}
