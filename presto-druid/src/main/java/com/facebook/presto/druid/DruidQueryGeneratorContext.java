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
package com.facebook.presto.druid;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.druid.DruidErrorCode.DRUID_QUERY_GENERATOR_FAILURE;
import static com.facebook.presto.druid.DruidPushdownUtils.DRUID_COUNT_DISTINCT_FUNCTION_NAME;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DruidQueryGeneratorContext
{
    private final Map<VariableReferenceExpression, Selection> selections;
    private final Map<VariableReferenceExpression, Selection> groupByColumns;
    private final Set<VariableReferenceExpression> hiddenColumnSet;
    private final Set<VariableReferenceExpression> variablesInAggregation;
    private final Optional<String> from;
    private final Optional<String> filter;
    private final OptionalLong limit;
    private final int aggregations;
    private final Optional<PlanNodeId> tableScanNodeId;

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("selections", selections)
                .add("groupByColumns", groupByColumns)
                .add("hiddenColumnSet", hiddenColumnSet)
                .add("variablesInAggregation", variablesInAggregation)
                .add("from", from)
                .add("filter", filter)
                .add("limit", limit)
                .add("aggregations", aggregations)
                .add("tableScanNodeId", tableScanNodeId)
                .toString();
    }

    DruidQueryGeneratorContext()
    {
        this(new LinkedHashMap<>(), null, null);
    }

    DruidQueryGeneratorContext(
            Map<VariableReferenceExpression, Selection> selections,
            String from,
            PlanNodeId planNodeId)
    {
        this(
                selections,
                Optional.ofNullable(from),
                Optional.empty(),
                OptionalLong.empty(),
                0,
                new LinkedHashMap<>(),
                new HashSet<>(),
                new HashSet<>(),
                Optional.ofNullable(planNodeId));
    }

    private DruidQueryGeneratorContext(
            Map<VariableReferenceExpression, Selection> selections,
            Optional<String> from,
            Optional<String> filter,
            OptionalLong limit,
            int aggregations,
            Map<VariableReferenceExpression, Selection> groupByColumns,
            Set<VariableReferenceExpression> variablesInAggregation,
            Set<VariableReferenceExpression> hiddenColumnSet,
            Optional<PlanNodeId> tableScanNodeId)
    {
        this.selections = new LinkedHashMap<>(requireNonNull(selections, "selections can't be null"));
        this.from = requireNonNull(from, "source can't be null");
        this.filter = requireNonNull(filter, "filter is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.aggregations = aggregations;
        this.groupByColumns = new LinkedHashMap<>(requireNonNull(groupByColumns, "groupByColumns can't be null. It could be empty if not available"));
        this.hiddenColumnSet = requireNonNull(hiddenColumnSet, "hidden column set is null");
        this.variablesInAggregation = requireNonNull(variablesInAggregation, "variables in aggregation is null");
        this.tableScanNodeId = requireNonNull(tableScanNodeId, "tableScanNodeId can't be null");
    }

    public DruidQueryGeneratorContext withFilter(String filter)
    {
        if (hasAggregation()) {
            throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Druid does not support filter on top of AggregationNode.");
        }
        checkState(!hasFilter(), "Druid doesn't support filters at multiple levels under AggregationNode");
        return new DruidQueryGeneratorContext(
                selections,
                from,
                Optional.of(filter),
                limit,
                aggregations,
                groupByColumns,
                variablesInAggregation,
                hiddenColumnSet,
                tableScanNodeId);
    }

    public DruidQueryGeneratorContext withProject(Map<VariableReferenceExpression, Selection> newSelections)
    {
        return new DruidQueryGeneratorContext(
                newSelections,
                from,
                filter,
                limit,
                aggregations,
                groupByColumns,
                variablesInAggregation,
                hiddenColumnSet,
                tableScanNodeId);
    }

    public DruidQueryGeneratorContext withLimit(long limit)
    {
        if (limit <= 0 || limit > Long.MAX_VALUE) {
            throw new PrestoException(DRUID_QUERY_GENERATOR_FAILURE, "Invalid limit: " + limit);
        }
        checkState(!hasLimit(), "Limit already exists. Druid doesn't support limit on top of another limit");
        return new DruidQueryGeneratorContext(
                selections,
                from,
                filter,
                OptionalLong.of(limit),
                aggregations,
                groupByColumns,
                variablesInAggregation,
                hiddenColumnSet,
                tableScanNodeId);
    }

    public DruidQueryGeneratorContext withAggregation(
            Map<VariableReferenceExpression, Selection> newSelections,
            Map<VariableReferenceExpression, Selection> newGroupByColumns,
            int newAggregations,
            Set<VariableReferenceExpression> newHiddenColumnSet)
    {
        AtomicBoolean pushDownDistinctCount = new AtomicBoolean(false);
        newSelections.values().forEach(selection -> {
            if (selection.getDefinition().startsWith(DRUID_COUNT_DISTINCT_FUNCTION_NAME.toUpperCase(Locale.ENGLISH))) {
                pushDownDistinctCount.set(true);
            }
        });
        Map<VariableReferenceExpression, Selection> targetSelections = newSelections;
        if (pushDownDistinctCount.get()) {
            // Push down count distinct query to Druid, clean up hidden column set by the non-aggregation groupBy Plan.
            newHiddenColumnSet = ImmutableSet.of();
            ImmutableMap.Builder<VariableReferenceExpression, Selection> builder = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, Selection> entry : newSelections.entrySet()) {
                if (entry.getValue().getDefinition().startsWith(DRUID_COUNT_DISTINCT_FUNCTION_NAME.toUpperCase(Locale.ENGLISH))) {
                    String definition = entry.getValue().getDefinition();
                    int start = definition.indexOf("(");
                    int end = definition.indexOf(")");
                    String countDistinctClause = "count ( distinct " + escapeSqlIdentifier(definition.substring(start + 1, end)) + ")";
                    Selection countDistinctSelection = new Selection(countDistinctClause, entry.getValue().getOrigin());
                    builder.put(entry.getKey(), countDistinctSelection);
                }
                else {
                    builder.put(entry.getKey(), entry.getValue());
                }
            }
            targetSelections = builder.build();
        }
        else {
            checkState(!hasAggregation(), "Druid doesn't support aggregation on top of the aggregated data");
        }
        checkState(!hasLimit(), "Druid doesn't support aggregation on top of the limit");
        checkState(newAggregations > 0, "Invalid number of aggregations");
        return new DruidQueryGeneratorContext(
                targetSelections,
                from,
                filter,
                limit,
                newAggregations,
                newGroupByColumns,
                variablesInAggregation,
                newHiddenColumnSet,
                tableScanNodeId);
    }

    private static String escapeSqlIdentifier(String identifier)
    {
        return "\"" + identifier + "\"";
    }

    public DruidQueryGeneratorContext withVariablesInAggregation(Set<VariableReferenceExpression> newVariablesInAggregation)
    {
        return new DruidQueryGeneratorContext(
                selections,
                from,
                filter,
                limit,
                aggregations,
                groupByColumns,
                newVariablesInAggregation,
                hiddenColumnSet,
                tableScanNodeId);
    }

    private boolean hasLimit()
    {
        return limit.isPresent();
    }

    private boolean hasFilter()
    {
        return filter.isPresent();
    }

    private boolean hasAggregation()
    {
        return aggregations > 0;
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

    public Optional<PlanNodeId> getTableScanNodeId()
    {
        return tableScanNodeId;
    }

    public DruidQueryGenerator.GeneratedDql toQuery()
    {
        if (hasLimit() && aggregations > 1 && !groupByColumns.isEmpty()) {
            throw new PrestoException(DRUID_QUERY_GENERATOR_FAILURE, "Could not pushdown multiple aggregates in the presence of group by and limit");
        }

        String expressions = selections.entrySet().stream()
                .map(s -> s.getValue().getEscapedDefinition())
                .collect(Collectors.joining(", "));
        if (expressions.isEmpty()) {
            throw new PrestoException(DRUID_QUERY_GENERATOR_FAILURE, "Empty Druid query");
        }

        String tableName = from.orElseThrow(() -> new PrestoException(DRUID_QUERY_GENERATOR_FAILURE, "Table name missing in Druid query"));
        String query = "SELECT " + expressions + " FROM " + escapeSqlIdentifier(tableName);
        boolean pushdown = false;
        if (filter.isPresent()) {
            // this is hack!!!. Ideally we want to clone the scan pipeline and create/update the filter in the scan pipeline to contain this filter and
            // at the same time add the time column to scan so that the query generator doesn't fail when it looks up the time column in scan output columns
            query += " WHERE " + filter.get();
            pushdown = true;
        }

        if (!groupByColumns.isEmpty()) {
            String groupByExpression = groupByColumns.entrySet().stream().map(v -> v.getValue().getEscapedDefinition()).collect(Collectors.joining(", "));
            query = query + " GROUP BY " + groupByExpression;
            pushdown = true;
        }

        if (hasAggregation()) {
            pushdown = true;
        }

        if (limit.isPresent()) {
            query += " LIMIT " + limit.getAsLong();
            pushdown = true;
        }
        return new DruidQueryGenerator.GeneratedDql(tableName, query, pushdown);
    }

    public Map<VariableReferenceExpression, DruidColumnHandle> getAssignments()
    {
        Map<VariableReferenceExpression, DruidColumnHandle> result = new LinkedHashMap<>();
        selections.entrySet().stream().filter(e -> !hiddenColumnSet.contains(e.getKey())).forEach(entry -> {
            VariableReferenceExpression variable = entry.getKey();
            Selection selection = entry.getValue();
            DruidColumnHandle handle = selection.getOrigin() == Origin.TABLE_COLUMN ?
                    new DruidColumnHandle(selection.getDefinition(), variable.getType(), DruidColumnHandle.DruidColumnType.REGULAR) :
                    new DruidColumnHandle(variable, DruidColumnHandle.DruidColumnType.DERIVED);
            result.put(variable, handle);
        });
        return result;
    }

    public DruidQueryGeneratorContext withOutputColumns(List<VariableReferenceExpression> outputColumns)
    {
        Map<VariableReferenceExpression, Selection> newSelections = new LinkedHashMap<>();
        outputColumns.forEach(o -> newSelections.put(o, requireNonNull(selections.get(o), "Cannot find the selection " + o + " in the original context " + this)));
        selections.entrySet().stream().filter(e -> hiddenColumnSet.contains(e.getKey())).forEach(e -> newSelections.put(e.getKey(), e.getValue()));

        return new DruidQueryGeneratorContext(
                newSelections,
                from,
                filter,
                limit,
                aggregations,
                groupByColumns,
                variablesInAggregation,
                hiddenColumnSet,
                tableScanNodeId);
    }

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
            this.definition = definition;
            this.origin = origin;
        }

        public String getDefinition()
        {
            return definition;
        }

        public String getEscapedDefinition()
        {
            if (origin == Origin.TABLE_COLUMN) {
                return escapeSqlIdentifier(definition);
            }
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
}
