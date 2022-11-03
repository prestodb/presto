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

import com.facebook.presto.plugin.clickhouse.ClickHouseColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.facebook.presto.plugin.clickhouse.ClickHouseErrorCode.CLICKHOUSE_PUSHDOWN_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.plugin.clickhouse.ClickHouseErrorCode.CLICKHOUSE_QUERY_GENERATOR_FAILURE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ClickHouseQueryGeneratorContext
{
    private final Map<VariableReferenceExpression, Selection> selections;
    private final Map<VariableReferenceExpression, Selection> groupByColumns;
    private final Set<VariableReferenceExpression> hiddenColumnSet;
    private final Set<VariableReferenceExpression> variablesInAggregation;
    private final Optional<String> from;
    private final String schema;
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
                .add("schema", schema)
                .add("filter", filter)
                .add("limit", limit)
                .add("aggregations", aggregations)
                .add("tableScanNodeId", tableScanNodeId)
                .toString();
    }

    ClickHouseQueryGeneratorContext()
    {
        this(new LinkedHashMap<>(), null, "default", null);
    }

    ClickHouseQueryGeneratorContext(
            Map<VariableReferenceExpression, Selection> selections,
            String from,
            String schema,
            PlanNodeId planNodeId)
    {
        this(
                selections,
                Optional.ofNullable(from),
                schema,
                Optional.empty(),
                OptionalLong.empty(),
                0,
                new LinkedHashMap<>(),
                new HashSet<>(),
                new HashSet<>(),
                Optional.ofNullable(planNodeId));
    }

    private ClickHouseQueryGeneratorContext(
            Map<VariableReferenceExpression, Selection> selections,
            Optional<String> from,
            String schema,
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
        this.schema = requireNonNull(schema, "source can't be null");
        this.filter = requireNonNull(filter, "filter is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.aggregations = aggregations;
        this.groupByColumns = new LinkedHashMap<>(requireNonNull(groupByColumns, "groupByColumns can't be null. It could be empty if not available"));
        this.hiddenColumnSet = requireNonNull(hiddenColumnSet, "hidden column set is null");
        this.variablesInAggregation = requireNonNull(variablesInAggregation, "variables in aggregation is null");
        this.tableScanNodeId = requireNonNull(tableScanNodeId, "tableScanNodeId can't be null");
    }

    public ClickHouseQueryGeneratorContext withFilter(String filter)
    {
        if (hasAggregation()) {
            throw new PrestoException(CLICKHOUSE_PUSHDOWN_UNSUPPORTED_EXPRESSION, "ClickHouse does not support filter on top of AggregationNode.");
        }
        checkState(!hasFilter(), "ClickHouse doesn't support filters at multiple levels under AggregationNode");
        return new ClickHouseQueryGeneratorContext(
                selections,
                from,
                schema,
                Optional.of(filter),
                limit,
                aggregations,
                groupByColumns,
                variablesInAggregation,
                hiddenColumnSet,
                tableScanNodeId);
    }

    public ClickHouseQueryGeneratorContext withProject(Map<VariableReferenceExpression, Selection> newSelections)
    {
        return new ClickHouseQueryGeneratorContext(
                newSelections,
                from,
                schema,
                filter,
                limit,
                aggregations,
                groupByColumns,
                variablesInAggregation,
                hiddenColumnSet,
                tableScanNodeId);
    }

    public ClickHouseQueryGeneratorContext withLimit(long limit)
    {
        if (limit <= 0 || limit > Long.MAX_VALUE) {
            throw new PrestoException(CLICKHOUSE_QUERY_GENERATOR_FAILURE, "Invalid limit: " + limit);
        }
        checkState(!hasLimit(), "Limit already exists. ClickHouse doesn't support limit on top of another limit");
        return new ClickHouseQueryGeneratorContext(
                selections,
                from,
                schema,
                filter,
                OptionalLong.of(limit),
                aggregations,
                groupByColumns,
                variablesInAggregation,
                hiddenColumnSet,
                tableScanNodeId);
    }

    public ClickHouseQueryGeneratorContext withAggregation(
            Map<VariableReferenceExpression, Selection> newSelections,
            Map<VariableReferenceExpression, Selection> newGroupByColumns,
            int newAggregations,
            Set<VariableReferenceExpression> newHiddenColumnSet)
    {
        AtomicBoolean pushDownDistinctCount = new AtomicBoolean(false);

        Map<VariableReferenceExpression, Selection> targetSelections = newSelections;
        if (pushDownDistinctCount.get()) {
            // Push down count distinct query to ClickHouse, clean up hidden column set by the non-aggregation groupBy Plan.
            newHiddenColumnSet = ImmutableSet.of();
            ImmutableMap.Builder<VariableReferenceExpression, Selection> builder = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, Selection> entry : newSelections.entrySet()) {
                builder.put(entry.getKey(), entry.getValue());
            }
            targetSelections = builder.build();
        }
        else {
            checkState(!hasAggregation(), "ClickHouse doesn't support aggregation on top of the aggregated data");
        }
        checkState(!hasLimit(), "ClickHouse doesn't support aggregation on top of the limit");
        checkState(newAggregations > 0, "Invalid number of aggregations");
        return new ClickHouseQueryGeneratorContext(
                targetSelections,
                from,
                schema,
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

    public ClickHouseQueryGeneratorContext withVariablesInAggregation(Set<VariableReferenceExpression> newVariablesInAggregation)
    {
        return new ClickHouseQueryGeneratorContext(
                selections,
                from,
                schema,
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

    public ClickHouseQueryGenerator.GeneratedCkql toQuery()
    {
        if (hasLimit() && aggregations > 1 && !groupByColumns.isEmpty()) {
            throw new PrestoException(CLICKHOUSE_QUERY_GENERATOR_FAILURE, "Could not pushdown multiple aggregates in the presence of group by and limit");
        }

        String expressions = selections.entrySet().stream()
                .map(s -> s.getValue().getEscapedDefinition())
                .collect(Collectors.joining(", "));
        if (expressions.isEmpty()) {
            throw new PrestoException(CLICKHOUSE_QUERY_GENERATOR_FAILURE, "Empty ClickHouse query");
        }

        String tableName = from.orElseThrow(() -> new PrestoException(CLICKHOUSE_QUERY_GENERATOR_FAILURE, "Table name missing in ClickHouse query"));
        String query = "SELECT " + expressions + " FROM " + schema + "." + escapeSqlIdentifier(tableName);
        boolean pushdown = false;
        if (filter.isPresent()) {
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
        return new ClickHouseQueryGenerator.GeneratedCkql(tableName, query, pushdown);
    }

    public Map<VariableReferenceExpression, ClickHouseColumnHandle> getAssignments()
    {
        Map<VariableReferenceExpression, ClickHouseColumnHandle> result = new LinkedHashMap<>();

        selections.entrySet().stream().filter(e -> !hiddenColumnSet.contains(e.getKey())).forEach(entry -> {
            VariableReferenceExpression variable = entry.getKey();
//            if (variable.getName().substring(0, 3).equals("avg")) {
//                variable = new VariableReferenceExpression(variable.getSourceLocation(), variable.getName(), DoubleType.DOUBLE);
//            }
            Selection selection = entry.getValue();

            ClickHouseColumnHandle handle = selection.getOrigin() == Origin.TABLE_COLUMN ?
                    new ClickHouseColumnHandle(selection.getDefinition(), variable.getType(), ClickHouseColumnHandle.ClickHouseColumnType.REGULAR) :
                    new ClickHouseColumnHandle(variable, ClickHouseColumnHandle.ClickHouseColumnType.DERIVED);
            result.put(variable, handle);
        });
        return result;
//        LinkedHashMap<VariableReferenceExpression, ClickHouseColumnHandle> result = new LinkedHashMap<>();
//        LinkedHashSet<VariableReferenceExpression> outputFields = new LinkedHashSet<>();
//        Set<VariableReferenceExpression> outputs = selections.keySet();
//        outputFields.addAll(outputs.stream().filter(variable -> !hiddenColumnSet.contains(variable)).collect(Collectors.toList()));
//        outputFields.stream().forEach(variable -> {
//            Selection selection = selections.get(variable);
//            ClickHouseColumnHandle handle = selection.getOrigin() == Origin.TABLE_COLUMN ? new ClickHouseColumnHandle(selection.getDefinition(), variable.getType(), ClickHouseColumnHandle.ClickHouseColumnType.REGULAR) : new ClickHouseColumnHandle(variable, ClickHouseColumnHandle.ClickHouseColumnType.DERIVED);
//            result.put(variable, handle);
//        });
//        return result;
    }

    public ClickHouseQueryGeneratorContext withOutputColumns(List<VariableReferenceExpression> outputColumns)
    {
        Map<VariableReferenceExpression, Selection> newSelections = new LinkedHashMap<>();
        outputColumns.forEach(o -> newSelections.put(o, requireNonNull(selections.get(o), "Cannot find the selection " + o + " in the original context " + this)));
        selections.entrySet().stream().filter(e -> hiddenColumnSet.contains(e.getKey())).forEach(e -> newSelections.put(e.getKey(), e.getValue()));

        return new ClickHouseQueryGeneratorContext(
                newSelections,
                from,
                schema,
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
