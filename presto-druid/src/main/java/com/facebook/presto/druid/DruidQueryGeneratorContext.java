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
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_QUERY_GENERATOR_FAILURE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DruidQueryGeneratorContext
{
    private final Map<VariableReferenceExpression, Selection> selections;
    private final Optional<String> from;
    private final Optional<String> filter;
    private final OptionalLong limit;

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("selections", selections)
                .add("from", from)
                .add("filter", filter)
                .add("limit", limit)
                .toString();
    }

    DruidQueryGeneratorContext()
    {
        this(new LinkedHashMap<>(), null);
    }

    DruidQueryGeneratorContext(
            Map<VariableReferenceExpression, Selection> selections,
            String from)
    {
        this(
                selections,
                Optional.ofNullable(from),
                Optional.empty(),
                OptionalLong.empty());
    }

    private DruidQueryGeneratorContext(
            Map<VariableReferenceExpression, Selection> selections,
            Optional<String> from,
            Optional<String> filter,
            OptionalLong limit)
    {
        this.selections = new LinkedHashMap<>(requireNonNull(selections, "selections can't be null"));
        this.from = requireNonNull(from, "source can't be null");
        this.filter = requireNonNull(filter, "filter is null");
        this.limit = requireNonNull(limit, "limit is null");
    }

    public DruidQueryGeneratorContext withFilter(String filter)
    {
        checkState(!hasFilter(), "Druid doesn't support filters at multiple levels");
        return new DruidQueryGeneratorContext(
                selections,
                from,
                Optional.of(filter),
                limit);
    }

    public DruidQueryGeneratorContext withProject(Map<VariableReferenceExpression, Selection> newSelections)
    {
        return new DruidQueryGeneratorContext(
                newSelections,
                from,
                filter,
                limit);
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
                OptionalLong.of(limit));
    }

    private boolean hasLimit()
    {
        return limit.isPresent();
    }

    private boolean hasFilter()
    {
        return filter.isPresent();
    }

    public Map<VariableReferenceExpression, Selection> getSelections()
    {
        return selections;
    }

    public DruidQueryGenerator.GeneratedDql toQuery()
    {
        String expressions = selections.entrySet().stream()
                .map(s -> s.getValue().getDefinition())
                .collect(Collectors.joining(", "));
        if (expressions.isEmpty()) {
            throw new PrestoException(DRUID_QUERY_GENERATOR_FAILURE, "Empty Druid query");
        }

        String tableName = from.orElseThrow(() -> new PrestoException(DRUID_QUERY_GENERATOR_FAILURE, "Table name missing in Druid query"));
        String query = "SELECT " + expressions + " FROM " + tableName;
        boolean pushdown = false;
        if (filter.isPresent()) {
            // this is hack!!!. Ideally we want to clone the scan pipeline and create/update the filter in the scan pipeline to contain this filter and
            // at the same time add the time column to scan so that the query generator doesn't fail when it looks up the time column in scan output columns
            query += " WHERE " + filter.get();
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
        selections.entrySet().forEach(entry -> {
            VariableReferenceExpression variable = entry.getKey();
            Selection selection = entry.getValue();
            DruidColumnHandle handle = selection.getOrigin() == Origin.TABLE_COLUMN ? new DruidColumnHandle(selection.getDefinition(), variable.getType(), DruidColumnHandle.DruidColumnType.REGULAR) : new DruidColumnHandle(variable, DruidColumnHandle.DruidColumnType.DERIVED);
            result.put(variable, handle);
        });
        return result;
    }

    public DruidQueryGeneratorContext withOutputColumns(List<VariableReferenceExpression> outputColumns)
    {
        Map<VariableReferenceExpression, Selection> newSelections = new LinkedHashMap<>();
        outputColumns.forEach(o -> newSelections.put(o, requireNonNull(selections.get(o), "Cannot find the selection " + o + " in the original context " + this)));

        return new DruidQueryGeneratorContext(newSelections, from, filter, limit);
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
