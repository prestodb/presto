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
package com.facebook.presto.plugin.opensearch;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds OpenSearch queries from Presto predicates.
 * Translates SQL WHERE clauses to OpenSearch Query DSL.
 * Supports nested field queries with automatic optimization.
 */
public class QueryBuilder
{
    private static final Logger log = Logger.get(QueryBuilder.class);

    private final boolean optimizeNestedQueries;

    public QueryBuilder()
    {
        this(true);
    }

    public QueryBuilder(boolean optimizeNestedQueries)
    {
        this.optimizeNestedQueries = optimizeNestedQueries;
    }

    public String buildQuery(TupleDomain<ColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll()) {
            return "{\"match_all\":{}}";
        }

        if (tupleDomain.isNone()) {
            return "{\"match_none\":{}}";
        }

        Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().get();
        if (domains.isEmpty()) {
            return "{\"match_all\":{}}";
        }

        List<String> conditions = new ArrayList<>();

        // Group nested field predicates by parent path for optimization
        if (optimizeNestedQueries) {
            Map<String, List<NestedPredicate>> nestedPredicates = groupNestedPredicates(domains);

            // Build optimized nested queries
            for (Map.Entry<String, List<NestedPredicate>> entry : nestedPredicates.entrySet()) {
                String parentPath = entry.getKey();
                List<NestedPredicate> predicates = entry.getValue();

                if (predicates.size() == 1) {
                    // Single predicate - use simple nested query
                    conditions.add(predicates.get(0).query);
                }
                else {
                    // Multiple predicates on same parent - group them
                    conditions.add(buildGroupedNestedQuery(parentPath, predicates));
                }
            }

            // Add flat field conditions
            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                OpenSearchColumnHandle column = (OpenSearchColumnHandle) entry.getKey();
                if (!column.isNestedField()) {
                    String condition = buildCondition(column, entry.getValue());
                    if (condition != null) {
                        conditions.add(condition);
                    }
                }
            }
        }
        else {
            // No optimization - build conditions individually
            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                OpenSearchColumnHandle column = (OpenSearchColumnHandle) entry.getKey();
                Domain domain = entry.getValue();

                String condition = buildCondition(column, domain);
                if (condition != null) {
                    conditions.add(condition);
                }
            }
        }

        if (conditions.isEmpty()) {
            return "{\"match_all\":{}}";
        }

        if (conditions.size() == 1) {
            return conditions.get(0);
        }

        // Multiple conditions - use bool query with must
        StringBuilder query = new StringBuilder();
        query.append("{\"bool\":{\"must\":[");
        query.append(String.join(",", conditions));
        query.append("]}}");

        return query.toString();
    }

    private String buildCondition(OpenSearchColumnHandle column, Domain domain)
    {
        if (domain.isNullAllowed()) {
            // Handle NULL values
            return null; // Skip for now
        }

        // Check if this is a nested field
        if (column.isNestedField()) {
            return buildNestedCondition(column, domain);
        }

        // Flat field logic
        if (domain.isSingleValue()) {
            Object value = domain.getSingleValue();
            return buildTermQuery(column.getColumnName(), value);
        }

        List<Range> ranges = ImmutableList.copyOf(domain.getValues().getRanges().getOrderedRanges());
        if (ranges.size() == 1) {
            Range range = ranges.get(0);
            return buildRangeQuery(column.getColumnName(), range);
        }

        // Multiple ranges - use bool should
        List<String> rangeQueries = new ArrayList<>();
        for (Range range : ranges) {
            String rangeQuery = buildRangeQuery(column.getColumnName(), range);
            if (rangeQuery != null) {
                rangeQueries.add(rangeQuery);
            }
        }

        if (rangeQueries.isEmpty()) {
            return null;
        }

        if (rangeQueries.size() == 1) {
            return rangeQueries.get(0);
        }

        return "{\"bool\":{\"should\":[" + String.join(",", rangeQueries) + "]}}";
    }

    /**
     * Builds an OpenSearch nested query for a nested field predicate.
     *
     * Example:
     * SQL: WHERE token_usage.total_tokens > 50000
     *
     * OpenSearch DSL:
     * {
     *   "nested": {
     *     "path": "token_usage",
     *     "query": {
     *       "range": {
     *         "token_usage.total_tokens": {"gt": 50000}
     *       }
     *     }
     *   }
     * }
     */
    private String buildNestedCondition(OpenSearchColumnHandle column, Domain domain)
    {
        String parentPath = column.getParentFieldPath();
        String fullFieldPath = column.getColumnName();

        // Build inner query
        String innerQuery;
        if (domain.isSingleValue()) {
            Object value = domain.getSingleValue();
            innerQuery = buildTermQuery(fullFieldPath, value);
        }
        else {
            List<Range> ranges = ImmutableList.copyOf(domain.getValues().getRanges().getOrderedRanges());
            if (ranges.size() == 1) {
                innerQuery = buildRangeQuery(fullFieldPath, ranges.get(0));
            }
            else {
                // Multiple ranges
                List<String> rangeQueries = new ArrayList<>();
                for (Range range : ranges) {
                    String rangeQuery = buildRangeQuery(fullFieldPath, range);
                    if (rangeQuery != null) {
                        rangeQueries.add(rangeQuery);
                    }
                }
                innerQuery = "{\"bool\":{\"should\":[" + String.join(",", rangeQueries) + "]}}";
            }
        }

        // Wrap in nested query
        String nestedQuery = String.format(
                "{\"nested\":{\"path\":\"%s\",\"query\":%s}}",
                parentPath,
                innerQuery);

        log.debug("Built nested query for field %s: %s", fullFieldPath, nestedQuery);
        return nestedQuery;
    }

    /**
     * Groups nested field predicates by parent path for optimization.
     * This allows multiple predicates on the same nested object to be combined.
     */
    private Map<String, List<NestedPredicate>> groupNestedPredicates(
            Map<ColumnHandle, Domain> domains)
    {
        Map<String, List<NestedPredicate>> grouped = new HashMap<>();

        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            OpenSearchColumnHandle column = (OpenSearchColumnHandle) entry.getKey();
            if (column.isNestedField()) {
                String parentPath = column.getParentFieldPath();
                String innerQuery = buildInnerQuery(column, entry.getValue());

                if (innerQuery != null) {
                    grouped.computeIfAbsent(parentPath, k -> new ArrayList<>())
                            .add(new NestedPredicate(
                                    buildNestedCondition(column, entry.getValue()),
                                    innerQuery));
                }
            }
        }

        return grouped;
    }

    /**
     * Builds the inner query for a nested field (without the nested wrapper).
     */
    private String buildInnerQuery(OpenSearchColumnHandle column, Domain domain)
    {
        String fullFieldPath = column.getColumnName();

        if (domain.isSingleValue()) {
            return buildTermQuery(fullFieldPath, domain.getSingleValue());
        }

        List<Range> ranges = ImmutableList.copyOf(domain.getValues().getRanges().getOrderedRanges());
        if (ranges.size() == 1) {
            return buildRangeQuery(fullFieldPath, ranges.get(0));
        }

        // Multiple ranges
        List<String> rangeQueries = new ArrayList<>();
        for (Range range : ranges) {
            String rangeQuery = buildRangeQuery(fullFieldPath, range);
            if (rangeQuery != null) {
                rangeQueries.add(rangeQuery);
            }
        }

        if (rangeQueries.isEmpty()) {
            return null;
        }

        return "{\"bool\":{\"should\":[" + String.join(",", rangeQueries) + "]}}";
    }

    /**
     * Builds an optimized nested query that groups multiple predicates on the same parent.
     *
     * Example:
     * SQL: WHERE token_usage.total_tokens > 50000 AND token_usage.output_tokens < 2000
     *
     * OpenSearch DSL:
     * {
     *   "nested": {
     *     "path": "token_usage",
     *     "query": {
     *       "bool": {
     *         "must": [
     *           {"range": {"token_usage.total_tokens": {"gt": 50000}}},
     *           {"range": {"token_usage.output_tokens": {"lt": 2000}}}
     *         ]
     *       }
     *     }
     *   }
     * }
     */
    private String buildGroupedNestedQuery(String parentPath, List<NestedPredicate> predicates)
    {
        List<String> innerQueries = new ArrayList<>();
        for (NestedPredicate predicate : predicates) {
            innerQueries.add(predicate.innerQuery);
        }

        String combinedInnerQuery = "{\"bool\":{\"must\":[" +
                                   String.join(",", innerQueries) + "]}}";

        String groupedQuery = String.format(
                "{\"nested\":{\"path\":\"%s\",\"query\":%s}}",
                parentPath,
                combinedInnerQuery);

        log.debug("Built grouped nested query for path %s with %d predicates",
                 parentPath, predicates.size());
        return groupedQuery;
    }

    /**
     * Helper class to store nested predicate information.
     */
    private static class NestedPredicate
    {
        final String query;       // Full nested query
        final String innerQuery;  // Inner query without nested wrapper

        NestedPredicate(String query, String innerQuery)
        {
            this.query = query;
            this.innerQuery = innerQuery;
        }
    }

    private String buildTermQuery(String field, Object value)
    {
        String valueStr = formatValue(value);
        return "{\"term\":{\"" + field + "\":" + valueStr + "}}";
    }

    private String buildRangeQuery(String field, Range range)
    {
        StringBuilder query = new StringBuilder();
        query.append("{\"range\":{\"").append(field).append("\":{");

        List<String> conditions = new ArrayList<>();

        if (!range.isLowUnbounded()) {
            String operator = range.isLowInclusive() ? "gte" : "gt";
            conditions.add("\"" + operator + "\":" + formatValue(range.getLowBoundedValue()));
        }

        if (!range.isHighUnbounded()) {
            String operator = range.isHighInclusive() ? "lte" : "lt";
            conditions.add("\"" + operator + "\":" + formatValue(range.getHighBoundedValue()));
        }

        if (conditions.isEmpty()) {
            return null;
        }

        query.append(String.join(",", conditions));
        query.append("}}}");

        return query.toString();
    }

    private String formatValue(Object value)
    {
        if (value == null) {
            return "null";
        }
        if (value instanceof String) {
            return "\"" + escapeJson((String) value) + "\"";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        return "\"" + value.toString() + "\"";
    }

    private String escapeJson(String value)
    {
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
