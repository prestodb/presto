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
package com.facebook.presto.pinot;

import io.airlift.log.Logger;

import java.util.List;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

/**
 * This class manages how to generate the query to send to Pinot servers.
 */
public final class PinotQueryGenerator
{
    private static final Logger log = Logger.get(PinotQueryGenerator.class);

    private PinotQueryGenerator()
    {
    }

    /**
     * QUERY_TEMPLATE looks like this:
     * SELECT $fields FROM $tableName $predicate LIMIT $limit.
     * <p>
     * Note $predicate is optional, and we intentionally add a space between $tableName $predicate for readability.
     * When $predicate is absent, there would be 2 spaces between $tableName and LIMIT, which is should not hurt the query itself.
     */
    public static final String QUERY_TEMPLATE = "SELECT %s FROM %s %s LIMIT %d";

    /**
     * Returns the Pinot Query to send for each split.
     *
     * <p>Pinot Query would be constructed based on {$link #QUERY_TEMPLATE} and predicates (WHERE ...).
     *
     * @return the constructed Pinot Query
     */
    static String getPinotQuery(PinotConfig pinotConfig, List<PinotColumnHandle> columnHandles, String pinotFilter, String timeFilter, String tableName, long splitLimit)
    {
        requireNonNull(pinotConfig, "pinotConfig is null");
        StringJoiner fieldsJoiner = new StringJoiner(", ");
        for (PinotColumnHandle columnHandle : columnHandles) {
            // No aggregation pushdown
            fieldsJoiner.add(columnHandle.getColumnName());
        }

        // Add predicates
        StringJoiner predicatesJoiner = new StringJoiner(" AND ");
        if (!pinotFilter.isEmpty()) {
            predicatesJoiner.add(String.format("(%s)", pinotFilter));
        }
        if (!timeFilter.isEmpty()) {
            predicatesJoiner.add(String.format("(%s)", timeFilter));
        }

        // Note pinotPredicate is optional. It would be empty when no predicates are pushed down.
        String pinotPredicate = "";
        if (predicatesJoiner.length() > 0) {
            pinotPredicate = "WHERE " + predicatesJoiner.toString();
        }

        long limit = splitLimit > 0 ? splitLimit : pinotConfig.getLimitAll();

        final String finalQuery = String.format(QUERY_TEMPLATE, fieldsJoiner.toString(), tableName, pinotPredicate, limit);
        log.debug("Plan to send PQL : %s", finalQuery);
        return finalQuery;
    }
}
