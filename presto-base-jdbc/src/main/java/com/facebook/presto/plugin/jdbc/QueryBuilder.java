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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;

public class QueryBuilder
{
    private final String quote;

    public QueryBuilder(String quote)
    {
        this.quote = checkNotNull(quote, "quote is null");
    }

    public String buildSql(String catalog, String schema, String table, List<JdbcColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain)
    {
        StringBuilder sql = new StringBuilder();

        sql.append("SELECT ");
        Joiner.on(", ").appendTo(sql, transform(columns, column -> quote(column.getColumnName())));
        if (columns.isEmpty()) {
            sql.append("null");
        }

        sql.append(" FROM ");
        if (!isNullOrEmpty(catalog)) {
            sql.append(quote(catalog)).append('.');
        }
        if (!isNullOrEmpty(schema)) {
            sql.append(quote(schema)).append('.');
        }
        sql.append(quote(table));

        List<String> clauses = toConjuncts(columns, tupleDomain);
        if (!clauses.isEmpty()) {
            sql.append(" WHERE ")
                    .append(Joiner.on(" AND ").join(clauses));
        }

        return sql.toString();
    }

    private List<String> toConjuncts(List<JdbcColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (JdbcColumnHandle column : columns) {
            Type type = column.getColumnType();
            if (type.equals(BigintType.BIGINT) || type.equals(DoubleType.DOUBLE) || type.equals(BooleanType.BOOLEAN)) {
                Domain domain = tupleDomain.getDomains().get(column);
                if (domain != null) {
                    builder.add(toPredicate(column.getColumnName(), domain));
                }
            }
        }
        return builder.build();
    }

    private String toPredicate(String columnName, Domain domain)
    {
        if (domain.getRanges().isNone() && domain.isNullAllowed()) {
            return quote(columnName) + " IS NULL";
        }

        if (domain.getRanges().isAll() && !domain.isNullAllowed()) {
            return quote(columnName) + " IS NOT NULL";
        }

        // Add disjuncts for ranges
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        for (Range range : domain.getRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                singleValues.add(range.getLow().getValue());
            }
            else {
                List<String> rangeConjuncts = new ArrayList<>();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue()));
                            break;
                        case EXACTLY:
                            rangeConjuncts.add(toPredicate(columnName, ">=", range.getLow().getValue()));
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low Marker should never use BELOW bound: " + range);
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalArgumentException("High Marker should never use ABOVE bound: " + range);
                        case EXACTLY:
                            rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue()));
                            break;
                        case BELOW:
                            rangeConjuncts.add(toPredicate(columnName, "<", range.getHigh().getValue()));
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(toPredicate(columnName, "=", getOnlyElement(singleValues)));
        }
        else if (singleValues.size() > 1) {
            disjuncts.add(quote(columnName) + " IN (" + Joiner.on(",").join(transform(singleValues, QueryBuilder::encode)) + ")");
        }

        // Add nullability disjuncts
        checkState(!disjuncts.isEmpty());
        if (domain.isNullAllowed()) {
            disjuncts.add(quote(columnName) + " IS NULL");
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private String toPredicate(String columnName, String operator, Object value)
    {
        return quote(columnName) + " " + operator + " " + encode(value);
    }

    private String quote(String name)
    {
        name = name.replace(quote, quote + quote);
        return quote + name + quote;
    }

    private static String encode(Object value)
    {
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        throw new UnsupportedOperationException("Can't handle type: " + value.getClass().getName());
    }
}
