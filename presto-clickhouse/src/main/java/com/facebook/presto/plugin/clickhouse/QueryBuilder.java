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
package com.facebook.presto.plugin.clickhouse;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.clickhouse.optimization.ClickHouseExpression;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.plugin.clickhouse.DateTimeUtil.convertZonedDaysToDate;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Float.intBitsToFloat;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class QueryBuilder
{
    private static final String ALWAYS_TRUE = "1=1";
    private static final String ALWAYS_FALSE = "1=0";

    private final String quote;

    private static class TypeAndValue
    {
        private final Type type;
        private final Object value;

        public TypeAndValue(Type type, Object value)
        {
            this.type = requireNonNull(type, "type is null");
            this.value = requireNonNull(value, "value is null");
        }

        public Type getType()
        {
            return type;
        }

        public Object getValue()
        {
            return value;
        }
    }

    public QueryBuilder(String quote)
    {
        this.quote = requireNonNull(quote, "quote is null");
    }

    public PreparedStatement buildSql(
            ClickHouseClient client,
            ConnectorSession session,
            Connection connection,
            String catalog,
            String schema,
            String table,
            List<ClickHouseColumnHandle> columns,
            TupleDomain<ColumnHandle> tupleDomain,
            Optional<ClickHouseExpression> additionalPredicate,
            Optional<String> simpleExpression)
            throws SQLException
    {
        StringBuilder sql = new StringBuilder();

        String columnNames = columns.stream()
                .map(ClickHouseColumnHandle::getColumnName)
                .map(this::quote)
                .collect(joining(", "));

        sql.append("SELECT ");
        sql.append(columnNames);
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

        List<TypeAndValue> accumulator = new ArrayList<>();

        List<String> clauses = toConjuncts(columns, tupleDomain, accumulator);
        if (additionalPredicate.isPresent()) {
            clauses = ImmutableList.<String>builder()
                    .addAll(clauses)
                    .add(additionalPredicate.get().getExpression())
                    .build();
            accumulator.addAll(additionalPredicate.get().getBoundConstantValues().stream()
                    .map(constantExpression -> new TypeAndValue(constantExpression.getType(), constantExpression.getValue()))
                    .collect(ImmutableList.toImmutableList()));
        }

        if (!clauses.isEmpty()) {
            sql.append(" WHERE ")
                    .append(Joiner.on(" AND ").join(clauses));
        }

        if (simpleExpression.isPresent()) {
            sql.append(simpleExpression.get());
        }

        sql.append(String.format("/* %s : %s */", session.getUser(), session.getQueryId()));
        PreparedStatement statement = client.getPreparedStatement(connection, sql.toString());

        for (int i = 0; i < accumulator.size(); i++) {
            TypeAndValue typeAndValue = accumulator.get(i);
            if (typeAndValue.getType().equals(BIGINT)) {
                statement.setLong(i + 1, (long) typeAndValue.getValue());
            }
            else if (typeAndValue.getType().equals(INTEGER)) {
                statement.setInt(i + 1, ((Number) typeAndValue.getValue()).intValue());
            }
            else if (typeAndValue.getType().equals(SMALLINT)) {
                statement.setShort(i + 1, ((Number) typeAndValue.getValue()).shortValue());
            }
            else if (typeAndValue.getType().equals(TINYINT)) {
                statement.setByte(i + 1, ((Number) typeAndValue.getValue()).byteValue());
            }
            else if (typeAndValue.getType().equals(DOUBLE)) {
                statement.setDouble(i + 1, (double) typeAndValue.getValue());
            }
            else if (typeAndValue.getType().equals(REAL)) {
                statement.setFloat(i + 1, intBitsToFloat(((Number) typeAndValue.getValue()).intValue()));
            }
            else if (typeAndValue.getType().equals(BOOLEAN)) {
                statement.setBoolean(i + 1, (boolean) typeAndValue.getValue());
            }
            else if (typeAndValue.getType().equals(DATE)) {
                statement.setDate(i + 1, convertZonedDaysToDate((long) typeAndValue.getValue()));
            }
            else if (typeAndValue.getType().equals(TIME)) {
                statement.setTime(i + 1, new Time((long) typeAndValue.getValue()));
            }
            else if (typeAndValue.getType().equals(TIME_WITH_TIME_ZONE)) {
                statement.setTime(i + 1, new Time(unpackMillisUtc((long) typeAndValue.getValue())));
            }
            else if (typeAndValue.getType().equals(TIMESTAMP)) {
                statement.setTimestamp(i + 1, new Timestamp((long) typeAndValue.getValue()));
            }
            else if (typeAndValue.getType().equals(TIMESTAMP_WITH_TIME_ZONE)) {
                statement.setTimestamp(i + 1, new Timestamp(unpackMillisUtc((long) typeAndValue.getValue())));
            }
            else if (typeAndValue.getType() instanceof VarcharType) {
                statement.setString(i + 1, ((Slice) typeAndValue.getValue()).toStringUtf8());
            }
            else if (typeAndValue.getType() instanceof CharType) {
                statement.setString(i + 1, ((Slice) typeAndValue.getValue()).toStringUtf8());
            }
            else {
                throw new UnsupportedOperationException("Can't handle type: " + typeAndValue.getType());
            }
        }

        return statement;
    }

    private static boolean isAcceptedType(Type type)
    {
        Type validType = requireNonNull(type, "type is null");
        return validType.equals(BIGINT) ||
                validType.equals(TINYINT) ||
                validType.equals(SMALLINT) ||
                validType.equals(INTEGER) ||
                validType.equals(DOUBLE) ||
                validType.equals(REAL) ||
                validType.equals(BOOLEAN) ||
                validType.equals(DATE) ||
                validType.equals(TIME) ||
                validType.equals(TIME_WITH_TIME_ZONE) ||
                validType.equals(TIMESTAMP) ||
                validType.equals(TIMESTAMP_WITH_TIME_ZONE) ||
                validType instanceof VarcharType ||
                validType instanceof CharType;
    }

    private List<String> toConjuncts(List<ClickHouseColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain, List<TypeAndValue> accumulator)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (ClickHouseColumnHandle column : columns) {
            Type type = column.getColumnType();
            if (isAcceptedType(type)) {
                Domain domain = tupleDomain.getDomains().get().get(column);
                if (domain != null) {
                    builder.add(toPredicate(column.getColumnName(), domain, type, accumulator));
                }
            }
        }
        return builder.build();
    }

    private String toPredicate(String columnName, Domain domain, Type type, List<TypeAndValue> accumulator)
    {
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? quote(columnName) + " IS NULL" : ALWAYS_FALSE;
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? ALWAYS_TRUE : quote(columnName) + " IS NOT NULL";
        }

        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll()); // Already checked
            if (range.isSingleValue()) {
                singleValues.add(range.getSingleValue());
            }
            else {
                List<String> rangeConjuncts = new ArrayList<>();
                if (!range.isLowUnbounded()) {
                    rangeConjuncts.add(toPredicate(columnName, range.isLowInclusive() ? ">=" : ">", range.getLowBoundedValue(), type, accumulator));
                }
                if (!range.isHighUnbounded()) {
                    rangeConjuncts.add(toPredicate(columnName, range.isHighInclusive() ? "<=" : "<", range.getHighBoundedValue(), type, accumulator));
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(toPredicate(columnName, "=", getOnlyElement(singleValues), type, accumulator));
        }
        else if (singleValues.size() > 1) {
            for (Object value : singleValues) {
                bindValue(value, type, accumulator);
            }
            String values = Joiner.on(",").join(nCopies(singleValues.size(), "?"));
            disjuncts.add(quote(columnName) + " IN (" + values + ")");
        }

        // Add nullability disjuncts
        checkState(!disjuncts.isEmpty());
        if (domain.isNullAllowed()) {
            disjuncts.add(quote(columnName) + " IS NULL");
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private String toPredicate(String columnName, String operator, Object value, Type type, List<TypeAndValue> accumulator)
    {
        bindValue(value, type, accumulator);
        return quote(columnName) + " " + operator + " ?";
    }

    private String quote(String name)
    {
        name = name.replace(quote, quote + quote);
        return quote + name + quote;
    }

    private static void bindValue(Object value, Type type, List<TypeAndValue> accumulator)
    {
        checkArgument(isAcceptedType(type), "Can't handle type: %s", type);
        accumulator.add(new TypeAndValue(type, value));
    }
}
