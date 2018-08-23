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
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.padEnd;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Float.intBitsToFloat;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.stream.Collectors.joining;
import static org.joda.time.DateTimeZone.UTC;

// TODO: Put the setter methods in another class to keep the API clean
// - It could be a new class or inner class; this is easiest.
// - It could be a class that shares code with JdbcPageSink; this is best.
//
// Consider the relaton between this and JdbcPageSink. They don't get their data
// directly from the same source, they both use PreparedStatement setters, and
// similar considerations must be taken between them.
// They also should support identical types, but this class supports more by default.
public class QueryBuilder
{
    // not all databases support booleans, so use 1=1 and 1=0 instead
    private static final String ALWAYS_TRUE = "1=1";
    private static final String ALWAYS_FALSE = "1=0";

    private final String quote;

    protected static class TypeAndValue
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
            JdbcClient client,
            Connection connection,
            String catalog,
            String schema,
            String table,
            List<JdbcColumnHandle> columns,
            TupleDomain<ColumnHandle> tupleDomain)
            throws SQLException
    {
        StringBuilder sql = new StringBuilder();

        String columnNames = columns.stream()
                .map(JdbcColumnHandle::getColumnName)
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
        if (!clauses.isEmpty()) {
            sql.append(" WHERE ")
                    .append(Joiner.on(" AND ").join(clauses));
        }

        PreparedStatement statement = client.getPreparedStatement(connection, sql.toString());
        setStatementParameters(statement, accumulator);
        return statement;
    }

    private void setStatementParameters(PreparedStatement statement, List<TypeAndValue> accumulator)
            throws SQLException
    {
        for (int i = 0; i < accumulator.size(); i++) {
            TypeAndValue typeAndValue = accumulator.get(i);
            int index = i + 1;
            if (isType(typeAndValue, BIGINT)) {
                setBigint(statement, index, typeAndValue);
            }
            else if (isType(typeAndValue, INTEGER)) {
                setInteger(statement, index, typeAndValue);
            }
            else if (isType(typeAndValue, SMALLINT)) {
                setSmallint(statement, index, typeAndValue);
            }
            else if (isType(typeAndValue, TINYINT)) {
                setTinyint(statement, index, typeAndValue);
            }
            else if (isType(typeAndValue, DOUBLE)) {
                setDouble(statement, index, typeAndValue);
            }
            else if (isType(typeAndValue, REAL)) {
                setReal(statement, index, typeAndValue);
            }
            else if (isType(typeAndValue, BOOLEAN)) {
                setBoolean(statement, index, typeAndValue);
            }
            else if (isType(typeAndValue, DATE)) {
                setDate(statement, index, typeAndValue);
            }
            else if (isType(typeAndValue, TIME)) {
                setTime(statement, index, typeAndValue);
            }
            else if (isType(typeAndValue, TIME_WITH_TIME_ZONE)) {
                setTimeWithTimeZone(statement, index, typeAndValue);
            }
            else if (isType(typeAndValue, TIMESTAMP)) {
                setTimestamp(statement, index, typeAndValue);
            }
            else if (isType(typeAndValue, TIMESTAMP_WITH_TIME_ZONE)) {
                setTimestampWithTimeZone(statement, index, typeAndValue);
            }
            else if (isVarcharType(typeAndValue.getType())) {
                setVarchar(statement, index, typeAndValue);
            }
            else if (isCharType(typeAndValue.getType())) {
                setChar(statement, index, typeAndValue);
            }
            else if (typeAndValue.getType() instanceof DecimalType) {
                setDecimal(statement, index, typeAndValue);
            }
            else {
                throw unsupportedType(typeAndValue.getType());
            }
        }
    }

    private static boolean isType(TypeAndValue typeval, Type type)
    {
        return type.equals(typeval.getType());
    }

    protected boolean isAcceptedType(Type type)
    {
        requireNonNull(type, "type is null");
        return type.equals(BIGINT) ||
                type.equals(TINYINT) ||
                type.equals(SMALLINT) ||
                type.equals(INTEGER) ||
                type.equals(DOUBLE) ||
                type.equals(REAL) ||
                type.equals(BOOLEAN) ||
                type.equals(DATE) ||
                type.equals(TIME) ||
                type.equals(TIME_WITH_TIME_ZONE) ||
                type.equals(TIMESTAMP) ||
                type.equals(TIMESTAMP_WITH_TIME_ZONE) ||
                isVarcharType(type) ||
                isCharType(type) ||
                type instanceof DecimalType;
    }

    private List<String> toConjuncts(List<JdbcColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain, List<TypeAndValue> accumulator)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (JdbcColumnHandle column : columns) {
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
                singleValues.add(range.getLow().getValue());
            }
            else {
                List<String> rangeConjuncts = new ArrayList<>();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue(), type, accumulator));
                            break;
                        case EXACTLY:
                            rangeConjuncts.add(toPredicate(columnName, ">=", range.getLow().getValue(), type, accumulator));
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low marker should never use BELOW bound");
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalArgumentException("High marker should never use ABOVE bound");
                        case EXACTLY:
                            rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue(), type, accumulator));
                            break;
                        case BELOW:
                            rangeConjuncts.add(toPredicate(columnName, "<", range.getHigh().getValue(), type, accumulator));
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

    protected String quote(String name)
    {
        name = name.replace(quote, quote + quote);
        return quote + name + quote;
    }

    private void bindValue(Object value, Type type, List<TypeAndValue> accumulator)
    {
        checkArgument(isAcceptedType(type), "Can't handle type: %s", type);
        accumulator.add(new TypeAndValue(type, value));
    }

    protected RuntimeException unsupportedType(Type type)
    {
        return new UnsupportedOperationException("Can't handle type: " + type);
    }

    protected void setBigint(PreparedStatement statement, int index, TypeAndValue typeAndValue)
            throws SQLException
    {
        statement.setLong(index, (long) typeAndValue.getValue());
    }

    protected void setInteger(PreparedStatement statement, int index, TypeAndValue typeAndValue)
            throws SQLException
    {
        statement.setInt(index, ((Number) typeAndValue.getValue()).intValue());
    }

    protected void setSmallint(PreparedStatement statement, int index, TypeAndValue typeAndValue)
            throws SQLException
    {
        statement.setShort(index, ((Number) typeAndValue.getValue()).shortValue());
    }

    protected void setTinyint(PreparedStatement statement, int index, TypeAndValue typeAndValue)
            throws SQLException
    {
        statement.setByte(index, ((Number) typeAndValue.getValue()).byteValue());
    }

    protected void setDouble(PreparedStatement statement, int index, TypeAndValue typeAndValue)
            throws SQLException
    {
        statement.setDouble(index, (double) typeAndValue.getValue());
    }

    protected void setReal(PreparedStatement statement, int index, TypeAndValue typeAndValue)
            throws SQLException
    {
        statement.setFloat(index, intBitsToFloat(((Number) typeAndValue.getValue()).intValue()));
    }

    protected void setBoolean(PreparedStatement statement, int index, TypeAndValue typeAndValue)
            throws SQLException
    {
        statement.setBoolean(index, (boolean) typeAndValue.getValue());
    }

    protected void setDate(PreparedStatement statement, int index, TypeAndValue typeAndValue)
            throws SQLException
    {
        long millis = DAYS.toMillis((long) typeAndValue.getValue());
        statement.setDate(index, new Date(UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millis)));
    }

    protected void setTime(PreparedStatement statement, int index, TypeAndValue typeAndValue)
            throws SQLException
    {
        statement.setTime(index, new Time((long) typeAndValue.getValue()));
    }

    protected void setTimeWithTimeZone(PreparedStatement statement, int index, TypeAndValue typeAndValue)
            throws SQLException
    {
        statement.setTime(index, new Time(unpackMillisUtc((long) typeAndValue.getValue())));
    }

    protected void setTimestamp(PreparedStatement statement, int index, TypeAndValue typeAndValue)
            throws SQLException
    {
        statement.setTimestamp(index, new Timestamp((long) typeAndValue.getValue()));
    }

    protected void setTimestampWithTimeZone(PreparedStatement statement, int index, TypeAndValue typeAndValue)
            throws SQLException
    {
        statement.setTimestamp(index, new Timestamp(unpackMillisUtc((long) typeAndValue.getValue())));
    }

    protected void setVarchar(PreparedStatement statement, int index, TypeAndValue typeAndValue)
            throws SQLException
    {
        statement.setString(index, ((Slice) typeAndValue.getValue()).toStringUtf8());
    }

    protected void setChar(PreparedStatement statement, int index, TypeAndValue typeAndValue)
            throws SQLException
    {
        String base = ((Slice) typeAndValue.getValue()).toStringUtf8();
        int size = ((CharType) typeAndValue.getType()).getLength();
        statement.setString(index, padEnd(base, size, ' '));
    }

    protected void setDecimal(PreparedStatement statement, int index, TypeAndValue typeAndValue)
            throws SQLException
    {
        DecimalType type = (DecimalType) typeAndValue.getType();

        BigInteger unscaledValue = type.isShort()
                ? BigInteger.valueOf((Long) typeAndValue.getValue())
                : decodeUnscaledValue((Slice) typeAndValue.getValue());

        // TODO: Is the MathContext necessary?
        // The rounding mode doesn't seem to do anything, because Presto
        // converts everything to the right scale/precision before it gets here.
        // If Presto didn't do that, I think we'd want to go without precision,
        // so numbers could be compared to numbers with different precision.
        BigDecimal value = new BigDecimal(unscaledValue, type.getScale(),
                new MathContext(type.getPrecision(), RoundingMode.UNNECESSARY));

        statement.setBigDecimal(index, value);
    }
}
