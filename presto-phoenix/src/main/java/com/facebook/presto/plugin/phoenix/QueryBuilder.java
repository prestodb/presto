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
package com.facebook.presto.plugin.phoenix;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeWithTimeZoneType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.Decimals.isLongDecimal;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Float.intBitsToFloat;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.stream.Collectors.joining;
import static org.joda.time.DateTimeZone.UTC;

public class QueryBuilder
{
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

    public QueryBuilder()
    {
    }

    public String buildSql(PhoenixConnection connection, String catalog, String schema, String table, List<PhoenixColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain)
            throws SQLException
    {
        StringBuilder sql = new StringBuilder();

        String columnNames = columns.stream()
                .map(PhoenixColumnHandle::getColumnName)
                .collect(joining(", "));

        sql.append("SELECT ");
        sql.append(columnNames);
        if (columns.isEmpty()) {
            sql.append("null");
        }

        sql.append(" FROM ");
        if (!isNullOrEmpty(catalog)) {
            sql.append(catalog).append('.');
        }
        if (!isNullOrEmpty(schema)) {
            sql.append(schema).append('.');
        }
        sql.append(table);

        List<TypeAndValue> accumulator = new ArrayList<>();

        List<String> clauses = toConjuncts(columns, tupleDomain, accumulator);
        if (!clauses.isEmpty()) {
            sql.append(" WHERE ")
                    .append(Joiner.on(" AND ").join(clauses));
        }

        try (PhoenixPreparedStatement statement = connection.prepareStatement(sql.toString()).unwrap(PhoenixPreparedStatement.class)) {
            for (int i = 0; i < accumulator.size(); i++) {
                TypeAndValue typeAndValue = accumulator.get(i);
                if (typeAndValue.getType().equals(BigintType.BIGINT)) {
                    statement.setLong(i + 1, (long) typeAndValue.getValue());
                }
                else if (typeAndValue.getType().equals(IntegerType.INTEGER)) {
                    statement.setInt(i + 1, ((Number) typeAndValue.getValue()).intValue());
                }
                else if (typeAndValue.getType().equals(SmallintType.SMALLINT)) {
                    statement.setShort(i + 1, ((Number) typeAndValue.getValue()).shortValue());
                }
                else if (typeAndValue.getType().equals(TinyintType.TINYINT)) {
                    statement.setByte(i + 1, ((Number) typeAndValue.getValue()).byteValue());
                }
                else if (typeAndValue.getType().equals(DoubleType.DOUBLE)) {
                    statement.setDouble(i + 1, (double) typeAndValue.getValue());
                }
                else if (typeAndValue.getType().equals(RealType.REAL)) {
                    statement.setFloat(i + 1, intBitsToFloat(((Number) typeAndValue.getValue()).intValue()));
                }
                else if (typeAndValue.getType().equals(BooleanType.BOOLEAN)) {
                    statement.setBoolean(i + 1, (boolean) typeAndValue.getValue());
                }
                else if (typeAndValue.getType().equals(DateType.DATE)) {
                    long millis = DAYS.toMillis((long) typeAndValue.getValue());
                    statement.setDate(i + 1, new Date(UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millis)));
                }
                else if (typeAndValue.getType().equals(TimeType.TIME)) {
                    statement.setTime(i + 1, new Time((long) typeAndValue.getValue()));
                }
                else if (typeAndValue.getType().equals(TimeWithTimeZoneType.TIME_WITH_TIME_ZONE)) {
                    statement.setTime(i + 1, new Time(unpackMillisUtc((long) typeAndValue.getValue())));
                }
                else if (typeAndValue.getType().equals(TimestampType.TIMESTAMP)) {
                    statement.setTimestamp(i + 1, new Timestamp((long) typeAndValue.getValue()));
                }
                else if (typeAndValue.getType().equals(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE)) {
                    statement.setTimestamp(i + 1, new Timestamp(unpackMillisUtc((long) typeAndValue.getValue())));
                }
                else if (typeAndValue.getType() instanceof VarcharType) {
                    statement.setString(i + 1, ((Slice) typeAndValue.getValue()).toStringUtf8());
                }
                else if (typeAndValue.getType() instanceof VarcharType) {
                    statement.setString(i + 1, ((Slice) typeAndValue.getValue()).toStringUtf8());
                }
                else if (isShortDecimal(typeAndValue.getType())) {
                    int scale = ((DecimalType) typeAndValue.getType()).getScale();
                    BigInteger unscaledValue = BigInteger.valueOf((long) typeAndValue.getValue());
                    statement.setBigDecimal(i + 1, new BigDecimal(unscaledValue, scale));
                }
                else if (isLongDecimal(typeAndValue.getType())) {
                    int scale = ((DecimalType) typeAndValue.getType()).getScale();
                    BigInteger unscaledValue = Decimals.decodeUnscaledValue((Slice) typeAndValue.getValue());
                    statement.setBigDecimal(i + 1, new BigDecimal(unscaledValue, scale));
                }
                else {
                    throw new UnsupportedOperationException("Can't handle type: " + typeAndValue.getType());
                }
            }
            return generateActualSql(statement.toString(), statement.getParameters().toArray());
        }
    }

    private static boolean isAcceptedType(Type type)
    {
        Type validType = requireNonNull(type, "type is null");
        return validType.equals(BigintType.BIGINT) ||
                validType.equals(TinyintType.TINYINT) ||
                validType.equals(SmallintType.SMALLINT) ||
                validType.equals(IntegerType.INTEGER) ||
                validType.equals(DoubleType.DOUBLE) ||
                validType.equals(RealType.REAL) ||
                validType.equals(BooleanType.BOOLEAN) ||
                validType.equals(DateType.DATE) ||
                validType.equals(TimeType.TIME) ||
                validType.equals(TimeWithTimeZoneType.TIME_WITH_TIME_ZONE) ||
                validType.equals(TimestampType.TIMESTAMP) ||
                validType.equals(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE) ||
                validType instanceof VarcharType ||
                validType instanceof DecimalType;
    }

    private List<String> toConjuncts(List<PhoenixColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain, List<TypeAndValue> accumulator)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (PhoenixColumnHandle column : columns) {
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
            return domain.isNullAllowed() ? columnName + " IS NULL" : "FALSE";
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? "TRUE" : columnName + " IS NOT NULL";
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
            disjuncts.add(columnName + " IN (" + values + ")");
        }

        // Add nullability disjuncts
        checkState(!disjuncts.isEmpty());
        if (domain.isNullAllowed()) {
            disjuncts.add(columnName + " IS NULL");
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private String toPredicate(String columnName, String operator, Object value, Type type, List<TypeAndValue> accumulator)
    {
        bindValue(value, type, accumulator);
        return columnName + " " + operator + " ?";
    }

    private static void bindValue(Object value, Type type, List<TypeAndValue> accumulator)
    {
        checkArgument(isAcceptedType(type), "Can't handle type: %s", type);
        accumulator.add(new TypeAndValue(type, value));
    }

    private static String generateActualSql(String sqlQuery, Object... parameters)
    {
        String[] parts = sqlQuery.split("\\?");
        StringBuilder sb = new StringBuilder();

        // This might be wrong if some '?' are used as litteral '?'
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            sb.append(part);
            if (i < parameters.length) {
                sb.append(formatParameter(parameters[i]));
            }
        }

        return sb.toString();
    }

    private static String formatParameter(Object parameter)
    {
        if (parameter == null) {
            return "NULL";
        }
        else {
            if (parameter instanceof String) {
                return "'" + ((String) parameter).replace("'", "''") + "'";
            }
            else if (parameter instanceof Timestamp) {
                return "to_timestamp('" + new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS").format(parameter) + "', 'mm/dd/yyyy hh24:mi:ss.ff3')";
            }
            else if (parameter instanceof Date) {
                return "to_date('" + new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(parameter) + "', 'mm/dd/yyyy hh24:mi:ss')";
            }
            else if (parameter instanceof Boolean) {
                return ((Boolean) parameter).booleanValue() ? "1" : "0";
            }
            else {
                return parameter.toString();
            }
        }
    }
}
