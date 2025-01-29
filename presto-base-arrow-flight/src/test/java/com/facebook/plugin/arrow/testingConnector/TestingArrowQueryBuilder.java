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
package com.facebook.plugin.arrow.testingConnector;

import com.facebook.plugin.arrow.ArrowColumnHandle;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class TestingArrowQueryBuilder
{
    // not all databases support booleans, so use 1=1 and 1=0 instead
    private static final String ALWAYS_TRUE = "1=1";
    private static final String ALWAYS_FALSE = "1=0";
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String TIME_FORMAT = "HH:mm:ss";
    public static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone(ZoneId.of("UTC"));

    public String buildSql(
            String schema,
            String table,
            List<ArrowColumnHandle> columns,
            Map<String, String> columnExpressions,
            TupleDomain<ColumnHandle> tupleDomain)
    {
        StringBuilder sql = new StringBuilder();

        sql.append("SELECT ");
        sql.append(addColumnExpression(columns, columnExpressions));

        sql.append(" FROM ");
        if (!isNullOrEmpty(schema)) {
            sql.append(quote(schema)).append('.');
        }
        sql.append(quote(table));

        List<TypeAndValue> accumulator = new ArrayList<>();

        if (tupleDomain != null && !tupleDomain.isAll()) {
            List<String> clauses = toConjuncts(columns, tupleDomain, accumulator);
            if (!clauses.isEmpty()) {
                sql.append(" WHERE ")
                        .append(Joiner.on(" AND ").join(clauses));
            }
        }

        return sql.toString();
    }

    public static String convertEpochToString(long epochValue, Type type)
    {
        if (type instanceof DateType) {
            long millis = TimeUnit.DAYS.toMillis(epochValue);
            Date date = new Date(millis);
            SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
            dateFormat.setTimeZone(UTC_TIME_ZONE);
            return dateFormat.format(date);
        }
        else if (type instanceof TimestampType) {
            Timestamp timestamp = new Timestamp(epochValue);
            SimpleDateFormat timestampFormat = new SimpleDateFormat(TIMESTAMP_FORMAT);
            timestampFormat.setTimeZone(UTC_TIME_ZONE);
            return timestampFormat.format(timestamp);
        }
        else if (type instanceof TimeType) {
            long millis = TimeUnit.SECONDS.toMillis(epochValue / 1000);
            Time time = new Time(millis);
            SimpleDateFormat timeFormat = new SimpleDateFormat(TIME_FORMAT);
            timeFormat.setTimeZone(UTC_TIME_ZONE);
            return timeFormat.format(time);
        }
        else {
            throw new UnsupportedOperationException(type + " is not supported.");
        }
    }

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

    private String addColumnExpression(List<ArrowColumnHandle> columns, Map<String, String> columnExpressions)
    {
        if (columns.isEmpty()) {
            return "null";
        }

        return columns.stream()
                .map(arrowColumnHandle -> {
                    String columnAlias = quote(arrowColumnHandle.getColumnName());
                    String expression = columnExpressions.get(arrowColumnHandle.getColumnName());
                    if (expression == null) {
                        return columnAlias;
                    }
                    return format("%s AS %s", expression, columnAlias);
                })
                .collect(joining(", "));
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
                validType.equals(TimestampType.TIMESTAMP) ||
                validType instanceof VarcharType ||
                validType instanceof CharType;
    }
    private List<String> toConjuncts(List<ArrowColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain, List<TypeAndValue> accumulator)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (ArrowColumnHandle column : columns) {
            Type type = column.getColumnType();
            if (isAcceptedType(type)) {
                Domain domain = tupleDomain.getDomains().get().get(column);
                if (domain != null) {
                    builder.add(toPredicate(column.getColumnName(), domain, column, accumulator));
                }
            }
        }
        return builder.build();
    }

    private String toPredicate(String columnName, Domain domain, ArrowColumnHandle columnHandle, List<TypeAndValue> accumulator)
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
                    rangeConjuncts.add(toPredicate(columnName, range.isLowInclusive() ? ">=" : ">", range.getLowBoundedValue(), columnHandle, accumulator));
                }
                if (!range.isHighUnbounded()) {
                    rangeConjuncts.add(toPredicate(columnName, range.isHighInclusive() ? "<=" : "<", range.getHighBoundedValue(), columnHandle, accumulator));
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(toPredicate(columnName, "=", getOnlyElement(singleValues), columnHandle, accumulator));
        }
        else if (singleValues.size() > 1) {
            for (Object value : singleValues) {
                bindValue(value, columnHandle, accumulator);
            }
            String values = Joiner.on(",").join(singleValues.stream().map(v ->
                    parameterValueToString(columnHandle.getColumnType(), v)).collect(Collectors.toList()));
            disjuncts.add(quote(columnName) + " IN (" + values + ")");
        }

        // Add nullability disjuncts
        checkState(!disjuncts.isEmpty());
        if (domain.isNullAllowed()) {
            disjuncts.add(quote(columnName) + " IS NULL");
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private String toPredicate(String columnName, String operator, Object value, ArrowColumnHandle columnHandle, List<TypeAndValue> accumulator)
    {
        bindValue(value, columnHandle, accumulator);
        return quote(columnName) + " " + operator + " " + parameterValueToString(columnHandle.getColumnType(), value);
    }
    private String quote(String name)
    {
        return "\"" + name + "\"";
    }

    private String quoteValue(String name)
    {
        return "'" + name + "'";
    }

    private void bindValue(Object value, ArrowColumnHandle columnHandle, List<TypeAndValue> accumulator)
    {
        Type type = columnHandle.getColumnType();
        accumulator.add(new TypeAndValue(type, value));
    }

    public static String convertLongToFloatString(Long value)
    {
        float floatFromIntBits = intBitsToFloat(toIntExact(value));
        return String.valueOf(floatFromIntBits);
    }

    private String parameterValueToString(Type type, Object value)
    {
        Class<?> javaType = type.getJavaType();
        if (type instanceof DateType && javaType == long.class) {
            return quoteValue(convertEpochToString((Long) value, type));
        }
        else if (type instanceof TimeType && javaType == long.class) {
            return quoteValue(convertEpochToString((Long) value, type));
        }
        else if (type instanceof TimestampType && javaType == long.class) {
            return quoteValue(convertEpochToString((Long) value, type));
        }
        else if (type instanceof RealType && javaType == long.class) {
            return convertLongToFloatString((Long) value);
        }
        else if (javaType == boolean.class || javaType == double.class || javaType == long.class) {
            return value.toString();
        }
        else if (javaType == Slice.class) {
            return quoteValue(((Slice) value).toStringUtf8());
        }
        else {
            return quoteValue(value.toString());
        }
    }
}
