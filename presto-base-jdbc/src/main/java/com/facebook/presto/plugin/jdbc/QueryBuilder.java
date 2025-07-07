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
import com.facebook.presto.common.type.TimeWithTimeZoneType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.mapping.WriteFunction;
import com.facebook.presto.plugin.jdbc.mapping.functions.BooleanWriteFunction;
import com.facebook.presto.plugin.jdbc.mapping.functions.DoubleWriteFunction;
import com.facebook.presto.plugin.jdbc.mapping.functions.LongWriteFunction;
import com.facebook.presto.plugin.jdbc.mapping.functions.ObjectWriteFunction;
import com.facebook.presto.plugin.jdbc.mapping.functions.SliceWriteFunction;
import com.facebook.presto.plugin.jdbc.optimization.JdbcExpression;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.plugin.jdbc.mapping.StandardColumnMappings.getWriteMappingForAccumulators;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class QueryBuilder
{
    // not all databases support booleans, so use 1=1 and 1=0 instead
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
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            String catalog,
            String schema,
            String table,
            List<JdbcColumnHandle> columns,
            Map<String, String> columnExpressions,
            TupleDomain<ColumnHandle> tupleDomain,
            Optional<JdbcExpression> additionalPredicate)
            throws SQLException
    {
        StringBuilder sql = new StringBuilder();

        sql.append("SELECT ");
        sql.append(addColumns(columns, columnExpressions));

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
        sql.append(format("/* %s : %s */", session.getUser(), session.getQueryId()));
        PreparedStatement statement = client.getPreparedStatement(session, connection, sql.toString());

        for (int i = 0; i < accumulator.size(); i++) {
            TypeAndValue typeAndValue = accumulator.get(i);
            int parameterIndex = i + 1;
            Type type = typeAndValue.getType();
            WriteFunction writeFunction = getWriteMappingForAccumulators(type)
                    .orElseThrow(() -> new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName()))
                    .getWriteFunction();
            Class<?> javaType = type.getJavaType();
            Object value = typeAndValue.getValue();
            if (javaType == boolean.class) {
                ((BooleanWriteFunction) writeFunction).set(statement, parameterIndex, (boolean) value);
            }
            else if (javaType == double.class) {
                ((DoubleWriteFunction) writeFunction).set(statement, parameterIndex, (double) value);
            }
            else if (javaType == long.class) {
                ((LongWriteFunction) writeFunction).set(statement, parameterIndex, (long) value);
            }
            else if (javaType == Slice.class) {
                ((SliceWriteFunction) writeFunction).set(statement, parameterIndex, (Slice) value);
            }
            else {
                try {
                    ((ObjectWriteFunction) writeFunction).set(statement, parameterIndex, value);
                }
                catch (SQLException e) {
                    throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
                }
            }
        }
        return statement;
    }

    public PreparedStatement buildSql(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            String catalog,
            String schema,
            String table,
            List<JdbcColumnHandle> columns,
            TupleDomain<ColumnHandle> tupleDomain,
            Optional<JdbcExpression> additionalPredicate)
            throws SQLException
    {
        return buildSql(client, session, connection, catalog, schema, table, columns, ImmutableMap.of(), tupleDomain, additionalPredicate);
    }

    private String addColumns(List<JdbcColumnHandle> columns, Map<String, String> columnExpressions)
    {
        if (columns.isEmpty()) {
            return "null";
        }

        return columns.stream()
                .map(jdbcColumnHandle -> {
                    String columnName = jdbcColumnHandle.getColumnName();
                    String columnAlias = quote(columnName);
                    String expression = columnExpressions.get(columnName);
                    if (expression == null) {
                        return columnAlias;
                    }
                    return format("%s AS %s", expression, columnAlias);
                })
                .collect(joining(", "));
    }

    protected boolean isAcceptedType(Type type)
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
                validType instanceof CharType;
    }

    private List<String> toConjuncts(List<JdbcColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain, List<TypeAndValue> accumulator)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (JdbcColumnHandle column : columns) {
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

    private String toPredicate(String columnName, Domain domain, JdbcColumnHandle columnHandle, List<TypeAndValue> accumulator)
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

    private String toPredicate(String columnName, String operator, Object value, JdbcColumnHandle columnHandle, List<TypeAndValue> accumulator)
    {
        bindValue(value, columnHandle, accumulator);
        return quote(columnName) + " " + operator + " ?";
    }

    private String quote(String name)
    {
        return quote(quote, name);
    }

    public static String quote(String identifierQuote, String name)
    {
        name = name.replace(identifierQuote, identifierQuote + identifierQuote);
        return identifierQuote + name + identifierQuote;
    }

    private static void bindValue(Object value, JdbcColumnHandle columnHandle, List<TypeAndValue> accumulator)
    {
        Type type = columnHandle.getColumnType();
        accumulator.add(new TypeAndValue(type, value));
    }
}
