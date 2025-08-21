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
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
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
            List<ConnectorTableHandle> joinPushdownTables,
            List<JdbcColumnHandle> columns,
            Map<String, String> columnExpressions,
            TupleDomain<ColumnHandle> tupleDomain,
            Optional<JdbcExpression> additionalPredicate,
            Optional<String> tableAlias)
            throws SQLException
    {
        StringBuilder sql = new StringBuilder();
        buildSelectClause(columns, sql, columnExpressions);

        if (!joinPushdownTables.isEmpty()) {
            buildFromClause(sql, joinPushdownTables);
        }
        else {
            buildFromClause(catalog, schema, table, sql, tableAlias);
        }

        ImmutableList.Builder<TypeAndValue> accumulatorBuilder = ImmutableList.builder();
        List<String> clauses = toConjuncts(columns, tupleDomain, accumulatorBuilder);
        clauses = buildClauses(additionalPredicate, accumulatorBuilder, clauses);
        buildWhereClause(clauses, sql);

        sql.append(format("/* %s : %s */", session.getUser(), session.getQueryId()));
        PreparedStatement statement = client.getPreparedStatement(session, connection, sql.toString());
        List<TypeAndValue> accumulator = accumulatorBuilder.build();
        bindParams(accumulator, statement, client, session);

        return statement;
    }

    private void buildSelectClause(List<JdbcColumnHandle> columns, StringBuilder sql, Map<String, String> columnExpressions)
    {
        sql.append("SELECT ");
        sql.append(addColumns(columns, columnExpressions));
    }

    private void buildFromClause(String catalog, String schema, String table, StringBuilder sql, Optional<String> tableAlias)
    {
        sql.append(" FROM ");
        if (!isNullOrEmpty(catalog)) {
            sql.append(quote(catalog)).append('.');
        }
        if (!isNullOrEmpty(schema)) {
            sql.append(quote(schema)).append('.');
        }
        if (tableAlias.isPresent()) {
            String alias = tableAlias.get();
            sql.append(quote(table)).append(" ").append(quote(alias));
        }
        else {
            sql.append(quote(table));
        }
    }

    private void buildWhereClause(List<String> clauses, StringBuilder sql)
    {
        if (!clauses.isEmpty()) {
            sql.append(" WHERE ")
                    .append(String.join(" AND ", clauses));
        }
    }

    private static List<String> buildClauses(Optional<JdbcExpression> additionalPredicate, ImmutableList.Builder<TypeAndValue> accumulator, List<String> clauses)
    {
        if (additionalPredicate.isPresent()) {
            clauses = ImmutableList.<String>builder()
                    .addAll(clauses)
                    .add(additionalPredicate.get().getExpression())
                    .build();
            accumulator.addAll(additionalPredicate.get().getBoundConstantValues().stream()
                    .map(constantExpression -> new TypeAndValue(constantExpression.getType(), constantExpression.getValue()))
                    .collect(ImmutableList.toImmutableList()));
        }
        return clauses;
    }

    private void bindParams(List<TypeAndValue> accumulator, PreparedStatement statement, JdbcClient client, ConnectorSession session) throws SQLException
    {
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
    }

    private String addColumns(List<JdbcColumnHandle> columns, Map<String, String> columnExpressions)
    {
        if (columns.isEmpty()) {
            return "null";
        }

        return columns.stream()
                .map(jdbcColumnHandle -> {
                    String columnName = jdbcColumnHandle.getColumnName();
                    String columnAlias = getColumnIdentifier(jdbcColumnHandle);
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

    private List<String> toConjuncts(List<JdbcColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain, ImmutableList.Builder<TypeAndValue> accumulatorBuilder)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (JdbcColumnHandle column : columns) {
            Type type = column.getColumnType();
            if (isAcceptedType(type)) {
                Domain domain = tupleDomain.getDomains().get().get(column);
                if (domain != null) {
                    builder.add(toPredicate(column.getColumnName(), domain, column, accumulatorBuilder));
                }
            }
        }
        return builder.build();
    }

    private String toPredicate(String columnName, Domain domain, JdbcColumnHandle columnHandle, ImmutableList.Builder<TypeAndValue> accumulatorBuilder)
    {
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? getColumnIdentifier(columnHandle) + " IS NULL" : ALWAYS_FALSE;
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? ALWAYS_TRUE : getColumnIdentifier(columnHandle) + " IS NOT NULL";
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
                    rangeConjuncts.add(toPredicate(columnName, range.isLowInclusive() ? ">=" : ">", range.getLowBoundedValue(), columnHandle, accumulatorBuilder));
                }
                if (!range.isHighUnbounded()) {
                    rangeConjuncts.add(toPredicate(columnName, range.isHighInclusive() ? "<=" : "<", range.getHighBoundedValue(), columnHandle, accumulatorBuilder));
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(toPredicate(columnName, "=", getOnlyElement(singleValues), columnHandle, accumulatorBuilder));
        }
        else if (singleValues.size() > 1) {
            for (Object value : singleValues) {
                bindValue(value, columnHandle, accumulatorBuilder);
            }
            String values = Joiner.on(",").join(nCopies(singleValues.size(), "?"));
            disjuncts.add(getColumnIdentifier(columnHandle) + " IN (" + values + ")");
        }

        // Add nullability disjuncts
        checkState(!disjuncts.isEmpty());
        if (domain.isNullAllowed()) {
            disjuncts.add(getColumnIdentifier(columnHandle) + " IS NULL");
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private String getColumnIdentifier(JdbcColumnHandle columnHandle)
    {
        return columnHandle.getTableAlias().map(s -> quote(s) + "." + quote(columnHandle.getColumnName())).orElseGet(() -> quote(columnHandle.getColumnName()));
    }

    private String toPredicate(String columnName, String operator, Object value, JdbcColumnHandle columnHandle, ImmutableList.Builder<TypeAndValue> accumulatorBuilder)
    {
        bindValue(value, columnHandle, accumulatorBuilder);
        return getColumnIdentifier(columnHandle) + " " + operator + " ?";
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

    private static void bindValue(Object value, JdbcColumnHandle columnHandle, ImmutableList.Builder<TypeAndValue> accumulatorBuilder)
    {
        Type type = columnHandle.getColumnType();
        accumulatorBuilder.add(new TypeAndValue(type, value));
    }

    private void buildFromClause(StringBuilder sql, List<ConnectorTableHandle> joinTables)
    {
        sql.append(" FROM");
        for (ConnectorTableHandle table : joinTables) {
            JdbcTableHandle tableHandle = (JdbcTableHandle) table;
            /*
               Schema name is null for connectors like MySQL, SingleStore etc.
               In such cases we are taking catalog name.
             */
            String schemaName = (null != tableHandle.getSchemaName() && !tableHandle.getSchemaName().isEmpty()) ? tableHandle.getSchemaName() : tableHandle.getCatalogName();
            String tableName = tableHandle.getTableName();
            Optional<String> tableAlias = tableHandle.getTableAlias();
            String schemaTableName = quote(schemaName) + "." + quote(tableName);

            if (tableAlias.isPresent()) {
                String alias = tableAlias.get();
                sql.append(" ").append(schemaTableName).append(" ").append(quote(alias));
            }
            else {
                sql.append(" ").append(schemaTableName);
            }
            sql.append(",");
        }
        // Remove the last comma
        sql.replace(sql.length() - 1, sql.length(), "");
    }
}
