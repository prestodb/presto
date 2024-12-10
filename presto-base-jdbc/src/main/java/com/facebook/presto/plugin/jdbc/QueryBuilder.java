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

import com.facebook.airlift.log.Logger;
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
import com.facebook.presto.plugin.jdbc.optimization.JdbcExpression;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
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
    private static final Logger log = Logger.get(QueryBuilder.class);

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
            Optional<List<ConnectorTableHandle>> joinPushdownTables,
            List<JdbcColumnHandle> columns,
            Map<String, String> columnExpressions,
            TupleDomain<ColumnHandle> tupleDomain,
            Optional<JdbcExpression> additionalPredicate)
            throws SQLException
    {
        boolean joinPushdownEnabled = checkIfJoinQuery(joinPushdownTables);
        if (joinPushdownEnabled) {
            return buildJoinSql(client, session, connection, joinPushdownTables.get(), columns, tupleDomain, additionalPredicate, true);
        }
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

        List<String> clauses = toConjuncts(columns, tupleDomain, accumulator, false);
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
        sql.append(String.format("/* %s : %s */", session.getUser(), session.getQueryId()));
        PreparedStatement statement = client.getPreparedStatement(session, connection, sql.toString());

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
            else if (typeAndValue.getType() instanceof CharType) {
                statement.setString(i + 1, ((Slice) typeAndValue.getValue()).toStringUtf8());
            }
            else {
                throw new UnsupportedOperationException("Can't handle type: " + typeAndValue.getType());
            }
        }
        // TODO This is just for debugging. Change to debug after testing
        log.info("Query is not Pushed down ");
        log.info("Normal query: " + sql);
        log.info("QueryBuilder.buildSql() :: Normal Query ", sql.toString());
        return statement;
    }

    private boolean checkIfJoinQuery(Optional<List<ConnectorTableHandle>> joinPushdownTables)
    {
        return (joinPushdownTables != null && joinPushdownTables.isPresent() && !joinPushdownTables.get().isEmpty());
    }

    private PreparedStatement buildJoinSql(
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            List<ConnectorTableHandle> joinTables,
            List<JdbcColumnHandle> columns,
            TupleDomain<ColumnHandle> tupleDomain,
            Optional<JdbcExpression> additionalPredicate,
            boolean joinPushdownEnabled)
            throws SQLException
    {
        StringBuilder sql = new StringBuilder();
        Map<String, String> tableReferencesAndTableAliases = new HashMap<>();

        List<String> selectColumns = getSelectColumns(columns);
        setTableReferences(joinTables, tableReferencesAndTableAliases);

        sql.append(getJoinProjection(selectColumns));
        sql.append(getTableReferences(tableReferencesAndTableAliases));

        List<TypeAndValue> accumulator = new ArrayList<>();

        List<String> clauses = toConjuncts(columns, tupleDomain, accumulator, joinPushdownEnabled);

        if (additionalPredicate.isPresent()) {
            clauses = ImmutableList.<String>builder()
                    .addAll(clauses)
                    .add(additionalPredicate.get().getExpression())
                    .build();
            accumulator.addAll(additionalPredicate.get().getBoundConstantValues().stream()
                    //see https://github.com/Ahana-Inc/prestodb/pull/144#discussion_r895353628 for details.
                    .map(constantExpression -> new TypeAndValue(constantExpression.getType(), constantExpression.getValue()))
                    .collect(ImmutableList.toImmutableList()));
        }
        if (!clauses.isEmpty()) {
            sql.append(" WHERE ");
            sql.append(String.join(" AND ", clauses));
        }
        sql.append(String.format("/* %s : %s */", session.getUser(), session.getQueryId()));

        PreparedStatement statement = client.getPreparedStatement(session, connection, sql.toString());

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
            else if (typeAndValue.getType() instanceof CharType) {
                statement.setString(i + 1, ((Slice) typeAndValue.getValue()).toStringUtf8());
            }
            else {
                throw new UnsupportedOperationException("Can't handle type: " + typeAndValue.getType());
            }
        }
        //TODO Change to debug after testing
        log.info("Query is Pushed down ");
        log.info("Join query: " + sql);
        log.info("QueryBuilder.buildJoinSql() :: Join Query ", sql.toString());
        return statement;
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
                validType instanceof CharType;
    }

    private List<String> toConjuncts(List<JdbcColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain, List<TypeAndValue> accumulator, boolean joinPushdownEnabled)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (JdbcColumnHandle column : columns) {
            Type type = column.getColumnType();
            if (isAcceptedType(type)) {
                Domain domain = tupleDomain.getDomains().get().get(column);
                if (domain != null) {
                    builder.add(toPredicate(column.getColumnName(), domain, column, accumulator, joinPushdownEnabled));
                }
            }
        }
        return builder.build();
    }

    private String toPredicate(String columnName, Domain domain, JdbcColumnHandle columnHandle, List<TypeAndValue> accumulator, boolean joinPushdownEnabled)
    {
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? getColumnIdentifier(columnName, columnHandle, joinPushdownEnabled) + " IS NULL" : ALWAYS_FALSE;
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? ALWAYS_TRUE : getColumnIdentifier(columnName, columnHandle, joinPushdownEnabled) + " IS NOT NULL";
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
                    rangeConjuncts.add(toPredicate(columnName, range.isLowInclusive() ? ">=" : ">", range.getLowBoundedValue(), columnHandle, accumulator, joinPushdownEnabled));
                }
                if (!range.isHighUnbounded()) {
                    rangeConjuncts.add(toPredicate(columnName, range.isHighInclusive() ? "<=" : "<", range.getHighBoundedValue(), columnHandle, accumulator, joinPushdownEnabled));
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(toPredicate(columnName, "=", getOnlyElement(singleValues), columnHandle, accumulator, joinPushdownEnabled));
        }
        else if (singleValues.size() > 1) {
            for (Object value : singleValues) {
                bindValue(value, columnHandle, accumulator);
            }
            String values = Joiner.on(",").join(nCopies(singleValues.size(), "?"));
            disjuncts.add(getColumnIdentifier(columnName, columnHandle, joinPushdownEnabled) + " IN (" + values + ")");
        }

        // Add nullability disjuncts
        checkState(!disjuncts.isEmpty());
        if (domain.isNullAllowed()) {
            disjuncts.add(getColumnIdentifier(columnName, columnHandle, joinPushdownEnabled) + " IS NULL");
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private String getColumnIdentifier(String columnName, JdbcColumnHandle columnHandle, boolean joinPushdownEnabled)
    {
        Optional<String> tableAlias = columnHandle.getTableAlias();
        if (joinPushdownEnabled && tableAlias.isPresent()) {
            return quote(tableAlias.get()) + "." + quote(columnName);
        }
        else {
            return quote(columnName);
        }
    }

    private String toPredicate(String columnName, String operator, Object value, JdbcColumnHandle columnHandle, List<TypeAndValue> accumulator, boolean joinPushdownEnabled)
    {
        bindValue(value, columnHandle, accumulator);
        return getColumnIdentifier(columnName, columnHandle, joinPushdownEnabled) + " " + operator + " ?";
    }

    private String quote(String name)
    {
        name = name.replace(quote, quote + quote);
        return quote + name + quote;
    }

    private static void bindValue(Object value, JdbcColumnHandle columnHandle, List<TypeAndValue> accumulator)
    {
        Type type = columnHandle.getColumnType();
        checkArgument(isAcceptedType(type), "Can't handle type: %s", type);
        accumulator.add(new TypeAndValue(type, value));
    }

    private List<String> getSelectColumns(List<JdbcColumnHandle> columns)
    {
        List<String> selectColumns = new ArrayList<>();
        for (JdbcColumnHandle columnHandle : columns) {
            String column = getColumnAssignment(columnHandle);
            if (null != column) {
                selectColumns.add(column);
            }
        }
        return selectColumns;
    }

    private String getJoinProjection(List<String> selectColumns)
    {
        return "SELECT " + String.join(", ", selectColumns) + " FROM ";
    }

    private String getTableReferences(Map<String, String> tableReferencesAndTableAliases)
    {
        return tableReferencesAndTableAliases.entrySet().stream()
                .map(entry -> entry.getValue() + " " + entry.getKey())
                .collect(Collectors.joining(", "));
    }

    private String getColumnAssignment(JdbcColumnHandle columnHandle)
    {
        String columnName = columnHandle.getColumnName();
        Optional<String> columnTableAlias = columnHandle.getTableAlias();
        return columnTableAlias.map(s -> quote(s) + "." + quote(columnName)).orElseGet(() -> quote(columnName));
    }

    private void setTableReferences(List<ConnectorTableHandle> joinTables, Map<String, String> tableReferencesAndTableAliases)
    {
        for (ConnectorTableHandle table : joinTables) {
            JdbcTableHandle tableHandle = (JdbcTableHandle) table;
            /*
               Schema name is null for connectors like MySQl, SingleStore etc.
               Such case we are taking catalog name.
             */
            String schemaName = (null != tableHandle.getSchemaName() && !tableHandle.getSchemaName().isEmpty()) ? tableHandle.getSchemaName() : tableHandle.getCatalogName();
            String tableName = tableHandle.getTableName();
            Optional<String> tableAlias = tableHandle.getTableAlias();
            String schemaTableName = quote(schemaName) + "." + quote(tableName);
            if (tableAlias.isPresent()) {
                String alias = tableAlias.get();
                tableReferencesAndTableAliases.put(quote(alias), schemaTableName);
            }
        }
    }
}
