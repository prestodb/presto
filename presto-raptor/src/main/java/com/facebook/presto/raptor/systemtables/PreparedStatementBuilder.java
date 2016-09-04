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
package com.facebook.presto.raptor.systemtables;

import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.raptor.util.DatabaseUtil.enableStreamingResults;
import static com.facebook.presto.raptor.util.UuidUtil.uuidToBytes;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.util.Collections.nCopies;
import static java.util.UUID.fromString;

public final class PreparedStatementBuilder
{
    private PreparedStatementBuilder() {}

    public static PreparedStatement create(
            Connection connection,
            String sql,
            List<String> columnNames,
            List<Type> types,
            Set<Integer> uuidColumnIndexes,
            TupleDomain<Integer> tupleDomain)
            throws SQLException
    {
        checkArgument(!isNullOrEmpty(sql), "sql is null or empty");

        List<ValueBuffer> bindValues = new ArrayList<>(256);
        sql += getWhereClause(tupleDomain, columnNames, types, uuidColumnIndexes, bindValues);

        PreparedStatement statement = connection.prepareStatement(sql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
        enableStreamingResults(statement);

        // bind values to statement
        int bindIndex = 1;
        for (ValueBuffer value : bindValues) {
            bindField(value, statement, bindIndex, uuidColumnIndexes.contains(value.getColumnIndex()));
            bindIndex++;
        }
        return statement;
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private static String getWhereClause(
            TupleDomain<Integer> tupleDomain,
            List<String> columnNames,
            List<Type> types,
            Set<Integer> uuidColumnIndexes,
            List<ValueBuffer> bindValues)
    {
        if (tupleDomain.isNone()) {
            return "";
        }

        ImmutableList.Builder<String> conjunctsBuilder = ImmutableList.builder();
        Map<Integer, Domain> domainMap = tupleDomain.getDomains().get();
        for (Map.Entry<Integer, Domain> entry : domainMap.entrySet()) {
            int index = entry.getKey();
            String columnName = columnNames.get(index);
            Type type = types.get(index);
            conjunctsBuilder.add(toPredicate(index, columnName, type, entry.getValue(), uuidColumnIndexes, bindValues));
        }
        List<String> conjuncts = conjunctsBuilder.build();

        if (conjuncts.isEmpty()) {
            return "";
        }
        StringBuilder where = new StringBuilder("WHERE ");
        return Joiner.on(" AND\n").appendTo(where, conjuncts).toString();
    }

    private static String toPredicate(
            int columnIndex,
            String columnName,
            Type type,
            Domain domain,
            Set<Integer> uuidColumnIndexes,
            List<ValueBuffer> bindValues)
    {
        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? "TRUE" : columnName + " IS NOT NULL";
        }
        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? columnName + " IS NULL" : "FALSE";
        }

        return domain.getValues().getValuesProcessor().transform(
                ranges -> {
                    // Add disjuncts for ranges
                    List<String> disjuncts = new ArrayList<>();
                    List<Object> singleValues = new ArrayList<>();

                    // Add disjuncts for ranges
                    for (Range range : ranges.getOrderedRanges()) {
                        checkState(!range.isAll()); // Already checked
                        if (range.isSingleValue()) {
                            singleValues.add(range.getLow().getValue());
                        }
                        else {
                            List<String> rangeConjuncts = new ArrayList<>();
                            if (!range.getLow().isLowerUnbounded()) {
                                Object bindValue = getBindValue(columnIndex, uuidColumnIndexes, range.getLow().getValue());
                                switch (range.getLow().getBound()) {
                                    case ABOVE:
                                        rangeConjuncts.add(toBindPredicate(columnName, ">"));
                                        bindValues.add(ValueBuffer.create(columnIndex, type, bindValue));
                                        break;
                                    case EXACTLY:
                                        rangeConjuncts.add(toBindPredicate(columnName, ">="));
                                        bindValues.add(ValueBuffer.create(columnIndex, type, bindValue));
                                        break;
                                    case BELOW:
                                        throw new VerifyException("Low Marker should never use BELOW bound");
                                    default:
                                        throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                                }
                            }
                            if (!range.getHigh().isUpperUnbounded()) {
                                Object bindValue = getBindValue(columnIndex, uuidColumnIndexes, range.getHigh().getValue());
                                switch (range.getHigh().getBound()) {
                                    case ABOVE:
                                        throw new VerifyException("High Marker should never use ABOVE bound");
                                    case EXACTLY:
                                        rangeConjuncts.add(toBindPredicate(columnName, "<="));
                                        bindValues.add(ValueBuffer.create(columnIndex, type, bindValue));
                                        break;
                                    case BELOW:
                                        rangeConjuncts.add(toBindPredicate(columnName, "<"));
                                        bindValues.add(ValueBuffer.create(columnIndex, type, bindValue));
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
                        disjuncts.add(toBindPredicate(columnName, "="));
                        bindValues.add(ValueBuffer.create(columnIndex, type, getBindValue(columnIndex, uuidColumnIndexes, getOnlyElement(singleValues))));
                    }
                    else if (singleValues.size() > 1) {
                        disjuncts.add(columnName + " IN (" + Joiner.on(",").join(nCopies(singleValues.size(), "?")) + ")");
                        for (Object singleValue : singleValues) {
                            bindValues.add(ValueBuffer.create(columnIndex, type, getBindValue(columnIndex, uuidColumnIndexes, singleValue)));
                        }
                    }

                    // Add nullability disjuncts
                    checkState(!disjuncts.isEmpty());
                    if (domain.isNullAllowed()) {
                        disjuncts.add(columnName + " IS NULL");
                    }

                    return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
                },

                discreteValues -> {
                    String values = Joiner.on(",").join(nCopies(discreteValues.getValues().size(), "?"));
                    String predicate = columnName + (discreteValues.isWhiteList() ? "" : " NOT") + " IN (" + values + ")";
                    for (Object value : discreteValues.getValues()) {
                        bindValues.add(ValueBuffer.create(columnIndex, type, getBindValue(columnIndex, uuidColumnIndexes, value)));
                    }
                    if (domain.isNullAllowed()) {
                        predicate = "(" + predicate + " OR " + columnName + " IS NULL)";
                    }
                    return predicate;
                },

                allOrNone -> {
                    throw new IllegalStateException("Case should not be reachable");
                });
    }

    private static Object getBindValue(int columnIndex, Set<Integer> uuidColumnIndexes, Object value)
    {
        if (uuidColumnIndexes.contains(columnIndex)) {
            return uuidToBytes(fromString(((Slice) value).toStringUtf8()));
        }
        return value;
    }

    private static String toBindPredicate(String columnName, String operator)
    {
        return format("%s %s ?", columnName, operator);
    }

    private static void bindField(ValueBuffer valueBuffer, PreparedStatement preparedStatement, int parameterIndex, boolean isUuid)
            throws SQLException
    {
        Type type = valueBuffer.getType();
        if (valueBuffer.isNull()) {
            preparedStatement.setNull(parameterIndex, typeToSqlType(type));
        }
        else if (type.getJavaType() == long.class) {
            preparedStatement.setLong(parameterIndex, valueBuffer.getLong());
        }
        else if (type.getJavaType() == double.class) {
            preparedStatement.setDouble(parameterIndex, valueBuffer.getDouble());
        }
        else if (type.getJavaType() == boolean.class) {
            preparedStatement.setBoolean(parameterIndex, valueBuffer.getBoolean());
        }
        else if (type.getJavaType() == Slice.class && isUuid) {
            preparedStatement.setBytes(parameterIndex, valueBuffer.getSlice().getBytes());
        }
        else if (type.getJavaType() == Slice.class) {
            preparedStatement.setString(parameterIndex, new String(valueBuffer.getSlice().getBytes()));
        }
        else {
            throw new IllegalArgumentException("Unknown Java type: " + type.getJavaType());
        }
    }

    private static int typeToSqlType(Type type)
    {
        if (type.equals(BIGINT)) {
            return Types.BIGINT;
        }
        if (type.equals(DOUBLE)) {
            return Types.DOUBLE;
        }
        if (type.equals(BOOLEAN)) {
            return Types.BOOLEAN;
        }
        if (isVarcharType(type)) {
            return Types.VARCHAR;
        }
        if (type.equals(VARBINARY)) {
            return Types.VARBINARY;
        }
        throw new IllegalArgumentException("Unknown type: " + type);
    }
}
