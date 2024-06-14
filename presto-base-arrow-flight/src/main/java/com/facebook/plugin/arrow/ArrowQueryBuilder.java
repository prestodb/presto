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
package com.facebook.plugin.arrow;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class ArrowQueryBuilder
{
    // not all databases support booleans, so use 1=1 and 1=0 instead
    private static final String ALWAYS_TRUE = "1=1";
    private static final String ALWAYS_FALSE = "1=0";
    private final String quote;
    private final String identifierQuote;

    private String addColumnExpression(List<ArrowColumnHandle> columns, Map<String, String> columnExpressions)
    {
        if (columns.isEmpty()) {
            return "null";
        }

        return columns.stream()
                .map(jdbcColumnHandle -> {
                    String columnAlias = quote(jdbcColumnHandle.getColumnName());
                    String expression = columnExpressions.get(jdbcColumnHandle.getColumnName());
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
//                validType.equals(BLOBType.BLOB) ||
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
//                validType instanceof CLOBType ||
                validType instanceof CharType;
    }
    private List<String> toConjuncts(List<ArrowColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain, List<TypeAndValue> accumulator)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (ArrowColumnHandle column : columns) {
            Type type = column.getColumnType();
            //Todo handle tupleDomain null
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

    //TODO check this method required
    private String quote(String name)
    {
        return identifierQuote + name + identifierQuote;
    }

    private String quoteValue(String name)
    {
        name = name.replace(quote, quote + quote);
        return quote + name + quote;
    }

    private void bindValue(Object value, ArrowColumnHandle columnHandle, List<TypeAndValue> accumulator)
    {
        Type type = columnHandle.getColumnType();
        accumulator.add(new TypeAndValue(type, value, columnHandle.getArrowTypeHandle()));
    }

    private String parameterValueToString(Type type, Object value)
    {
        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            return value.toString();
        }
        else if (javaType == double.class) {
            return value.toString();
        }
        else if (javaType == long.class) {
            return value.toString();
        }
        else if (javaType == Slice.class) {
            // TODO How to write varbinary value in SQL string
            return quoteValue(((Slice) value).toStringUtf8());
        }
        else {
            //no inspection, unchecked raw types
            return quoteValue(value.toString());
        }
    }

    protected static class TypeAndValue
    {
        private final Type type;
        private final Object value;
        private final ArrowTypeHandle typeHandle;

        public TypeAndValue(Type type, Object value, ArrowTypeHandle typeHandle)
        {
            this.type = requireNonNull(type, "type is null");
            this.value = requireNonNull(value, "value is null");
            this.typeHandle = typeHandle;
        }

        public Type getType()
        {
            return type;
        }

        public Object getValue()
        {
            return value;
        }

        public ArrowTypeHandle getTypeHandle()
        {
            return typeHandle;
        }
    }

    public ArrowQueryBuilder(String quote, String identifierQuote)
    {
        this.quote = requireNonNull(quote, "quote is null");
        this.identifierQuote = requireNonNull(identifierQuote, "identifierQuote is null");
    }

    //TODO arrow rename this method
    public String buildSql(
            String schema,
            String table,
            List<ArrowColumnHandle> columns,
            Map<String, String> columnExpressions,
            TupleDomain<ColumnHandle> tupleDomain,
            Optional<ArrowExpression> additionalPredicate)
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

        if (tupleDomain != null) {
            List<String> clauses = toConjuncts(columns, tupleDomain, accumulator);
            if (additionalPredicate.isPresent()) {
                clauses = ImmutableList.<String>builder()
                        .addAll(clauses)
                        .add(additionalPredicate.get().getExpression())
                        .build();
                accumulator.addAll(additionalPredicate.get().getBoundConstantValues().stream()
                        //see https://github.com/Ahana-Inc/prestodb/pull/144#discussion_r895353628 for details.
                        .map(constantExpression -> new TypeAndValue(constantExpression.getType(), constantExpression.getValue(), null))
                        .collect(ImmutableList.toImmutableList()));
            }
            if (!clauses.isEmpty()) {
                sql.append(" WHERE ")
                        .append(Joiner.on(" AND ").join(clauses));
            }
        }

        return sql.toString();
    }
}
