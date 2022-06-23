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
package com.facebook.presto.hive;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarcharType;
import com.google.common.base.Joiner;
import io.airlift.slice.Slice;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.Builder;
import static com.google.common.collect.ImmutableList.builder;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.stream.Collectors.joining;
import static org.joda.time.chrono.ISOChronology.getInstanceUTC;
import static org.joda.time.format.ISODateTimeFormat.date;

/**
 * S3 Select uses Ion SQL++ query language. This class is used to construct a valid Ion SQL++ query
 * to be evaluated with S3 Select on an S3 object.
 */
public class IonSqlQueryBuilder
{
    private static final DateTimeFormatter FORMATTER = date().withChronology(getInstanceUTC());
    private static final String DATA_SOURCE = "S3Object s";
    private final TypeManager typeManager;

    public IonSqlQueryBuilder(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public String buildSql(List<HiveColumnHandle> columns, TupleDomain<HiveColumnHandle> tupleDomain)
    {
        StringBuilder sql = new StringBuilder("SELECT ");

        if (columns.isEmpty()) {
            sql.append("' '");
        }
        else {
            String columnNames = columns.stream()
                    .map(column -> format("s._%d", column.getHiveColumnIndex() + 1))
                    .collect(joining(", "));
            sql.append(columnNames);
        }

        sql.append(" FROM ");
        sql.append(DATA_SOURCE);

        List<String> clauses = toConjuncts(columns, tupleDomain);
        if (!clauses.isEmpty()) {
            sql.append(" WHERE ")
                    .append(Joiner.on(" AND ").join(clauses));
        }

        return sql.toString();
    }

    private List<String> toConjuncts(List<HiveColumnHandle> columns, TupleDomain<HiveColumnHandle> tupleDomain)
    {
        Builder<String> builder = builder();
        for (HiveColumnHandle column : columns) {
            Type type = column.getHiveType().getType(typeManager);
            if (tupleDomain.getDomains().isPresent() && isSupported(type)) {
                Domain domain = tupleDomain.getDomains().get().get(column);
                if (domain != null) {
                    builder.add(toPredicate(domain, type, column.getHiveColumnIndex()));
                }
            }
        }
        return builder.build();
    }

    private static boolean isSupported(Type type)
    {
        Type validType = requireNonNull(type, "type is null");
        return validType.equals(BIGINT) ||
                validType.equals(TINYINT) ||
                validType.equals(SMALLINT) ||
                validType.equals(INTEGER) ||
                validType instanceof DecimalType ||
                validType.equals(BOOLEAN) ||
                validType.equals(DATE) ||
                validType instanceof VarcharType;
    }

    private String toPredicate(Domain domain, Type type, int position)
    {
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");

        if (domain.getValues().isNone()) {
            if (domain.isNullAllowed()) {
                return format("s._%d", position + 1) + " = '' ";
            }
            return "FALSE";
        }

        if (domain.getValues().isAll()) {
            if (domain.isNullAllowed()) {
                return "TRUE";
            }
            return format("s._%d", position + 1) + " <> '' ";
        }

        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll());
            if (range.isSingleValue()) {
                singleValues.add(range.getSingleValue());
                continue;
            }
            List<String> rangeConjuncts = new ArrayList<>();
            if (!range.isLowUnbounded()) {
                rangeConjuncts.add(toPredicate(range.isLowInclusive() ? ">=" : ">", range.getLowBoundedValue(), type, position));
            }
            if (!range.isHighUnbounded()) {
                rangeConjuncts.add(toPredicate(range.isHighInclusive() ? "<=" : "<", range.getHighBoundedValue(), type, position));
            }
            // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
            checkState(!rangeConjuncts.isEmpty());
            disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(toPredicate("=", getOnlyElement(singleValues), type, position));
        }
        else if (singleValues.size() > 1) {
            List<String> values = new ArrayList<>();
            for (Object value : singleValues) {
                checkType(type);
                values.add(valueToQuery(type, value));
            }
            disjuncts.add(createColumn(type, position) + " IN (" + Joiner.on(",").join(values) + ")");
        }

        // Add nullability disjuncts
        checkState(!disjuncts.isEmpty());
        if (domain.isNullAllowed()) {
            disjuncts.add(format("s._%d", position + 1) + " = '' ");
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private String toPredicate(String operator, Object value, Type type, int position)
    {
        checkType(type);

        return format("%s %s %s", createColumn(type, position), operator, valueToQuery(type, value));
    }

    private static void checkType(Type type)
    {
        checkArgument(isSupported(type), "Type not supported: %s", type);
    }

    private static String valueToQuery(Type type, Object value)
    {
        if (type.equals(BIGINT)) {
            return String.valueOf(((Number) value).longValue());
        }
        if (type.equals(INTEGER)) {
            return String.valueOf(((Number) value).intValue());
        }
        if (type.equals(SMALLINT)) {
            return String.valueOf(((Number) value).shortValue());
        }
        if (type.equals(TINYINT)) {
            return String.valueOf(((Number) value).byteValue());
        }
        if (type.equals(BOOLEAN)) {
            return String.valueOf(value);
        }
        if (type.equals(DATE)) {
            return "`" + FORMATTER.print(DAYS.toMillis((long) value)) + "`";
        }
        if (type.equals(VarcharType.VARCHAR)) {
            return "'" + ((Slice) value).toStringUtf8() + "'";
        }
        if (type instanceof DecimalType) {
            if (Decimals.isLongDecimal(type)) {
                return Decimals.toString((Slice) value, ((DecimalType) type).getScale());
            }
            return Decimals.toString((long) value, ((DecimalType) type).getScale());
        }
        return "'" + ((Slice) value).toStringUtf8() + "'";
    }

    private String createColumn(Type type, int position)
    {
        String column = format("s._%d", position + 1);

        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            return formatPredicate(column, "INT");
        }
        if (type.equals(BOOLEAN)) {
            return formatPredicate(column, "BOOL");
        }
        if (type.equals(DATE)) {
            return formatPredicate(column, "TIMESTAMP");
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return formatPredicate(column, format("DECIMAL(%s,%s)", decimalType.getPrecision(), decimalType.getScale()));
        }
        return column;
    }

    private String formatPredicate(String column, String type)
    {
        return format("case %s when '' then null else CAST(%s AS %s) end", column, column, type);
    }
}
