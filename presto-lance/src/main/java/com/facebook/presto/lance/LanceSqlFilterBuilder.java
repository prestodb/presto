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
package com.facebook.presto.lance;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.toIntExact;

public final class LanceSqlFilterBuilder
{
    private static final int MAX_RANGES_PER_COLUMN = 100;
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private LanceSqlFilterBuilder() {}

    public static Optional<String> buildFilter(TupleDomain<ColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll()) {
            return Optional.empty();
        }
        if (tupleDomain.isNone()) {
            return Optional.of("false");
        }

        Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().get();
        ImmutableList.Builder<String> conjuncts = ImmutableList.builder();

        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            LanceColumnHandle column = (LanceColumnHandle) entry.getKey();
            Domain domain = entry.getValue();
            Optional<String> predicate = buildColumnPredicate(column, domain);
            predicate.ifPresent(conjuncts::add);
        }

        List<String> parts = conjuncts.build();
        if (parts.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(String.join(" AND ", parts));
    }

    /**
     * Extract column names referenced by the filter predicates.
     * Used to determine filter projection columns (columns needed for
     * filter evaluation but not in query output).
     */
    public static List<String> extractFilterColumnNames(TupleDomain<ColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll() || tupleDomain.isNone()) {
            return ImmutableList.of();
        }
        return tupleDomain.getDomains().get().keySet().stream()
                .map(LanceColumnHandle.class::cast)
                .map(LanceColumnHandle::getColumnName)
                .collect(toImmutableList());
    }

    private static Optional<String> buildColumnPredicate(LanceColumnHandle column, Domain domain)
    {
        String quotedName = quoteColumnName(column.getColumnName());
        Type type = column.getColumnType();

        if (domain.isAll()) {
            return Optional.empty();
        }

        if (domain.getValues().isNone()) {
            if (domain.isNullAllowed()) {
                return Optional.of(quotedName + " IS NULL");
            }
            // Domain matches nothing — return "false" to filter all rows
            return Optional.of("false");
        }

        SortedRangeSet rangeSet = (SortedRangeSet) domain.getValues();
        List<Range> ranges = rangeSet.getOrderedRanges();

        if (ranges.size() > MAX_RANGES_PER_COLUMN) {
            return Optional.empty();
        }

        ImmutableList.Builder<String> disjuncts = ImmutableList.builder();

        // Check if all ranges are single values for IN optimization
        boolean allSingleValues = ranges.stream().allMatch(Range::isSingleValue);

        if (allSingleValues && ranges.size() > 1) {
            ImmutableList.Builder<String> values = ImmutableList.builder();
            for (Range range : ranges) {
                Optional<String> literal = toLiteral(type, range.getSingleValue());
                if (!literal.isPresent()) {
                    return Optional.empty();
                }
                values.add(literal.get());
            }
            disjuncts.add(quotedName + " IN (" + String.join(", ", values.build()) + ")");
        }
        else {
            for (Range range : ranges) {
                Optional<String> rangeExpr = buildRangeExpression(quotedName, type, range);
                if (!rangeExpr.isPresent()) {
                    return Optional.empty();
                }
                disjuncts.add(rangeExpr.get());
            }
        }

        List<String> disjunctList = disjuncts.build();
        String valuesPredicate;
        if (disjunctList.size() == 1) {
            valuesPredicate = disjunctList.get(0);
        }
        else {
            valuesPredicate = "(" + String.join(" OR ", disjunctList) + ")";
        }

        if (domain.isNullAllowed()) {
            return Optional.of("(" + valuesPredicate + " OR " + quotedName + " IS NULL)");
        }
        return Optional.of(valuesPredicate);
    }

    private static Optional<String> buildRangeExpression(String quotedName, Type type, Range range)
    {
        if (range.isSingleValue()) {
            Optional<String> literal = toLiteral(type, range.getSingleValue());
            return literal.map(l -> quotedName + " = " + l);
        }

        ImmutableList.Builder<String> parts = ImmutableList.builder();

        if (!range.getLow().isLowerUnbounded()) {
            Optional<String> literal = toLiteral(type, range.getLow().getValue());
            if (!literal.isPresent()) {
                return Optional.empty();
            }
            String op = (range.getLow().getBound() == Marker.Bound.ABOVE) ? " > " : " >= ";
            parts.add(quotedName + op + literal.get());
        }

        if (!range.getHigh().isUpperUnbounded()) {
            Optional<String> literal = toLiteral(type, range.getHigh().getValue());
            if (!literal.isPresent()) {
                return Optional.empty();
            }
            String op = (range.getHigh().getBound() == Marker.Bound.BELOW) ? " < " : " <= ";
            parts.add(quotedName + op + literal.get());
        }

        List<String> partList = parts.build();
        if (partList.isEmpty()) {
            return Optional.of("true");
        }

        // Parenthesize multi-part range expressions for unambiguous precedence
        if (partList.size() > 1) {
            return Optional.of("(" + String.join(" AND ", partList) + ")");
        }
        return Optional.of(partList.get(0));
    }

    private static Optional<String> toLiteral(Type type, Object value)
    {
        if (type instanceof BooleanType) {
            return Optional.of(((Boolean) value) ? "true" : "false");
        }
        if (type instanceof TinyintType || type instanceof SmallintType
                || type instanceof IntegerType || type instanceof BigintType) {
            return Optional.of(String.valueOf((long) value));
        }
        if (type instanceof RealType) {
            float floatVal = Float.intBitsToFloat(toIntExact((long) value));
            return Optional.of(String.valueOf(floatVal));
        }
        if (type instanceof DoubleType) {
            return Optional.of(String.valueOf((double) value));
        }
        if (type instanceof VarcharType) {
            String strVal = ((Slice) value).toStringUtf8();
            return Optional.of("'" + strVal.replace("'", "''") + "'");
        }
        if (type instanceof DateType) {
            long daysSinceEpoch = (long) value;
            LocalDate date = LocalDate.ofEpochDay(daysSinceEpoch);
            return Optional.of("date '" + date + "'");
        }
        if (type instanceof TimestampType) {
            long micros = (long) value;
            Instant instant = Instant.ofEpochSecond(
                    micros / 1_000_000,
                    (micros % 1_000_000) * 1000);
            String formatted = TIMESTAMP_FORMATTER.format(instant.atOffset(ZoneOffset.UTC));
            return Optional.of("timestamp '" + formatted + "'");
        }
        // Unsupported type — skip pushdown for this column
        return Optional.empty();
    }

    private static String quoteColumnName(String columnName)
    {
        return "`" + columnName + "`";
    }
}
