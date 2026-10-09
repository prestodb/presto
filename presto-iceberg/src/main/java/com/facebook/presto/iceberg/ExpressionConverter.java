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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.UuidType;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.iceberg.expressions.Expression;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.common.predicate.Marker.Bound.ABOVE;
import static com.facebook.presto.common.predicate.Marker.Bound.BELOW;
import static com.facebook.presto.common.predicate.Marker.Bound.EXACTLY;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.iceberg.IcebergColumnHandle.getPushedDownSubfield;
import static com.facebook.presto.iceberg.IcebergColumnHandle.isPushedDownSubfield;
import static com.facebook.presto.parquet.ParquetTypeUtils.columnPathFromSubfield;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.or;

public final class ExpressionConverter
{
    private ExpressionConverter() {}

    public static Expression toIcebergExpression(TupleDomain<IcebergColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll()) {
            return alwaysTrue();
        }
        if (!tupleDomain.getDomains().isPresent()) {
            return alwaysFalse();
        }
        Map<IcebergColumnHandle, Domain> domainMap = tupleDomain.getDomains().get();
        Expression expression = alwaysTrue();
        for (Map.Entry<IcebergColumnHandle, Domain> entry : domainMap.entrySet()) {
            IcebergColumnHandle columnHandle = entry.getKey();
            Domain domain = entry.getValue();
            String columnName = columnHandle.getName();

            if (isPushedDownSubfield(columnHandle)) {
                Subfield pushedDownSubfield = getPushedDownSubfield(columnHandle);
                columnName = pushdownColumnNameForSubfield(pushedDownSubfield);
            }
            expression = and(expression, toIcebergExpression(columnName, columnHandle.getType(), domain));
        }
        return expression;
    }

    public static String pushdownColumnNameForSubfield(Subfield subfield)
    {
        return String.join(".", columnPathFromSubfield(subfield));
    }

    private static Expression toIcebergExpression(String columnName, Type type, Domain domain)
    {
        if (domain.isAll()) {
            return alwaysTrue();
        }
        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? isNull(columnName) : alwaysFalse();
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? alwaysTrue() : not(isNull(columnName));
        }

        // Skip structural types. TODO: Evaluate Apache Iceberg's support for predicate on structural types
        if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            return alwaysTrue();
        }

        ValueSet domainValues = domain.getValues();
        // Each range is built as a self-contained predicate before it is OR'd into the result.
        // Applying a bound to the accumulator would also constrain isNull and the earlier ranges.
        Expression expression = null;
        if (domain.isNullAllowed()) {
            expression = isNull(columnName);
        }

        if (domainValues instanceof SortedRangeSet) {
            List<Range> orderedRanges = ((SortedRangeSet) domainValues).getOrderedRanges();
            expression = firstNonNull(expression, alwaysFalse());

            ImmutableList.Builder<Object> equalityValuesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Expression> rangeExprsBuilder = ImmutableList.builder();
            for (Range range : orderedRanges) {
                if (range.isSingleValue()) {
                    equalityValuesBuilder.add(getIcebergLiteralValue(type, range.getLow()));
                }
                else {
                    rangeToExpression(columnName, type, range).ifPresent(rangeExprsBuilder::add);
                }
            }

            List<Object> equalityValues = equalityValuesBuilder.build();
            if (!equalityValues.isEmpty()) {
                // Build a balanced OR tree of equal() predicates (depth O(log N)).
                // Iceberg's in() throws NullPointerException when the partition value is null
                // (partition evolution leaves null partition entries for pre-evolution files).
                // Individual equal() predicates use null-safe comparators and are safe.
                ImmutableList.Builder<Expression> equalExprs = ImmutableList.builder();
                for (Object v : equalityValues) {
                    equalExprs.add(equal(columnName, v));
                }
                expression = or(expression, buildOrTree(equalExprs.build()));
            }

            List<Expression> rangeExprs = rangeExprsBuilder.build();
            if (!rangeExprs.isEmpty()) {
                expression = or(expression, buildOrTree(rangeExprs));
            }
            return expression;
        }

        throw new VerifyException("Did not expect a domain value set other than SortedRangeSet but got " + domainValues.getClass().getSimpleName());
    }

    // Build a balanced binary OR tree with depth O(log N) instead of a left-skewed chain of
    // depth O(N), to avoid stack overflow in ExpressionVisitors.visit() for large collections.
    private static Expression buildOrTree(List<Expression> exprs)
    {
        checkArgument(!exprs.isEmpty(), "exprs must not be empty");
        if (exprs.size() == 1) {
            return exprs.get(0);
        }
        int mid = exprs.size() / 2;
        return or(buildOrTree(exprs.subList(0, mid)), buildOrTree(exprs.subList(mid, exprs.size())));
    }

    private static Optional<Expression> rangeToExpression(String columnName, Type type, Range range)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        Marker.Bound lowBound = low.getBound();
        Marker.Bound highBound = high.getBound();

        if (lowBound == EXACTLY && highBound == EXACTLY) {
            return Optional.of(and(
                    greaterThanOrEqual(columnName, getIcebergLiteralValue(type, low)),
                    lessThanOrEqual(columnName, getIcebergLiteralValue(type, high))));
        }

        Expression rangeExpr = null;
        if (lowBound == EXACTLY && low.getValueBlock().isPresent()) {
            rangeExpr = greaterThanOrEqual(columnName, getIcebergLiteralValue(type, low));
        }
        else if (lowBound == ABOVE && low.getValueBlock().isPresent()) {
            rangeExpr = greaterThan(columnName, getIcebergLiteralValue(type, low));
        }

        if (highBound == EXACTLY && high.getValueBlock().isPresent()) {
            Expression upperPred = lessThanOrEqual(columnName, getIcebergLiteralValue(type, high));
            rangeExpr = rangeExpr != null ? and(rangeExpr, upperPred) : upperPred;
        }
        else if (highBound == BELOW && high.getValueBlock().isPresent()) {
            Expression upperPred = lessThan(columnName, getIcebergLiteralValue(type, high));
            rangeExpr = rangeExpr != null ? and(rangeExpr, upperPred) : upperPred;
        }

        return Optional.ofNullable(rangeExpr);
    }

    private static Object getIcebergLiteralValue(Type type, Marker marker)
    {
        if (type instanceof IntegerType) {
            return toIntExact((long) marker.getValue());
        }

        if (type instanceof RealType) {
            return intBitsToFloat(toIntExact((long) marker.getValue()));
        }

        // TODO: Remove this conversion once we move to next iceberg version
        if (type instanceof DateType) {
            return toIntExact(((Long) marker.getValue()));
        }

        if (type instanceof TimestampType || type instanceof TimeType) {
            return MILLISECONDS.toMicros((Long) marker.getValue());
        }

        if (type instanceof TimestampWithTimeZoneType) {
            return MILLISECONDS.toMicros(unpackMillisUtc((Long) marker.getValue()));
        }

        if (type instanceof VarcharType) {
            return ((Slice) marker.getValue()).toStringUtf8();
        }

        if (type instanceof VarbinaryType) {
            return ByteBuffer.wrap(((Slice) marker.getValue()).getBytes());
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            Object value = requireNonNull(marker.getValue(), "The value of the marker must be non-null");
            if (Decimals.isShortDecimal(decimalType)) {
                checkArgument(value instanceof Long, "A short decimal should be represented by a Long value but was %s", value.getClass().getName());
                return BigDecimal.valueOf((long) value).movePointLeft(decimalType.getScale());
            }
            checkArgument(value instanceof Slice, "A long decimal should be represented by a Slice value but was %s", value.getClass().getName());
            return new BigDecimal(Decimals.decodeUnscaledValue((Slice) value), decimalType.getScale());
        }

        if (type instanceof UuidType) {
            UuidType uuidType = (UuidType) type;
            return marker.getValueBlock()
                    .map(block -> UUID.fromString((String) uuidType.getObjectValue(null, block, 0)))
                    .orElseThrow(NullPointerException::new);
        }
        return marker.getValue();
    }
}
