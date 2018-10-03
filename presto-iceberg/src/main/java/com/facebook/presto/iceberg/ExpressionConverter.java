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

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.iceberg.type.TypeConveter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.EquatableValueSet;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.DateTimeEncoding;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeWithTimeZoneType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableMap;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.expressions.And;
import com.netflix.iceberg.expressions.BoundReference;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.expressions.NamedReference;
import com.netflix.iceberg.expressions.Not;
import com.netflix.iceberg.expressions.Or;
import com.netflix.iceberg.expressions.Predicate;
import com.netflix.iceberg.expressions.Reference;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.facebook.presto.spi.predicate.Domain.create;
import static com.facebook.presto.spi.predicate.Marker.Bound.ABOVE;
import static com.facebook.presto.spi.predicate.Marker.Bound.BELOW;
import static com.facebook.presto.spi.predicate.Marker.Bound.EXACTLY;
import static com.facebook.presto.spi.predicate.ValueSet.ofRanges;
import static com.facebook.presto.spi.type.Decimals.MAX_SHORT_PRECISION;
import static com.facebook.presto.spi.type.Decimals.encodeScaledValue;
import static com.facebook.presto.spi.type.StandardTypes.TIME;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.StandardTypes.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.StandardTypes.VARBINARY;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.netflix.iceberg.expressions.Expressions.and;
import static com.netflix.iceberg.expressions.Expressions.equal;
import static com.netflix.iceberg.expressions.Expressions.greaterThan;
import static com.netflix.iceberg.expressions.Expressions.greaterThanOrEqual;
import static com.netflix.iceberg.expressions.Expressions.lessThan;
import static com.netflix.iceberg.expressions.Expressions.lessThanOrEqual;
import static com.netflix.iceberg.expressions.Expressions.or;

// TODO Wish there was a way to actually get condition expressions instead of dealing with Domain
public class ExpressionConverter
{
    private ExpressionConverter()
    {}

    public static Expression toIceberg(TupleDomain<HiveColumnHandle> tupleDomain, ConnectorSession session)
    {
        if (tupleDomain.isAll()) {
            return Expressions.alwaysTrue();
        }
        else if (tupleDomain.isNone()) {
            return Expressions.alwaysFalse();
        }
        else {
            final Map<HiveColumnHandle, Domain> tDomainMap = tupleDomain.getDomains().get();
            Expression expression = Expressions.alwaysTrue();
            for (Map.Entry<HiveColumnHandle, Domain> tDomainEntry : tDomainMap.entrySet()) {
                final HiveColumnHandle key = tDomainEntry.getKey();
                final Domain domain = tDomainEntry.getValue();
                expression = Expressions.and(expression, toIceberg(key, domain, session));
            }
            return expression;
        }
    }

    private static Expression toIceberg(HiveColumnHandle column, Domain domain, ConnectorSession session)
    {
        String columnName = column.getName();
        if (domain.isAll()) {
            return Expressions.alwaysTrue();
        }
        else if (domain.isNone()) {
            return Expressions.alwaysFalse();
        }
        else if (domain.isOnlyNull()) {
            return Expressions.isNull(columnName);
        }
        else {
            final ValueSet domainValues = domain.getValues();
            Expression expression = null;
            if (domain.isNullAllowed()) {
                expression = Expressions.isNull(columnName);
            }

            if (domainValues instanceof EquatableValueSet) {
                expression = (expression == null ? Expressions.alwaysFalse() : expression);
                if (((EquatableValueSet) domainValues).isWhiteList()) {
                    // if whitelist is true than this is a case of "in", otherwise this is a case of "not in".
                    return or(expression, equal(columnName, ((EquatableValueSet) domainValues).getValues()));
                }
                else {
                    return or(expression, Expressions.notEqual(columnName, ((EquatableValueSet) domainValues).getValues()));
                }
            }
            else {
                if (domainValues instanceof SortedRangeSet) {
                    final List<Range> orderedRanges = ((SortedRangeSet) domainValues).getOrderedRanges();
                    expression = (expression == null ? Expressions.alwaysFalse() : expression);
                    for (Range range : orderedRanges) {
                        final Marker low = range.getLow();
                        final Marker high = range.getHigh();
                        final Marker.Bound lowBound = low.getBound();
                        final Marker.Bound highBound = high.getBound();

                        // case col <> 'val' is represented as (col < 'val' or col > 'val')
                        if (lowBound.equals(EXACTLY) && highBound.equals(EXACTLY)) {
                            // case ==
                            if (getValue(column, low, session).equals(getValue(column, high, session))) {
                                expression = or(expression, equal(columnName, getValue(column, low, session)));
                            }
                            else { // case between
                                final Expression between = and(greaterThanOrEqual(columnName, getValue(column, low, session)), lessThanOrEqual(columnName, getValue(column, high, session)));
                                expression = or(expression, between);
                            }
                        }
                        else {
                            if (lowBound.equals(EXACTLY) && low.getValueBlock().isPresent()) {
                                // case >=
                                expression = or(expression, greaterThanOrEqual(columnName, getValue(column, low, session)));
                            }
                            else if (lowBound.equals(ABOVE) && low.getValueBlock().isPresent()) {
                                // case >
                                expression = or(expression, greaterThan(columnName, getValue(column, low, session)));
                            }

                            if (highBound.equals(EXACTLY) && high.getValueBlock().isPresent()) {
                                // case <=
                                expression = or(expression, lessThanOrEqual(columnName, getValue(column, high, session)));
                            }
                            else if (highBound.equals(BELOW) && high.getValueBlock().isPresent()) {
                                // case <
                                expression = or(expression, lessThan(columnName, getValue(column, high, session)));
                            }
                        }
                    }
                }
                else {
                    throw new IllegalStateException("Did not expect a domain value set other than SortedRangeSet and EquatableValueSet but got " + domainValues.getClass().getSimpleName());
                }
            }
            return expression;
        }
    }

    private static Object getValue(HiveColumnHandle columnHandle, Marker marker, ConnectorSession session)
    {
        final String base = columnHandle.getTypeSignature().getBase();
        if (base.equals(TIMESTAMP_WITH_TIME_ZONE) || base.equals(TIME_WITH_TIME_ZONE)) {
            return TimeUnit.MILLISECONDS.toNanos(DateTimeEncoding.unpackMillisUtc((Long) marker.getValue()));
        }
        else if (base.equals(TIME) || base.equals(TIMESTAMP)) {
            return TimeUnit.MILLISECONDS.toNanos((Long) marker.getValue());
        }
        else if (base.equals(VARCHAR)) {
            return marker.getPrintableValue(session);
        }
        else if (base.equals(VARBINARY)) {
            return ((Slice) marker.getValue()).getBytes();
        }
        return marker.getValue();
    }

    private static Object getValue(Type type, Object value)
    {
        if (type instanceof TimestampType || type instanceof TimestampWithTimeZoneType || type instanceof TimeType || type instanceof TimeWithTimeZoneType) {
            // iceberg does not support zone preservation.
            return TimeUnit.NANOSECONDS.toMillis((Long) value);
        }
        else if (type instanceof VarcharType) {
            return Slices.utf8Slice(value.toString());
        }
        else if (type instanceof DecimalType) {
            if (((DecimalType) type).getPrecision() <= MAX_SHORT_PRECISION) {
                return value;
            }
            else {
                return encodeScaledValue(new BigDecimal(String.valueOf(value)));
            }
        }
        else if (type instanceof IntegerType && value instanceof Integer) {
            // TODO this is not good, The range creation internally uses a Util class that validates if the value type matches with type's java class
            // representation. But does not coerce for things like IntegerType which is represented with java `long` but the value it self could be int.
            return Long.valueOf((Integer) value);
        }
        else if (type instanceof RealType && value instanceof Float) {
            return Long.valueOf(Float.floatToIntBits((Float) value));
        }
        return value;
    }

    public static TupleDomain<HiveColumnHandle> fromIceberg(Expression expression, Map<String, HiveColumnHandle> hiveColumnHandleMap, Schema schema, TypeManager typeManager)
    {
        // this assumes that null allowed would be a separate domainTuple
        final boolean nullAllowed = false;
        switch (expression.op()) {
            case TRUE:
                return TupleDomain.all();
            case FALSE:
                return TupleDomain.none();
            case AND:
                final And and = (And) expression;
                TupleDomain<HiveColumnHandle> left = fromIceberg(and.left(), hiveColumnHandleMap, schema, typeManager);
                TupleDomain<HiveColumnHandle> right = fromIceberg(and.right(), hiveColumnHandleMap, schema, typeManager);
                return left.intersect(right);
            case OR:
                final Or or = (Or) expression;
                left = fromIceberg(or.left(), hiveColumnHandleMap, schema, typeManager);
                right = fromIceberg(or.right(), hiveColumnHandleMap, schema, typeManager);
                return TupleDomain.columnWiseUnion(left, right);
            case NOT:
                return fromIceberg(((Not) expression).child(), hiveColumnHandleMap, schema, typeManager);
            case IS_NULL:
                return fromIceberg(expression, hiveColumnHandleMap, schema, typeManager, Domain::onlyNull);
            case NOT_NULL:
                return fromIceberg(expression, hiveColumnHandleMap, schema, typeManager, Domain::notNull);
            case EQ:
                return fromIceberg(expression, schema, typeManager, hiveColumnHandleMap, nullAllowed, Range::equal);
            case NOT_EQ:
                Predicate<?, Reference> predicate = (Predicate) expression;
                int fieldId = getFieldIdByName(predicate.ref(), schema);
                Type type = TypeConveter.convert(schema.findType(fieldId), typeManager);
                ValueSet values = ofRanges(Range.lessThan(type, getValue(type, predicate.literal().value())), Range.greaterThan(type, getValue(type, predicate.literal().value()))).complement();
                return TupleDomain.withColumnDomains(ImmutableMap.of(hiveColumnHandleMap.get(schema.findColumnName(fieldId)), create(values, nullAllowed)));
            case LT:
                return fromIceberg(expression, schema, typeManager, hiveColumnHandleMap, nullAllowed, Range::lessThan);
            case GT:
                return fromIceberg(expression, schema, typeManager, hiveColumnHandleMap, nullAllowed, Range::greaterThan);
            case LT_EQ:
                return fromIceberg(expression, schema, typeManager, hiveColumnHandleMap, nullAllowed, Range::lessThanOrEqual);
            case GT_EQ:
                return fromIceberg(expression, schema, typeManager, hiveColumnHandleMap, nullAllowed, Range::greaterThanOrEqual);
            case IN: // implemented through chaining EQ operation with OR
            case NOT_IN: // implemented through chaining NOT_EQ operation with OR
                throw new RuntimeException("IN and NOT_IN expression are not expected as those expressions are represented by chaining EQ/NOT_EQ with OR.");
            default:
                throw new RuntimeException("Can't handle operation " + expression.op());
        }
    }

    private static TupleDomain fromIceberg(Expression expression,
            Schema schema,
            TypeManager typeManager,
            Map<String, HiveColumnHandle> hiveColumnHandleMap,
            boolean nullAllowed, BiFunction<Type, Object, Range> valueExtractor)
    {
        Predicate<?, Reference> predicate = (Predicate) expression;
        int fieldId = getFieldIdByName(predicate.ref(), schema);
        Type type = TypeConveter.convert(schema.findType(fieldId), typeManager);
        ValueSet values = ofRanges(valueExtractor.apply(type, getValue(type, predicate.literal().value())));
        return TupleDomain.withColumnDomains(ImmutableMap.of(hiveColumnHandleMap.get(schema.findColumnName(fieldId)), create(values, nullAllowed)));
    }

    private static TupleDomain<HiveColumnHandle> fromIceberg(Expression expression,
            Map<String, HiveColumnHandle> hiveColumnHandleMap,
            Schema schema,
            TypeManager typeManager,
            Function<Type, Domain> domainExtractor)
    {
        Predicate<?, Reference> predicate = (Predicate) expression;
        int fieldId = getFieldIdByName(predicate.ref(), schema);
        Type type = TypeConveter.convert(schema.findType(fieldId), typeManager);
        return TupleDomain.withColumnDomains(ImmutableMap.of(hiveColumnHandleMap.get(schema.findColumnName(fieldId)), domainExtractor.apply(type)));
    }

    private static <R extends Reference> int getFieldIdByName(R reference, Schema schema)
    {
        if (reference instanceof BoundReference) {
            return ((BoundReference) reference).fieldId();
        }
        else if (reference instanceof NamedReference) {
            return schema.findField(((NamedReference) reference).name()).fieldId();
        }
        else {
            throw new UnsupportedOperationException("Iceberg Predicates should only be of BoundReference or NamedReference type but got " + reference.getClass().getSimpleName());
        }
    }
}
