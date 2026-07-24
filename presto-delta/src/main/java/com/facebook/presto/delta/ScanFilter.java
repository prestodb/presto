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
package com.facebook.presto.delta;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.EquatableValueSet;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.Ranges;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import io.airlift.slice.Slice;
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ScanFilter
{
    private static final String ALWAYS_FALSE = "ALWAYS_FALSE";
    private static final String OR = "OR";
    private static final String IS_NULL = "IS_NULL";
    private static final String IS_NOT_NULL = "IS_NOT_NULL";
    private static final String EQUALS = "=";
    private static final String LESS_THAN = "<";
    private static final String LESS_THAN_OR_EQUAL = "<=";
    private static final String GREATER_THAN = ">";
    private static final String GREATER_THAN_OR_EQUAL = ">=";
    private final Predicate predicate;

    private ScanFilter(Builder builder)
    {
        this.predicate = createPredicate(builder.expressions);
    }

    private Predicate createPredicate(List<Expression> expressions)
    {
        if (expressions.isEmpty()) {
            return null;
        }

        Predicate result = (Predicate) expressions.get(0);
        for (int i = 1; i < expressions.size(); i++) {
            result = new And(result, (Predicate) expressions.get(i));
        }
        return result;
    }

    public Optional<Predicate> getPredicate()
    {
        return Optional.ofNullable(predicate);
    }

    public static class Builder
    {
        private final List<Expression> expressions = new ArrayList<>(0);
        private final TypeManager typeManager;

        public Builder(TypeManager typeManager)
        {
            this.typeManager = typeManager;
        }

        public ScanFilter build()
        {
            return new ScanFilter(this);
        }

        public Builder withDomain(TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain)
        {
            DeltaColumnHandle columnHandle = columnDomain.getColumn();
            Domain domain = columnDomain.getDomain();

            // Skip if domain is all (no filtering)
            if (domain.isAll()) {
                return this;
            }

            // Handle null only domain
            if (domain.isOnlyNull()) {
                expressions.add(new Predicate(IS_NULL, new Column(columnHandle.getLogicalName())));
                return this;
            }

            ValueSet valueSet = domain.getValues();
            Column column = new Column(columnHandle.getLogicalName());

            // should not happen, but handle it anyway
            if (valueSet.isNone()) {
                expressions.add(new Predicate(ALWAYS_FALSE, column));
                return this;
            }

            // Handle single value predicates
            if (domain.isSingleValue()) {
                Expression literalExpr = convertToLiteral(domain.getSingleValue(), columnHandle.getDataType());
                Predicate equalsPredicate = new Predicate(EQUALS, column, literalExpr);

                addPredicateWithNullHandling(equalsPredicate, column, domain.isNullAllowed());
                return this;
            }

            if (valueSet instanceof SortedRangeSet) {
                handleSortedRangeSet((SortedRangeSet) valueSet, column, columnHandle.getDataType(), domain.isNullAllowed());
            }
            else if (valueSet instanceof EquatableValueSet) {
                handleEquatableValueSet((EquatableValueSet) valueSet, column);
            }
            else {
                throw new UnsupportedOperationException("Unsupported value set type: " + valueSet.getClass());
            }

            return this;
        }

        private void handleSortedRangeSet(SortedRangeSet sortedRangeSet, Column column, TypeSignature typeSignature, boolean nullAllowed)
        {
            Ranges ranges = sortedRangeSet.getRanges();
            List<Predicate> rangePredicates = new ArrayList<>();

            for (Range range : ranges.getOrderedRanges()) {
                rangePredicates.add(convertRangeToPredicate(range, column, typeSignature));
            }

            if (!rangePredicates.isEmpty()) {
                Predicate combinedPredicate = combinePredicatesWithOr(rangePredicates);
                // handle || IS NULL case
                addPredicateWithNullHandling(combinedPredicate, column, nullAllowed);
            }
        }

        private void handleEquatableValueSet(EquatableValueSet equatableValueSet, Column column)
        {
            // send is not null predicate if the column is not in the whitelist (nulls are not allowed)
            Predicate predicate = equatableValueSet.isWhiteList() ? null : new Predicate(IS_NOT_NULL, column);
            addPredicateWithNullHandling(predicate, column, equatableValueSet.isWhiteList());
        }

        private Predicate combinePredicatesWithOr(List<Predicate> predicates)
        {
            Predicate result = predicates.get(0);
            for (int i = 1; i < predicates.size(); i++) {
                result = new Predicate(OR, result, predicates.get(i));
            }
            return result;
        }

        private void addPredicateWithNullHandling(Predicate predicate, Column column, boolean nullAllowed)
        {
            if (nullAllowed) {
                // if null is allowed, it only can be added as OR to the given predicate, as
                // the same column cannot be bounded and null at the same time, and the
                // "null only" case is already handled at the start of the "withFilter" implementation
                Predicate isNullPredicate = new Predicate(IS_NULL, column);
                expressions.add(new Predicate(OR, predicate, isNullPredicate));
            }
            else {
                // pass if there is already a predicate as will filter nulls to avoid
                // generating a redundant extra IS_NOT_NULL predicate
                expressions.add(predicate);
            }
        }

        private Predicate convertRangeToPredicate(Range range, Column column, TypeSignature typeSignature)
        {
            boolean lowUnbounded = range.isLowUnbounded();
            boolean highUnbounded = range.isHighUnbounded();

            if (lowUnbounded && highUnbounded) {
                return new Predicate(IS_NOT_NULL, column);
            }

            if (range.isSingleValue()) {
                Expression literalExpr = convertToLiteral(range.getLowBoundedValue(), typeSignature);
                return new Predicate(EQUALS, column, literalExpr);
            }

            List<Predicate> rangeComponents = new ArrayList<>();

            if (!lowUnbounded) {
                Expression lowLiteral = convertToLiteral(range.getLowBoundedValue(), typeSignature);
                String lowOperator = range.isLowInclusive() ? GREATER_THAN_OR_EQUAL : GREATER_THAN;
                rangeComponents.add(new Predicate(lowOperator, column, lowLiteral));
            }

            if (!highUnbounded) {
                Expression highLiteral = convertToLiteral(range.getHighBoundedValue(), typeSignature);
                String highOperator = range.isHighInclusive() ? LESS_THAN_OR_EQUAL : LESS_THAN;
                rangeComponents.add(new Predicate(highOperator, column, highLiteral));
            }

            if (rangeComponents.size() == 1) {
                return rangeComponents.get(0);
            }
            else {
                return new And(rangeComponents.get(0), rangeComponents.get(1));
            }
        }

        private Expression convertToLiteral(Object value, TypeSignature typeSignature)
        {
            String typeBase = typeSignature.getBase();

            switch (typeBase) {
                case StandardTypes.BOOLEAN:
                    return Literal.ofBoolean((Boolean) value);
                case StandardTypes.TINYINT:
                    return Literal.ofByte(((Long) value).byteValue());
                case StandardTypes.SMALLINT:
                    return Literal.ofShort(((Long) value).shortValue());
                case StandardTypes.INTEGER:
                    return Literal.ofInt(((Long) value).intValue());
                case StandardTypes.BIGINT:
                    return Literal.ofLong((Long) value);
                case StandardTypes.REAL:
                    return Literal.ofFloat(Float.intBitsToFloat(((Long) value).intValue()));
                case StandardTypes.DOUBLE:
                    return Literal.ofDouble((Double) value);
                case StandardTypes.DECIMAL:
                    DecimalType decimalType = (DecimalType) this.typeManager.getType(typeSignature);
                    BigDecimal decimal = null;
                    if (decimalType.isShort()) {
                        decimal = new BigDecimal((long) value);
                    }
                    else {
                        BigInteger decimalValue = Decimals.decodeUnscaledValue((Slice) value);
                        decimal = new BigDecimal(decimalValue, decimalType.getScale());
                    }
                    return Literal.ofDecimal(decimal, decimalType.getPrecision(), decimalType.getScale());
                case StandardTypes.VARCHAR:
                case StandardTypes.CHAR:
                    return Literal.ofString(((Slice) value).toStringUtf8());
                case StandardTypes.VARBINARY:
                    return Literal.ofBinary(((Slice) value).getBytes());
                case StandardTypes.DATE:
                    return Literal.ofDate(((Long) value).intValue());
                // Due to issues with timestamp pushdowns in test table delta_v3/timestamp64, disabled for now
                // Code works for all other test tables
                // The issue lies in two fronts:
                //   - Presto does not handle timestamps with microsecond precision
                //   - Delta kernel library does not correctly parses ISO-8601 timestamps with microsecond precision
                // As we cannot detect this cases in runtime from the query predicate, we need to disable pushdown for now
                case StandardTypes.TIMESTAMP:
                    //// Delta Kernel expects microsecond precision
                    //return Literal.ofTimestampNtz((Long) value * 1000L);
                case StandardTypes.TIMESTAMP_WITH_TIME_ZONE:
                    //TimeZoneKey tzKey = DateTimeEncoding.unpackZoneKey((Long) value);
                    //long unpackedMillisUtc = DateTimeEncoding.unpackMillisUtc((Long) value);
                    //Instant instant = Instant.ofEpochMilli(unpackedMillisUtc);
                    //ZonedDateTime withZoneOffset = instant.atZone(ZoneId.of(tzKey.getId()));
                    //withZoneOffset.withZoneSameInstant(ZoneId.of(TimeZoneKey.UTC_KEY.getId()));
                    //// Delta Kernel expects microsecond precision
                    //long deltaMicros = withZoneOffset.toInstant().toEpochMilli() * 1000L;
                    //return Literal.ofTimestamp(deltaMicros);
                case StandardTypes.ARRAY:
                case StandardTypes.MAP:
                case StandardTypes.ROW:
                default:
                    throw new UnsupportedOperationException("Unsupported type for pushdown: " + typeBase);
            }
        }
    }
}
