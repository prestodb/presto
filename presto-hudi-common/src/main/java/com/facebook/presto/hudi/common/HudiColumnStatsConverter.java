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
package com.facebook.presto.hudi.common;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.stats.ValueMetadata;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

/**
 * Converts unwrapped scalar column statistics to a conservative domain.
 *
 * The caller must verify the field's schema and identify the physical file.
 * This class does not establish snapshot consistency.
 */
public final class HudiColumnStatsConverter
{
    private HudiColumnStatsConverter() {}

    public static Domain toDomain(Type type, HoodieMetadataColumnStats statistics)
    {
        requireNonNull(type, "type is null");
        if (statistics == null || !Boolean.FALSE.equals(statistics.getIsDeleted()) ||
                statistics.getValueCount() == null || statistics.getNullCount() == null) {
            return Domain.all(type);
        }
        try {
            ValueMetadata valueMetadata = ValueMetadata.getValueMetadata(statistics.getValueType());
            return toDomain(type, statistics.getValueCount(), statistics.getNullCount(),
                    valueMetadata.unwrapValue(statistics.getMinValue()),
                    valueMetadata.unwrapValue(statistics.getMaxValue()));
        }
        catch (RuntimeException ignored) {
            // A corrupt or unsupported value encoding must not reject the file.
            return Domain.all(type);
        }
    }

    /**
     * The value count includes nulls. Bounds are inclusive and may be absent.
     * Invalid statistics and unsupported types produce an unconstrained domain.
     */
    public static Domain toDomain(Type type, long valueCount, long nullCount, Object minimum, Object maximum)
    {
        requireNonNull(type, "type is null");
        if (!isSupportedType(type) || valueCount <= 0 || nullCount < 0 || nullCount > valueCount) {
            return Domain.all(type);
        }

        if (nullCount == valueCount) {
            // Bounds on an all-null column contradict the counts.
            return minimum == null && maximum == null ? Domain.onlyNull(type) : Domain.all(type);
        }

        boolean nullAllowed = nullCount > 0;
        if (minimum == null || maximum == null) {
            return Domain.create(ValueSet.all(type), nullAllowed);
        }

        if (type.equals(BOOLEAN)) {
            if (!(minimum instanceof Boolean) || !(maximum instanceof Boolean) || ((boolean) minimum && !(boolean) maximum)) {
                return Domain.all(type);
            }
            return Domain.create(
                    minimum.equals(maximum) ? ValueSet.of(type, minimum) : ValueSet.all(type),
                    nullAllowed);
        }

        // Do not accept arbitrary Number implementations: longValue() can truncate or overflow.
        if (!isIntegralValue(minimum) || !isIntegralValue(maximum)) {
            return Domain.all(type);
        }
        long min = ((Number) minimum).longValue();
        long max = ((Number) maximum).longValue();
        if (min > max || isOverflow(type, min, max)) {
            return Domain.all(type);
        }
        return Domain.create(ValueSet.ofRanges(Range.range(type, min, true, max, true)), nullAllowed);
    }

    static boolean isSupportedType(Type type)
    {
        // String ordering, timestamp units and floating-point NaN semantics require
        // separate validation against the Hudi writer before those bounds can be used.
        return type.equals(BOOLEAN) || type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT);
    }

    private static boolean isIntegralValue(Object value)
    {
        return value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long;
    }

    private static boolean isOverflow(Type type, long min, long max)
    {
        if (type.equals(TINYINT)) {
            return min < Byte.MIN_VALUE || max > Byte.MAX_VALUE;
        }
        if (type.equals(SMALLINT)) {
            return min < Short.MIN_VALUE || max > Short.MAX_VALUE;
        }
        if (type.equals(INTEGER)) {
            return min < Integer.MIN_VALUE || max > Integer.MAX_VALUE;
        }
        return false;
    }
}
