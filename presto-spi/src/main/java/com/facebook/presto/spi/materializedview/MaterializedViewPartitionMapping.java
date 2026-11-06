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
package com.facebook.presto.spi.materializedview;

import com.facebook.presto.spi.relation.ConstantExpression;

import static java.util.Objects.requireNonNull;

/**
 * The exact source partition values represented by one canonical target partition value.
 */
public final class MaterializedViewPartitionMapping
{
    public enum Kind
    {
        SINGLE_VALUE,
        HALF_OPEN_RANGE
    }

    private final Kind kind;
    private final ConstantExpression singleValue;
    private final MaterializedViewPartitionRange range;

    private MaterializedViewPartitionMapping(Kind kind, ConstantExpression singleValue, MaterializedViewPartitionRange range)
    {
        this.kind = requireNonNull(kind, "kind is null");
        this.singleValue = singleValue;
        this.range = range;
    }

    public static MaterializedViewPartitionMapping singleValue(ConstantExpression value)
    {
        requireNonNull(value, "value is null");
        if (value.isNull()) {
            throw new IllegalArgumentException("value is null");
        }
        return new MaterializedViewPartitionMapping(Kind.SINGLE_VALUE, value, null);
    }

    public static MaterializedViewPartitionMapping halfOpenRange(ConstantExpression lowerBound, ConstantExpression upperBound)
    {
        return new MaterializedViewPartitionMapping(
                Kind.HALF_OPEN_RANGE,
                null,
                new MaterializedViewPartitionRange(lowerBound, upperBound));
    }

    public Kind getKind()
    {
        return kind;
    }

    public ConstantExpression getSingleValue()
    {
        if (kind != Kind.SINGLE_VALUE) {
            throw new IllegalStateException("mapping is not a single value");
        }
        return singleValue;
    }

    public MaterializedViewPartitionRange getRange()
    {
        if (kind != Kind.HALF_OPEN_RANGE) {
            throw new IllegalStateException("mapping is not a half-open range");
        }
        return range;
    }
}
