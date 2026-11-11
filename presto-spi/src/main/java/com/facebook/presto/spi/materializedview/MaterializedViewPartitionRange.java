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
 * The exact half-open source interval represented by one canonical target partition value.
 */
public final class MaterializedViewPartitionRange
{
    private final ConstantExpression lowerBound;
    private final ConstantExpression upperBound;

    public MaterializedViewPartitionRange(ConstantExpression lowerBound, ConstantExpression upperBound)
    {
        this.lowerBound = requireNonNull(lowerBound, "lowerBound is null");
        this.upperBound = requireNonNull(upperBound, "upperBound is null");
        if (lowerBound.isNull()) {
            throw new IllegalArgumentException("lowerBound is null");
        }
        if (upperBound.isNull()) {
            throw new IllegalArgumentException("upperBound is null");
        }
        if (!lowerBound.getType().equals(upperBound.getType())) {
            throw new IllegalArgumentException("range bound types do not match");
        }
    }

    public ConstantExpression getLowerBound()
    {
        return lowerBound;
    }

    public ConstantExpression getUpperBound()
    {
        return upperBound;
    }
}
