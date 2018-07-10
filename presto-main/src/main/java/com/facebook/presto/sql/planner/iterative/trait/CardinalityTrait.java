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

package com.facebook.presto.sql.planner.iterative.trait;

import com.facebook.presto.sql.planner.iterative.Trait;
import com.facebook.presto.sql.planner.iterative.TraitType;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

@Immutable
public final class CardinalityTrait
        implements Trait
{
    public static final TraitType<CardinalityTrait> CARDINALITY = new TraitType<>(CardinalityTrait.class);

    public static CardinalityTrait scalar()
    {
        return exactly(1);
    }

    public static CardinalityTrait exactly(long cardinality)
    {
        return new CardinalityTrait(cardinality, cardinality);
    }

    public static CardinalityTrait atMostScalar()
    {
        return atMost(1L);
    }

    public static CardinalityTrait atMost(long cardinality)
    {
        return new CardinalityTrait(0, cardinality);
    }

    private final long minCardinality;
    private final long maxCardinality;

    public CardinalityTrait(long minCardinality, long maxCardinality)
    {
        checkCardinalityIsPositive(minCardinality);
        checkCardinalityIsPositive(maxCardinality);
        checkArgument(minCardinality <= maxCardinality, "min '%s' cardinality is greater than max '%s'", minCardinality, maxCardinality);
        this.minCardinality = minCardinality;
        this.maxCardinality = maxCardinality;
    }

    private void checkCardinalityIsPositive(long cardinality)
    {
        checkArgument(cardinality >= 0, "Cardinality cannot be negative");
    }

    @Override
    public TraitType getType()
    {
        return CARDINALITY;
    }

    public boolean isScalar()
    {
        return isExactly(1L);
    }

    public boolean isExactly(long cardinality)
    {
        checkCardinalityIsPositive(cardinality);
        return getExactCardinality().orElse(-1) == cardinality;
    }

    private OptionalLong getExactCardinality()
    {
        if (minCardinality == maxCardinality) {
            return OptionalLong.of(minCardinality);
        }
        return OptionalLong.empty();
    }

    public boolean isAtMostScalar()
    {
        return isAtMost(1L);
    }

    public boolean isAtMost(long maxCardinality)
    {
        return atMost(maxCardinality).encloses(this);
    }

    private boolean encloses(CardinalityTrait other)
    {
        return minCardinality <= other.minCardinality && maxCardinality >= other.maxCardinality;
    }

    public long getMaxCardinality()
    {
        return maxCardinality;
    }

    public long getMinCardinality()
    {
        return minCardinality;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CardinalityTrait that = (CardinalityTrait) o;
        return minCardinality == that.minCardinality &&
                maxCardinality == that.maxCardinality;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(minCardinality, maxCardinality);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("minCardinality", minCardinality)
                .add("maxCardinality", maxCardinality)
                .toString();
    }
}
