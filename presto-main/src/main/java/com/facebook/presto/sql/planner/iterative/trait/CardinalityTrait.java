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
import com.google.common.collect.Range;

import java.util.Objects;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CardinalityTrait
        implements Trait<CardinalityTrait>
{
    public static CardinalityTrait scalar()
    {
        return exactly(1);
    }

    public static CardinalityTrait exactly(long cardinality)
    {
        return new CardinalityTrait(Range.singleton(cardinality));
    }

    public static CardinalityTrait atMostScalar()
    {
        return atMost(1L);
    }

    public static CardinalityTrait atMost(long cardinality)
    {
        return new CardinalityTrait(Range.closed(0L, cardinality));
    }

    private final Range<Long> cardinalityRange;

    public CardinalityTrait(Range<Long> cardinalityRange)
    {
        this.cardinalityRange = requireNonNull(cardinalityRange, "cardinalityRange is null");
    }

    @Override
    public TraitType<CardinalityTrait> getType()
    {
        return CardinalityTraitType.CARDINALITY;
    }

    @Override
    public boolean satisfies(CardinalityTrait required)
    {
        return required.getCardinalityRange().encloses(cardinalityRange);
    }

    public boolean isScalar()
    {
        return Range.singleton(1L).encloses(cardinalityRange);
    }

    public boolean isAtMostScalar()
    {
        return isAtMost(1L);
    }

    public boolean isAtMost(long maxCardinality)
    {
        return Range.closed(0L, maxCardinality).encloses(cardinalityRange);
    }

    public OptionalLong getCardinality()
    {
        if (cardinalityRange.hasLowerBound() && cardinalityRange.hasUpperBound() && cardinalityRange.lowerEndpoint() == cardinalityRange.upperEndpoint()) {
            return OptionalLong.of(cardinalityRange.lowerEndpoint());
        }
        return OptionalLong.empty();
    }

    public Range<Long> getCardinalityRange()
    {
        return cardinalityRange;
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
        return Objects.equals(cardinalityRange, that.cardinalityRange);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cardinalityRange);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("cardinalityRange", cardinalityRange)
                .toString();
    }
}
