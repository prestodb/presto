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

import com.facebook.presto.sql.planner.iterative.GroupTrait;
import com.google.common.collect.Range;

import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CardinalityGroupTrait
        implements GroupTrait
{
    public static CardinalityGroupTrait scalar()
    {
        return exactly(1);
    }

    public static CardinalityGroupTrait exactly(long cardinality)
    {
        return new CardinalityGroupTrait(Range.singleton(cardinality));
    }

    public static CardinalityGroupTrait atMostScalar()
    {
        return atMost(1L);
    }

    public static CardinalityGroupTrait atMost(long cardinality)
    {
        return new CardinalityGroupTrait(Range.closed(0L, cardinality));
    }

    public static CardinalityGroupTrait unknown()
    {
        return new CardinalityGroupTrait(Range.atLeast(0L));
    }

    private final Range<Long> cardinalityRange;

    public CardinalityGroupTrait(Range<Long> cardinalityRange)
    {
        this.cardinalityRange = requireNonNull(cardinalityRange, "cardinalityRange is null");
    }

    @Override
    public Type getType()
    {
        return Type.CARDINALITY;
    }

    @Override
    public boolean satisfies(GroupTrait groupTrait)
    {
        checkArgument(groupTrait instanceof CardinalityGroupTrait, "Expected groupTrait of type: " + getClass().getName());
        CardinalityGroupTrait cardinalityGroupTrait = (CardinalityGroupTrait) groupTrait;
        return cardinalityRange.encloses(cardinalityGroupTrait.cardinalityRange);
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
}
