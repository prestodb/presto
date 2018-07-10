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

package com.facebook.presto.sql.planner.iterative;

import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public final class TraitSet
{
    private static final TraitSet EMPTY = new TraitSet(ImmutableMap.of());

    public static TraitSet empty()
    {
        return EMPTY;
    }

    public static TraitSet of(Trait trait)
    {
        return new TraitSet(ImmutableMap.of(trait.getType(), trait));
    }

    private final Map<TraitType, Trait> traits;

    private TraitSet(Map<TraitType, Trait> traits)
    {
        this.traits = ImmutableMap.copyOf(requireNonNull(traits, "traits is null"));
    }

    public <T extends Trait> Optional<T> getTrait(TraitType traitType)
    {
        return Optional.ofNullable((T) traits.get(traitType));
    }

    public TraitSet addTrait(Trait trait)
    {
        TraitType type = trait.getType();
        ImmutableMap.Builder<TraitType, Trait> newTraits = ImmutableMap.builder();
        if (traits.containsKey(type)) {
            checkArgument(
                    Objects.equals(traits.get(type), trait),
                    "TraitSet already contains trait for type '%s' with value '%s', while trying to '%s'",
                    type,
                    traits.get(type),
                    trait);
        }
        else {
            newTraits.put(trait.getType(), trait);
        }
        newTraits.putAll(traits);
        return new TraitSet(newTraits.build());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("traits", traits.values())
                .toString();
    }
}
