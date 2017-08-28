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
import com.google.common.collect.Streams;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class TraitSet
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

    public TraitSet(Map<TraitType, Trait> traits)
    {
        this.traits = ImmutableMap.copyOf(requireNonNull(traits, "traits is null"));
    }

    public <T extends Trait> Optional<T> getTrait(TraitType<T> traitType)
    {
        // TODO remove the cast below
        return Optional.ofNullable((T) traits.get(traitType));
    }

    public TraitSet merge(TraitSet second)
    {
        Map<TraitType, Trait> newTraits = Streams.concat(
                traits.keySet().stream(),
                second.traits.keySet().stream())
                .distinct()
                .collect(toImmutableMap(
                        traitType -> traitType,
                        traitType -> chooseMoreSpecific(getTrait(traitType), second.getTrait(traitType))));

        return new TraitSet(newTraits);
    }

    /**
     * If first is SORT(A, B) and second is SORT(A, B, C), then second satisfies first, but first does not satisfy second.
     * So second is more specific.
     */
    private <T extends Trait> T chooseMoreSpecific(Optional<T> first, Optional<T> second)
    {
        checkState(first.isPresent() || second.isPresent(), "Either first or second has to be present");
        if (!first.isPresent()) {
            return second.get();
        }
        if (!second.isPresent()) {
            return first.get();
        }
        if (first.get().satisfies(second.get())) {
            return second.get();
        }
        return first.get();
    }
}
