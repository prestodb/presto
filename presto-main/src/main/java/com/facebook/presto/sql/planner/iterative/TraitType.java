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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class TraitType<T extends Trait>
{
    private final Class<T> traitClass;

    public TraitType(Class<T> traitClass)
    {
        this.traitClass = requireNonNull(traitClass, "traitClass is null");
    }

    public T cast(Trait trait)
    {
        return traitClass.cast(trait);
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
        TraitType<?> traitType = (TraitType<?>) o;
        return Objects.equals(traitClass, traitType.traitClass);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(traitClass);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("traitClass", traitClass.getName())
                .toString();
    }
}
