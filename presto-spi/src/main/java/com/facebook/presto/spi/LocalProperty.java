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
package com.facebook.presto.spi;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public interface LocalProperty<E>
{
    <T> Optional<LocalProperty<T>> translate(Function<E, Optional<T>> translator);

    /**
     * Rules:
     * <p>
     * 1. G => {}
     * 2. G => G - C where every c in C is constant
     * 3. G1, G2 => G1 U G2
     * 4. Gi, ..., Gk  => Gi, ..., Gk-1
     * 5. Gi, ..., Gk <=> Gi, ..., Gi U Gk
     */
    static <T> Set<T> getMaxGroupingSubset(List<LocalProperty<T>> properties, Set<T> constants, Collection<T> groupKeys)
    {
        // Any columns matching those in the requirement or known to be constant
        // can be used to make inferences
        Set<T> candidates = new HashSet<>();
        candidates.addAll(constants);
        candidates.addAll(groupKeys);

        // constant keys are a trivial result
        Set<T> result = new HashSet<>(constants);
        result.retainAll(groupKeys);

        for (LocalProperty<T> property : properties) {
            Set<T> columns = new HashSet<>();
            if (property instanceof GroupingProperty) {
                columns.addAll(((GroupingProperty<T>) property).getColumns());
            }
            else if (property instanceof SortingProperty) {
                columns.add(((SortingProperty<T>) property).getColumn());
            }
            else {
                throw new UnsupportedOperationException("Local property type not supported: " + property.getClass().getName());
            }

            if (!candidates.containsAll(columns)) {
                break;
            }

            // Grouping may be satisfied due to a combination of constants and columns
            // present in the requirement, but we only care about the ones coming from
            // the requirement
            columns.retainAll(groupKeys);

            result.addAll(columns);
        }

        return result;
    }
}
