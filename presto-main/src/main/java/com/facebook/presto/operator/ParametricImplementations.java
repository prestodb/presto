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
package com.facebook.presto.operator;

import com.facebook.presto.metadata.Signature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * This class represents set of three collections representing implementations of operators
 * similar to partial template specialization from C++ allowing more optimized implementations to be provided for specific types.
 * @param <T> type of implementation details
 */
public class ParametricImplementations<T extends ParametricImplementation>
{
    // These are implementations for concrete types (they have no unbound type parameters), and have the highest priority when picking an implementation
    private final Map<Signature, T> exactImplementations;
    // These are implementations for case when a type parameter binds to a specific native container type
    private final List<T> specializedImplementations;
    // These are generic implementations
    private final List<T> genericImplementations;

    public ParametricImplementations(Map<Signature, T> exactImplementations, List<T> specializedImplementations, List<T> genericImplementations)
    {
        this.exactImplementations = ImmutableMap.copyOf(requireNonNull(exactImplementations, "exactImplementation cannot be null"));
        this.specializedImplementations = ImmutableList.copyOf(requireNonNull(specializedImplementations, "specializedImplementations cannot be null"));
        this.genericImplementations = ImmutableList.copyOf(requireNonNull(genericImplementations, "genericImplementations cannot be null"));
    }

    public List<T> getGenericImplementations()
    {
        return genericImplementations;
    }

    public Map<Signature, T> getExactImplementations()
    {
        return exactImplementations;
    }

    public List<T> getSpecializedImplementations()
    {
        return specializedImplementations;
    }
}
