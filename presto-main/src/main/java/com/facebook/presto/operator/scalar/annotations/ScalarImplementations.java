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
package com.facebook.presto.operator.scalar.annotations;

import com.facebook.presto.metadata.Signature;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ScalarImplementations
{
    // These three collections implement something similar to partial template specialization from C++, and allow more optimized implementations to be provided for specific types
    // These are implementations for concrete types (they have no unbound type parameters), and have the highest priority when picking an implementation
    private final Map<Signature, ScalarImplementation> exactImplementations;
    // These are implementations for when a type parameter binds to a specific native container type
    private final List<ScalarImplementation> specializedImplementations;
    // These are generic implementations
    private final List<ScalarImplementation> genericImplementations;

    public ScalarImplementations(Map<Signature, ScalarImplementation> exactImplementations, List<ScalarImplementation> specializedImplementations, List<ScalarImplementation> genericImplementations)
    {
        this.exactImplementations = requireNonNull(exactImplementations);
        this.specializedImplementations = requireNonNull(specializedImplementations);
        this.genericImplementations = requireNonNull(genericImplementations);
    }

    public List<ScalarImplementation> getGenericImplementations()
    {
        return genericImplementations;
    }

    public Map<Signature, ScalarImplementation> getExactImplementations()
    {
        return exactImplementations;
    }

    public List<ScalarImplementation> getSpecializedImplementations()
    {
        return specializedImplementations;
    }
}
