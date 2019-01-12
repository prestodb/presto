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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.type.TypeSignature;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.operator.annotations.FunctionsParserHelper.validateSignaturesCompatibility;
import static java.util.Objects.requireNonNull;

/**
 * This class represents set of three collections representing implementations of operators
 * similar to partial template specialization from C++ allowing more optimized implementations to be provided for specific types.
 *
 * @param <T> type of implementation details
 */
public class ParametricImplementationsGroup<T extends ParametricImplementation>
{
    // These are implementations for concrete types (they have no unbound type parameters), and have the highest priority when picking an implementation
    private final Map<Signature, T> exactImplementations;
    // These are implementations for case when a type parameter binds to a specific native container type
    private final List<T> specializedImplementations;
    // These are generic implementations
    private final List<T> genericImplementations;

    private final Signature signature;

    public ParametricImplementationsGroup(
            Map<Signature, T> exactImplementations,
            List<T> specializedImplementations,
            List<T> genericImplementations,
            Signature signature)
    {
        this.exactImplementations = ImmutableMap.copyOf(requireNonNull(exactImplementations, "exactImplementation cannot be null"));
        this.specializedImplementations = ImmutableList.copyOf(requireNonNull(specializedImplementations, "specializedImplementations cannot be null"));
        this.genericImplementations = ImmutableList.copyOf(requireNonNull(genericImplementations, "genericImplementations cannot be null"));
        this.signature = requireNonNull(signature, "signature cannot be null");
    }

    public static <T extends ParametricImplementation> ParametricImplementationsGroup<T> of(T... implementations)
    {
        ParametricImplementationsGroup.Builder<T> builder = builder();
        for (T implementation : implementations) {
            builder.addImplementation(implementation);
        }
        return builder.build();
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

    public Signature getSignature()
    {
        return signature;
    }

    public static <T extends ParametricImplementation> Builder<T> builder()
    {
        return new Builder<>();
    }

    public static final class Builder<T extends ParametricImplementation>
    {
        private final ImmutableMap.Builder<Signature, T> exactImplementations = ImmutableMap.builder();
        private final ImmutableList.Builder<T> specializedImplementations = ImmutableList.builder();
        private final ImmutableList.Builder<T> genericImplementations = ImmutableList.builder();

        private Builder() {}

        public ParametricImplementationsGroup<T> build()
        {
            Map<Signature, T> exactImplementations = this.exactImplementations.build();
            List<T> specializedImplementations = this.specializedImplementations.build();
            List<T> genericImplementations = this.genericImplementations.build();
            return new ParametricImplementationsGroup<>(
                    exactImplementations,
                    specializedImplementations,
                    genericImplementations,
                    determineGenericSignature(exactImplementations, specializedImplementations, genericImplementations));
        }

        public void addImplementation(T implementation)
        {
            if (implementation.getSignature().getTypeVariableConstraints().isEmpty()
                    && implementation.getSignature().getArgumentTypes().stream().noneMatch(TypeSignature::isCalculated)
                    && !implementation.getSignature().getReturnType().isCalculated()) {
                exactImplementations.put(implementation.getSignature(), implementation);
            }
            else if (implementation.hasSpecializedTypeParameters()) {
                specializedImplementations.add(implementation);
            }
            else {
                genericImplementations.add(implementation);
            }
        }

        private static <T extends ParametricImplementation> Signature determineGenericSignature(
                Map<Signature, T> exactImplementations,
                List<T> specializedImplementations,
                List<T> genericImplementations)
        {
            if (specializedImplementations.size() + genericImplementations.size() == 0) {
                return getOnlyElement(exactImplementations.keySet());
            }

            Optional<Signature> signature = Optional.empty();
            for (T implementation : specializedImplementations) {
                validateSignaturesCompatibility(signature, implementation.getSignature());
                signature = Optional.of(implementation.getSignature());
            }

            for (T implementation : genericImplementations) {
                validateSignaturesCompatibility(signature, implementation.getSignature());
                signature = Optional.of(implementation.getSignature());
            }

            return signature.get();
        }
    }
}
