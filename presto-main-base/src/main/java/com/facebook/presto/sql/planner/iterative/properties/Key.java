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
package com.facebook.presto.sql.planner.iterative.properties;

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Represents a primary or unique key constraint that holds for a final or
 * intermediate result set produced by a PlanNode.
 * It can also be used to represent a key requirement that must be
 * satisfied by a PlanNode (e.g. distinct requirement)
 */
public final class Key
{
    private final Set<VariableReferenceExpression> variables;

    /**
     * A set of variable references that satisfy a primary or unique key constraint.
     *
     * @param variables
     */
    public Key(Set<VariableReferenceExpression> variables)
    {
        requireNonNull(variables, "Variables is null.");
        checkArgument(!variables.isEmpty(), "Variables is empty");
        this.variables = ImmutableSet.copyOf(variables);
    }

    /**
     * Determines if a provided key requirement is satisfied by this key.
     * This is true if the variables in this key are a subset of the variables in the key requirement.
     * Note the this operation should be called only after using the normalize method to render
     * the key and key requirement into their canonical forms using equivalence classes.
     *
     * @param keyRequirement
     * @return True if this key satisfies the key requirement and False otherwise.
     */
    public boolean keySatisifiesRequirement(Key keyRequirement)
    {
        //ideally this would be a simple subset operation but the "canonicalize" operation in UnliasSymbols inexplicably
        //clones VariableReferenceExpression's so two references to the same outputs might be made via different objects
        return variables.stream().allMatch(vk -> keyRequirement.variables.stream().anyMatch(vk::equals));
    }

    /**
     * Returns a canonical version of this key wherein duplicate or constant variables are removed
     * and any remaining variables are replaced with their equivalence class heads.
     * Note that if all key variables are bound to constants an empty result is
     * returned, signaling that at most a single record is in the result set constrained
     * by this key.
     *
     * @param equivalenceClassProperty
     * @return A normalized version of this key or empty if all variables are bound to constants.
     */
    public static Optional<Key> getNormalizedKey(Key key, EquivalenceClassProperty equivalenceClassProperty)
    {
        Set<VariableReferenceExpression> unboundVariables = key.variables.stream()
                .map(v -> equivalenceClassProperty.getEquivalenceClassHead(v))
                .filter(equivalenceHead -> (equivalenceHead instanceof VariableReferenceExpression))
                .map(eqHead -> ((VariableReferenceExpression) eqHead))
                .collect(toImmutableSet());

        return unboundVariables.isEmpty() ? Optional.empty() : Optional.of(new Key(unboundVariables));
    }

    /**
     * Returns a projected version of this key.
     * Variables in the key are mapped to output variables in the context beyond the project operation.
     * If a key attribute does not have an assignment in the new attribute context, it is mapped to the assignment of
     * an equivalent attribute whenever possible. For example, assume A is a key attribute and there is no new assignment
     * for A. Assume further that A and B are in the same equivalence class and there is an assignment from B to B’.
     * Consequently, A can be assigned to B' rather than get projected. If any of the variables are not mapped then an
     * empty result is returned signaling that the key is effectively uninteresting beyond the project operation and hence is not propagated.
     *
     * @param inverseVariableMappings
     * @return A projected version of this key or empty if any variables are not propagated.
     */
    public Optional<Key> project(LogicalPropertiesImpl.InverseVariableMappingsWithEquivalence inverseVariableMappings)
    {
        Set<VariableReferenceExpression> mappedVariables = new HashSet<>();
        Optional<VariableReferenceExpression> mappedVariable;
        for (VariableReferenceExpression v : variables) {
            mappedVariable = inverseVariableMappings.get(v);
            if (mappedVariable.isPresent()) {
                mappedVariables.add(mappedVariable.get());
            }
            else {
                return Optional.empty();
            }
        }
        return Optional.of(new Key(mappedVariables));
    }

    /**
     * Returns a version of thisKey concatenated with the otherKey.
     * A concatenated key results from a join operation where concatenated keys of the left and
     * right join inputs form unique constraints on the join result.
     *
     * @param thisKey
     * @param otherKey
     * @return a version of thisKey concatenated with the otherKey.
     */
    public static Key concatKeys(Key thisKey, Key otherKey)
    {
        ImmutableSet.Builder<VariableReferenceExpression> concatenatedVariables = ImmutableSet.builder();
        concatenatedVariables
                .addAll(thisKey.variables)
                .addAll(otherKey.variables);
        return new Key(concatenatedVariables.build());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("variables", variables.stream().map(VariableReferenceExpression::toString).collect(Collectors.joining(",")))
                .toString();
    }
}
