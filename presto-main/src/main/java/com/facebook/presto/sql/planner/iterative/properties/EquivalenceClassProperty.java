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

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Represents classes of equivalent variable and constants references that
 * hold for a final or intermediate result set produced by a PlanNode.
 * Variable and constant references are made equivalent via predicate application.
 * <p>
 * Each equivalence class is represented by a head. The head is carefully chosen
 * to be a the member with the strongest binding (i.e. a constant).
 * <p>
 * Note that the equivalence class property does not store trivial equivalence
 * classes with only one member. All variable or constant references are
 * considered to be in their own virtual equivalence class until combined
 * into stored larger classes by predicate application.
 */
public class EquivalenceClassProperty
{
    private final Map<RowExpression, RowExpression> equivalenceClassHeads = new HashMap<>();
    private final Map<RowExpression, List<RowExpression>> equivalenceClasses = new HashMap<>();
    private final FunctionResolution functionResolution;

    private boolean updated;

    public EquivalenceClassProperty(FunctionResolution functionResolution)
    {
        this.functionResolution = functionResolution;
    }

    /**
     * Determines if one equivalence class property is more general than another.
     * An equivalence class property is more general than another if it includes all equivalences of the other.
     *
     * @param otherEquivalenceClassProperty
     * @return True if this equivalenceClassProperty is more general than otherEquivalenceClassProperty or False otherwise.
     */
    public boolean isMoreGeneralThan(EquivalenceClassProperty otherEquivalenceClassProperty)
    {
        requireNonNull(otherEquivalenceClassProperty, "otherEquivalenceClassProperty is null");
        if (equivalenceClasses.isEmpty() && otherEquivalenceClassProperty.equivalenceClasses.isEmpty()) {
            return true;
        }
        if (equivalenceClasses.isEmpty() || otherEquivalenceClassProperty.equivalenceClasses.isEmpty()) {
            return false;
        }

        ImmutableList<ImmutableSet<RowExpression>> eqClassSets = equivalenceClasses.entrySet()
                .stream()
                .map(e1 -> new ImmutableSet.Builder<RowExpression>()
                        .add(e1.getKey())
                        .addAll(e1.getValue())
                        .build())
                .collect(toImmutableList());

        //every equivalence class of other is a subset of some equivalence class of the first
        return otherEquivalenceClassProperty.equivalenceClasses.entrySet()
                .stream()
                .allMatch(e -> {
                    final Set<RowExpression> otherEqClass = new HashSet<>();
                    otherEqClass.add(e.getKey());
                    otherEqClass.addAll(e.getValue());
                    return eqClassSets.stream().anyMatch(eqClassSet -> eqClassSet.containsAll(otherEqClass));
                });
    }

    /**
     * Returns the head of the equivalence class of the provided variable or constant reference.
     *
     * @param expression
     * @return The head of the equivalence class of the provided variable or constant reference.
     */
    public RowExpression getEquivalenceClassHead(RowExpression expression)
    {
        requireNonNull(expression, "expression is null");
        checkArgument((expression instanceof VariableReferenceExpression || expression instanceof ConstantExpression),
                "Row expression is of type " + expression.getClass().getSimpleName() + ", must be a VariableReferenceExpression or a ConstantExpression.");
        //all variables start out by default in their own virtual singleton class
        return equivalenceClassHeads.getOrDefault(expression, expression);
    }

    /**
     * Returns the equivalence classes members for the given equivalence class head.
     * <p>
     * Note that the provided head could be that of a new equivalence class in which case
     * an empty member list is returned.
     *
     * @param head
     * @return The equivalence class members for the given equivalence class head.
     */
    public List<RowExpression> getEquivalenceClasses(RowExpression head)
    {
        requireNonNull(head, "head is null");
        checkArgument((head instanceof VariableReferenceExpression || head instanceof ConstantExpression),
                "Row expression is of type " + head.getClass().getSimpleName() + ", must be a VariableReferenceExpression or a ConstantExpression.");

        return equivalenceClasses.getOrDefault(head, new ArrayList<>());
    }

    /**
     * Updates this equivalence class property with the equivalences of another equivalence class property.
     *
     * @param equivalenceClassProperty
     */
    public void update(EquivalenceClassProperty equivalenceClassProperty)
    {
        requireNonNull(equivalenceClassProperty, "equivalenceClassProperty is null");

        equivalenceClassProperty.equivalenceClasses.forEach((head, members) -> members.forEach(member -> updateInternal(head, member)));
    }

    /**
     * Updates this equivalence class property with "variable reference = variable reference" or
     * "variable reference = constant reference" conjuncts applied by the provided predicate.
     * Returns true if any equivalence class heads changed, and false otherwise.
     *
     * @param predicate
     * @return
     */
    public boolean update(RowExpression predicate)
    {
        requireNonNull(predicate, "predicate is null");
        updated = false;
        //TODO tunnel through CAST functions?
        extractConjuncts(predicate).stream()
                .filter(CallExpression.class::isInstance)
                .map(CallExpression.class::cast)
                .filter(this::isVariableEqualVariableOrConstant)
                .forEach(e -> updateInternal(e.getArguments().get(0), e.getArguments().get(1)));
        return updated;
    }

    private boolean isVariableEqualVariableOrConstant(RowExpression expression)
    {
        if (expression instanceof CallExpression
                && functionResolution.isEqualFunction(((CallExpression) expression).getFunctionHandle())
                && ((CallExpression) expression).getArguments().size() == 2) {
            RowExpression e1 = ((CallExpression) expression).getArguments().get(0);
            RowExpression e2 = ((CallExpression) expression).getArguments().get(1);

            return e1 instanceof VariableReferenceExpression && (e2 instanceof VariableReferenceExpression || e2 instanceof ConstantExpression) || e2 instanceof VariableReferenceExpression && e1 instanceof ConstantExpression;
        }
        return false;
    }

    public void update(RowExpression firstExpression, RowExpression secondExpression)
    {
        updateInternal(firstExpression, secondExpression);
    }

    /**
     * Updates this equivalence class property with pairs of variable or column references deemed
     * equivalent via the application of predicates.
     * Side effect is that it sets updated instance variable true if any equivalence class heads changed.
     * This can be used to optimize methods that maintain alignment with equivalence classes.
     *
     * @param firstExpression
     * @param secondExpression
     */
    private void updateInternal(RowExpression firstExpression, RowExpression secondExpression)
    {
        RowExpression head1 = getEquivalenceClassHead(firstExpression);
        RowExpression head2 = getEquivalenceClassHead(secondExpression);

        //already in same equivalence class, nothing to do
        //note that we do not check head1.equal(head2) so that two different variable reference objects
        //referencing the same reference are both added to the equivalence class
        if (head1 == head2) {
            return;
        }

        updated = true;
        List<RowExpression> head1Class = getEquivalenceClasses(head1);
        List<RowExpression> head2Class = getEquivalenceClasses(head2);

        //pick new head and merge other class into head class
        RowExpression newHead = pickNewHead(head1, head2);
        if (newHead == head1) {
            combineClasses(head1, head1Class, head2, head2Class);
        }
        else {
            combineClasses(head2, head2Class, head1, head1Class);
        }
    }

    private RowExpression pickNewHead(RowExpression head1, RowExpression head2)
    {
        //always use constant as the head
        if (head2 instanceof ConstantExpression) {
            return head2;
        }
        return head1;
    }

    //combine an equivalence class with head class
    private void combineClasses(RowExpression head, List<RowExpression> headClass, RowExpression headOfOtherEqClass, List<RowExpression> otherEqClass)
    {
        //merge other eq class into head class
        headClass.addAll(otherEqClass);
        headClass.add(headOfOtherEqClass);
        //update the head of the other class members
        equivalenceClassHeads.put(headOfOtherEqClass, head);
        for (RowExpression expression : otherEqClass) {
            equivalenceClassHeads.put(expression, head);
        }
        equivalenceClasses.putIfAbsent(head, headClass);
        equivalenceClasses.remove(headOfOtherEqClass);
    }

    /**
     * Returns a projected version of this equivalence class property.
     * Variables in each class are mapped to output variables in the context beyond the project operation.
     * It is possible that this operation projects all members of a particular class.
     *
     * @param inverseVariableMappings
     * @return A projected version of this equivalence class property.
     */
    public EquivalenceClassProperty project(Map<VariableReferenceExpression, VariableReferenceExpression> inverseVariableMappings)
    {
        EquivalenceClassProperty projectedEquivalenceClassProperty = new EquivalenceClassProperty(functionResolution);
        for (Map.Entry<RowExpression, List<RowExpression>> entry : equivalenceClasses.entrySet()) {
            //first project the members of the current class
            List<RowExpression> projectedMembers = new ArrayList<>();
            for (RowExpression member : entry.getValue()) {
                if (inverseVariableMappings.containsKey(member)) {
                    RowExpression projectedMember = inverseVariableMappings.get(member);
                    if (!projectedMembers.contains(projectedMember)) {
                        projectedMembers.add(projectedMember);
                    }
                }
            }
            //boundary cases....
            // head projects but no members project -> trivial class, do not store
            // head does not project and one member projects -> trivial class, do not store
            // head does not project and more than one member projects -> pick a new head and update equivalence class
            if (!projectedMembers.isEmpty()) {
                RowExpression currentHead = entry.getKey();
                RowExpression projectedHead = currentHead;
                if ((currentHead instanceof VariableReferenceExpression)) {
                    if (inverseVariableMappings.containsKey(currentHead)) {
                        //head is not projected
                        projectedHead = inverseVariableMappings.get(currentHead);
                        projectedMembers.remove(projectedHead);
                    }
                    else {
                        //pick the first projected member as the new head
                        projectedHead = projectedMembers.get(0);
                        projectedMembers.remove(0);
                    }
                }

                RowExpression finalProjectedHead = projectedHead;
                projectedMembers.forEach(m -> projectedEquivalenceClassProperty.update(finalProjectedHead, m));
            }
        }
        return projectedEquivalenceClassProperty;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("EquivalenceClassHeads", String.join(",", equivalenceClassHeads.entrySet().stream().map(e -> e.getKey().toString() + ":" + e.getValue().toString()).collect(toImmutableList())))
                .add("EquivalenceClasses", String.join(",", equivalenceClasses.entrySet().stream().map(e -> e.getKey().toString() + ":" + e.getValue().toString()).collect(toImmutableList())))
                .toString();
    }
}
