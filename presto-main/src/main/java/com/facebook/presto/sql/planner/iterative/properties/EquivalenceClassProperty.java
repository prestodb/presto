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
import com.google.common.collect.ImmutableMap;
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
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

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
public final class EquivalenceClassProperty
{
    private final Map<RowExpression, RowExpression> equivalenceClassHeads;
    private final Map<RowExpression, List<RowExpression>> equivalenceClasses;

    public EquivalenceClassProperty()
    {
        this(ImmutableMap.of(), ImmutableMap.of());
    }

    public EquivalenceClassProperty(EquivalenceClassProperty equivalenceClassProperty)
    {
        this(equivalenceClassProperty.getEquivalenceClassHeads(), equivalenceClassProperty.getEquivalenceClasses());
    }

    public EquivalenceClassProperty(Map<RowExpression, RowExpression> equivalenceClassHeads, Map<RowExpression, List<RowExpression>> equivalenceClasses)
    {
        this.equivalenceClassHeads = ImmutableMap.copyOf(requireNonNull(equivalenceClassHeads, "equivalenceClassHeads is null"));
        this.equivalenceClasses = ImmutableMap.copyOf(requireNonNull(equivalenceClasses, "equivalenceClasses is null"));
    }

    public Map<RowExpression, List<RowExpression>> getEquivalenceClasses()
    {
        return equivalenceClasses;
    }

    public Map<RowExpression, RowExpression> getEquivalenceClassHeads()
    {
        return equivalenceClassHeads;
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
        checkArgument((head instanceof VariableReferenceExpression || head instanceof ConstantExpression),
                "Row expression is of type " + head.getClass().getSimpleName() + ", must be a VariableReferenceExpression or a ConstantExpression.");

        return equivalenceClasses.getOrDefault(head, new ArrayList<>());
    }

    /**
     * Updates thisEquivalenceClassProperty with the equivalences of otherEquivalenceClassProperty.
     *
     * @param equivalenceClassProperty
     */
    public EquivalenceClassProperty combineWith(EquivalenceClassProperty equivalenceClassProperty)
    {
        EquivalenceClassProperty newEquivalenceClassProperty = new EquivalenceClassProperty(equivalenceClassHeads, equivalenceClasses);

        for (Map.Entry<RowExpression, List<RowExpression>> equivalenceClass : equivalenceClassProperty.equivalenceClasses.entrySet()) {
            for (RowExpression member : equivalenceClass.getValue()) {
                newEquivalenceClassProperty = newEquivalenceClassProperty.combineWith(equivalenceClass.getKey(), member);
            }
        }

        return newEquivalenceClassProperty;
    }

    /**
     * Updates this equivalence class property with "variable reference = variable reference" or
     * "variable reference = constant reference" conjuncts applied by the provided predicate.
     * Returns true if any equivalence class heads changed, and false otherwise.
     *
     * @param predicate
     * @return
     */
    public EquivalenceClassProperty addPredicate(RowExpression predicate, FunctionResolution functionResolution)
    {
        //TODO tunnel through CAST functions?
        EquivalenceClassProperty newEquivalenceClassProperty = this;
        Set<CallExpression> callExprs = extractConjuncts(predicate)
                .stream()
                .filter(CallExpression.class::isInstance)
                .map(CallExpression.class::cast)
                .filter(e -> isVariableEqualVariableOrConstant(functionResolution, e))
                .collect(toImmutableSet());

        for (CallExpression callExpr : callExprs) {
            checkState(callExpr.getArguments().size() == 2, "callExpr must have 2 arguments");
            newEquivalenceClassProperty = newEquivalenceClassProperty.combineWith(callExpr.getArguments().get(0), callExpr.getArguments().get(1));
        }

        return newEquivalenceClassProperty;
    }

    private static boolean isVariableEqualVariableOrConstant(FunctionResolution functionResolution, RowExpression expression)
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

    /**
     * Updates this equivalence class property with pairs of variable or column references deemed
     * equivalent via the application of predicates.
     * Side effect is that it sets updated instance variable true if any equivalence class heads changed.
     * This can be used to optimize methods that maintain alignment with equivalence classes.
     *
     * @param firstExpression
     * @param secondExpression
     */
    public EquivalenceClassProperty combineWith(RowExpression firstExpression, RowExpression secondExpression)
    {
        RowExpression head1 = getEquivalenceClassHead(firstExpression);
        RowExpression head2 = getEquivalenceClassHead(secondExpression);

        //already in same equivalence class, nothing to do
        //note that we do not check head1.equal(head2) so that two different variable reference objects
        //referencing the same reference are both added to the equivalence class
        if (head1 == head2) {
            return this;
        }

        List<RowExpression> head1Class = getEquivalenceClasses(head1);
        List<RowExpression> head2Class = getEquivalenceClasses(head2);

        //pick new head and merge other class into head class
        RowExpression newHead = pickNewHead(head1, head2);
        ImmutableList.Builder<RowExpression> mergedEquivalenceClasses = ImmutableList.builder();
        mergedEquivalenceClasses
                .addAll(head1Class)
                .addAll(head2Class);

        if (newHead == head1) {
            //merge other eq class into head class
            return combineClasses(
                    head1,
                    mergedEquivalenceClasses
                            .add(head2)
                            .build(),
                    head2,
                    head2Class);
        }

        return combineClasses(
                head2,
                mergedEquivalenceClasses
                        .add(head1)
                        .build(),
                head1,
                head1Class);
    }

    private static RowExpression pickNewHead(RowExpression head1, RowExpression head2)
    {
        //always use constant as the head
        if (head2 instanceof ConstantExpression) {
            return head2;
        }
        return head1;
    }

    //combine an equivalence class with head class
    private EquivalenceClassProperty combineClasses(RowExpression head, List<RowExpression> headClass, RowExpression headOfOtherEqClass, List<RowExpression> otherEqClass)
    {
        Map<RowExpression, RowExpression> newEquivalenceClassHeads = new HashMap();
        newEquivalenceClassHeads.putAll(getEquivalenceClassHeads());
        newEquivalenceClassHeads.put(headOfOtherEqClass, head);

        otherEqClass.stream()
                .forEach(expression -> newEquivalenceClassHeads.put(expression, head));

        Map<RowExpression, List<RowExpression>> newEquivalenceClasses = equivalenceClasses
                .entrySet()
                .stream()
                .filter(e -> e.getKey() != headOfOtherEqClass)
                .collect(toMap(e -> e.getKey(), e -> e.getValue()));

        if (!equivalenceClasses.containsKey(head)) {
            newEquivalenceClasses.put(head, headClass);
        }
        else {
            // Merge the equivalence class lists
            List<RowExpression> currExprs = equivalenceClasses.get(head);
            ImmutableList.Builder<RowExpression> newExprs = ImmutableList.builder();
            newExprs.addAll(currExprs);
            for (RowExpression rowExpr : headClass) {
                if (!currExprs.contains(rowExpr)) {
                    newExprs.add(rowExpr);
                }
            }
            newEquivalenceClasses.put(head, newExprs.build());
        }

        return new EquivalenceClassProperty(newEquivalenceClassHeads, newEquivalenceClasses);
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
        EquivalenceClassProperty projectedEquivalenceClassProperty = new EquivalenceClassProperty();
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

                for (RowExpression rowExpr : projectedMembers) {
                    projectedEquivalenceClassProperty = projectedEquivalenceClassProperty.combineWith(finalProjectedHead, rowExpr);
                }
            }
        }
        return projectedEquivalenceClassProperty;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("EquivalenceClassHeads", String.join(",", equivalenceClassHeads.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(toImmutableList())))
                .add("EquivalenceClasses", String.join(",", equivalenceClasses.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(toImmutableList())))
                .toString();
    }
}
