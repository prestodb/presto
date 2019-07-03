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
package com.facebook.presto.spi.relation;

import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.SpecialFormExpression.Form;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class LogicalRowExpressions
{
    public static final ConstantExpression TRUE_CONSTANT = new ConstantExpression(true, BOOLEAN);
    public static final ConstantExpression FALSE_CONSTANT = new ConstantExpression(false, BOOLEAN);

    private final DeterminismEvaluator determinismEvaluator;
    private final StandardFunctionResolution functionResolution;

    public LogicalRowExpressions(DeterminismEvaluator determinismEvaluator, StandardFunctionResolution functionResolution)
    {
        this.determinismEvaluator = requireNonNull(determinismEvaluator, "determinismEvaluator is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
    }

    public static List<RowExpression> extractConjuncts(RowExpression expression)
    {
        return extractPredicates(AND, expression);
    }

    public static List<RowExpression> extractDisjuncts(RowExpression expression)
    {
        return extractPredicates(OR, expression);
    }

    public static List<RowExpression> extractPredicates(RowExpression expression)
    {
        if (expression instanceof SpecialFormExpression) {
            Form form = ((SpecialFormExpression) expression).getForm();
            if (form == AND || form == OR) {
                return extractPredicates(form, expression);
            }
        }
        return singletonList(expression);
    }

    public static List<RowExpression> extractPredicates(Form form, RowExpression expression)
    {
        if (expression instanceof SpecialFormExpression && ((SpecialFormExpression) expression).getForm() == form) {
            SpecialFormExpression specialFormExpression = (SpecialFormExpression) expression;
            if (specialFormExpression.getArguments().size() != 2) {
                throw new IllegalStateException("logical binary expression requires exactly 2 operands");
            }

            List<RowExpression> predicates = new ArrayList<>();
            predicates.addAll(extractPredicates(form, specialFormExpression.getArguments().get(0)));
            predicates.addAll(extractPredicates(form, specialFormExpression.getArguments().get(1)));
            return unmodifiableList(predicates);
        }

        return singletonList(expression);
    }

    public static RowExpression and(RowExpression... expressions)
    {
        return and(Arrays.asList(expressions));
    }

    public static RowExpression and(Collection<RowExpression> expressions)
    {
        return binaryExpression(AND, expressions);
    }

    public static RowExpression or(RowExpression... expressions)
    {
        return or(Arrays.asList(expressions));
    }

    public static RowExpression or(Collection<RowExpression> expressions)
    {
        return binaryExpression(OR, expressions);
    }

    public static RowExpression binaryExpression(Form form, Collection<RowExpression> expressions)
    {
        requireNonNull(form, "operator is null");
        requireNonNull(expressions, "expressions is null");

        if (expressions.isEmpty()) {
            switch (form) {
                case AND:
                    return TRUE_CONSTANT;
                case OR:
                    return FALSE_CONSTANT;
                default:
                    throw new IllegalArgumentException("Unsupported binary expression operator");
            }
        }

        // Build balanced tree for efficient recursive processing that
        // preserves the evaluation order of the input expressions.
        //
        // The tree is built bottom up by combining pairs of elements into
        // binary AND expressions.
        //
        // Example:
        //
        // Initial state:
        //  a b c d e
        //
        // First iteration:
        //
        //  /\    /\   e
        // a  b  c  d
        //
        // Second iteration:
        //
        //    / \    e
        //  /\   /\
        // a  b c  d
        //
        //
        // Last iteration:
        //
        //      / \
        //    / \  e
        //  /\   /\
        // a  b c  d

        Queue<RowExpression> queue = new ArrayDeque<>(expressions);
        while (queue.size() > 1) {
            Queue<RowExpression> buffer = new ArrayDeque<>();

            // combine pairs of elements
            while (queue.size() >= 2) {
                List<RowExpression> arguments = Arrays.asList(queue.remove(), queue.remove());
                buffer.add(new SpecialFormExpression(form, BOOLEAN, arguments));
            }

            // if there's and odd number of elements, just append the last one
            if (!queue.isEmpty()) {
                buffer.add(queue.remove());
            }

            // continue processing the pairs that were just built
            queue = buffer;
        }

        return queue.remove();
    }

    public RowExpression combinePredicates(Form form, RowExpression... expressions)
    {
        return combinePredicates(form, Arrays.asList(expressions));
    }

    public RowExpression combinePredicates(Form form, Collection<RowExpression> expressions)
    {
        if (form == AND) {
            return combineConjuncts(expressions);
        }
        return combineDisjuncts(expressions);
    }

    public RowExpression combineConjuncts(RowExpression... expressions)
    {
        return combineConjuncts(Arrays.asList(expressions));
    }

    public RowExpression combineConjuncts(Collection<RowExpression> expressions)
    {
        requireNonNull(expressions, "expressions is null");

        List<RowExpression> conjuncts = expressions.stream()
                .flatMap(e -> extractConjuncts(e).stream())
                .filter(e -> !e.equals(TRUE_CONSTANT))
                .collect(toList());

        conjuncts = removeDuplicates(conjuncts);

        if (conjuncts.contains(FALSE_CONSTANT)) {
            return FALSE_CONSTANT;
        }

        return and(conjuncts);
    }

    public RowExpression combineDisjuncts(RowExpression... expressions)
    {
        return combineDisjuncts(Arrays.asList(expressions));
    }

    public RowExpression combineDisjuncts(Collection<RowExpression> expressions)
    {
        return combineDisjunctsWithDefault(expressions, FALSE_CONSTANT);
    }

    public RowExpression combineDisjunctsWithDefault(Collection<RowExpression> expressions, RowExpression emptyDefault)
    {
        requireNonNull(expressions, "expressions is null");

        List<RowExpression> disjuncts = expressions.stream()
                .flatMap(e -> extractDisjuncts(e).stream())
                .filter(e -> !e.equals(FALSE_CONSTANT))
                .collect(toList());

        disjuncts = removeDuplicates(disjuncts);

        if (disjuncts.contains(TRUE_CONSTANT)) {
            return TRUE_CONSTANT;
        }

        return disjuncts.isEmpty() ? emptyDefault : or(disjuncts);
    }

    /**
     * Given a logical expression, the goal is to push negation to the leaf nodes.
     * This only applies to propositional logic. this utility cannot be applied to high-order logic.
     * Examples of non-applicable cases could be f(a AND b) > 5
     *
     * An applicable example:
     *
     *        NOT
     *         |
     *      ___OR_                          AND
     *     /      \                      /      \
     *    NOT     OR        ==>        AND      AND
     *     |     /  \                 /  \     /   \
     *    AND   c   NOT              a    b   NOT  d
     *   /  \        |                         |
     *  a    b       d                         c
     */
    public RowExpression pushNegationToLeaves(RowExpression expression)
    {
        return expression.accept(new PushNegationVisitor(), null);
    }

    /**
     * Given a logical expression, the goal is to convert to conjuctive normal form (CNF).
     * This requires making a call to `pushNegationToLeaves`. There is no guarantee as to
     * the balance of the resulting expression tree.
     *
     * This only applies to propositional logic. this utility cannot be applied to high-order logic.
     * Examples of non-applicable cases could be f(a AND b) > 5
     *
     * NOTE: This may exponentially increase the number of RowExpressions in the expression.
     *
     * An applicable example:
     *
     *        NOT
     *         |
     *      ___OR_                          AND
     *     /      \                      /      \
     *    NOT     OR        ==>        OR      AND
     *     |     /  \                 /  \     /   \
     *    OR   c   NOT              a    b   NOT  d
     *   /  \        |                         |
     *  a    b       d                         c
     */
    public RowExpression convertToConjunctiveNormalForm(RowExpression expression)
    {
        return convertToNormalForm(expression, AND);
    }

    /**
     * Given a logical expression, the goal is to convert to disjunctive normal form (DNF).
     * The same limitations, format, and risks apply as for converting to conjunctive normal form (CNF).
     *
     * An applicable example:
     *
     *        NOT                                    OR
     *         |                                 /        \
     *      ___OR_                          AND            AND
     *     /      \                        /    \        /     \
     *    NOT     OR        ==>          a     AND      b     AND
     *     |     /  \                         /   \          /   \
     *    OR   c   NOT                       NOT  d         NOT  d
     *   /  \        |                         |              |
     *  a    b       d                         c              c
     */
    public RowExpression convertToDisjunctiveNormalForm(RowExpression expression)
    {
        return convertToNormalForm(expression, OR);
    }

    public RowExpression convertToNormalForm(RowExpression expression, Form clauseJoiner)
    {
        return pushNegationToLeaves(expression).accept(new ConvertNormalFormVisitor(), clauseJoiner);
    }

    public RowExpression filterDeterministicConjuncts(RowExpression expression)
    {
        return filterConjuncts(expression, this.determinismEvaluator::isDeterministic);
    }

    public RowExpression filterNonDeterministicConjuncts(RowExpression expression)
    {
        return filterConjuncts(expression, predicate -> !this.determinismEvaluator.isDeterministic(predicate));
    }

    public RowExpression filterConjuncts(RowExpression expression, Predicate<RowExpression> predicate)
    {
        List<RowExpression> conjuncts = extractConjuncts(expression).stream()
                .filter(predicate)
                .collect(Collectors.toList());

        return combineConjuncts(conjuncts);
    }

    /**
     * Removes duplicate deterministic expressions. Preserves the relative order
     * of the expressions in the list.
     */
    private List<RowExpression> removeDuplicates(List<RowExpression> expressions)
    {
        Set<RowExpression> seen = new HashSet<>();

        List<RowExpression> result = new ArrayList<>();
        for (RowExpression expression : expressions) {
            if (!determinismEvaluator.isDeterministic(expression)) {
                result.add(expression);
            }
            else if (!seen.contains(expression)) {
                result.add(expression);
                seen.add(expression);
            }
        }

        return unmodifiableList(result);
    }

    private boolean isConjunctionOrDisjunction(RowExpression expression)
    {
        if (expression instanceof SpecialFormExpression) {
            Form form = ((SpecialFormExpression) expression).getForm();
            return form == AND || form == OR;
        }
        return false;
    }

    private final class PushNegationVisitor
            implements RowExpressionVisitor<RowExpression, Void>
    {
        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            if (!isNegationExpression(call)) {
                return call;
            }

            RowExpression argument = call.getArguments().get(0);

            // eliminate two consecutive negations
            if (isNegationExpression(argument)) {
                return ((CallExpression) argument).getArguments().get(0).accept(new PushNegationVisitor(), null);
            }

            if (!isConjunctionOrDisjunction(argument)) {
                return call;
            }

            // push negation through conjunction or disjunction
            SpecialFormExpression specialForm = ((SpecialFormExpression) argument);
            RowExpression left = specialForm.getArguments().get(0);
            RowExpression right = specialForm.getArguments().get(1);
            if (specialForm.getForm() == AND) {
                // !(a AND b) ==> !a OR !b
                return or(notCallExpression(left).accept(new PushNegationVisitor(), null), notCallExpression(right).accept(new PushNegationVisitor(), null));
            }
            // !(a OR b) ==> !a AND !b
            return and(notCallExpression(left).accept(new PushNegationVisitor(), null), notCallExpression(right).accept(new PushNegationVisitor(), null));
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            if (!isConjunctionOrDisjunction(specialForm)) {
                return specialForm;
            }

            RowExpression left = specialForm.getArguments().get(0);
            RowExpression right = specialForm.getArguments().get(1);

            if (specialForm.getForm() == AND) {
                return and(left.accept(new PushNegationVisitor(), null), right.accept(new PushNegationVisitor(), null));
            }
            return or(left.accept(new PushNegationVisitor(), null), right.accept(new PushNegationVisitor(), null));
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Void context)
        {
            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return lambda;
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return reference;
        }

        private boolean isNegationExpression(RowExpression expression)
        {
            return expression instanceof CallExpression && ((CallExpression) expression).getFunctionHandle().equals(functionResolution.notFunction());
        }

        private RowExpression notCallExpression(RowExpression argument)
        {
            return new CallExpression("not", functionResolution.notFunction(), BOOLEAN, singletonList(argument));
        }
    }

    private class ConvertNormalFormVisitor
            implements RowExpressionVisitor<RowExpression, Form>
    {
        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Form clauseJoiner)
        {
            if (!isConjunctionOrDisjunction(specialForm)) {
                return specialForm;
            }

            // normalize arguments
            RowExpression left = specialForm.getArguments().get(0).accept(new ConvertNormalFormVisitor(), clauseJoiner);
            RowExpression right = specialForm.getArguments().get(1).accept(new ConvertNormalFormVisitor(), clauseJoiner);

            // check if already in correct form
            if (specialForm.getForm() == clauseJoiner) {
                return clauseJoiner == AND ? and(left, right) : or(left, right);
            }

            // else, we expand and rewrite based on distributive property of Boolean algebra, for example
            // (l1 OR l2) AND (r1 OR r2) <=> (l1 AND r1) OR (l1 AND r2) OR (l2 AND r1) OR (l2 AND r2)
            Form termJoiner = clauseJoiner == AND ? OR : AND;
            List<RowExpression> leftClauses = extractPredicates(clauseJoiner, left);
            List<RowExpression> rightClauses = extractPredicates(clauseJoiner, right);
            List<RowExpression> permutationClauses = new ArrayList<>();
            for (RowExpression leftClause : leftClauses) {
                for (RowExpression rightClause : rightClauses) {
                    permutationClauses.add(termJoiner == AND ? and(leftClause, rightClause) : or(leftClause, rightClause));
                }
            }
            return combinePredicates(clauseJoiner, unmodifiableList(permutationClauses));
        }

        @Override
        public RowExpression visitCall(CallExpression call, Form clauseJoiner)
        {
            return call;
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, SpecialFormExpression.Form clauseJoiner)
        {
            return reference;
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, SpecialFormExpression.Form clauseJoiner)
        {
            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, SpecialFormExpression.Form clauseJoiner)
        {
            return lambda;
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, SpecialFormExpression.Form clauseJoiner)
        {
            return reference;
        }
    }
}
