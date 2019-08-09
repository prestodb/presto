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

import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.SpecialFormExpression.Form;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class LogicalRowExpressions
{
    public static final ConstantExpression TRUE_CONSTANT = new ConstantExpression(true, BOOLEAN);
    public static final ConstantExpression FALSE_CONSTANT = new ConstantExpression(false, BOOLEAN);
    // 10000 is very conservative estimation
    private static final int ELIMINATE_COMMON_SIZE_LIMIT = 10000;

    private final DeterminismEvaluator determinismEvaluator;
    private final StandardFunctionResolution functionResolution;
    private final FunctionMetadataManager functionMetadataManager;

    public LogicalRowExpressions(DeterminismEvaluator determinismEvaluator, StandardFunctionResolution functionResolution, FunctionMetadataManager functionMetadataManager)
    {
        this.determinismEvaluator = requireNonNull(determinismEvaluator, "determinismEvaluator is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
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
        return and(asList(expressions));
    }

    public static RowExpression and(Collection<RowExpression> expressions)
    {
        return binaryExpression(AND, expressions);
    }

    public static RowExpression or(RowExpression... expressions)
    {
        return or(asList(expressions));
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
                List<RowExpression> arguments = asList(queue.remove(), queue.remove());
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
        return combinePredicates(form, asList(expressions));
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
        return combineConjuncts(asList(expressions));
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
        return combineDisjuncts(asList(expressions));
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
     * This only applies to propositional logic and comparison. this utility cannot be applied to high-order logic.
     * Examples of non-applicable cases could be f(a AND b) > 5
     *
     * An applicable example:
     *
     * NOT
     * |
     * ___OR_                          AND
     * /      \                      /      \
     * NOT     OR        ==>        AND      AND
     * |     /  \                 /  \     /   \
     * AND   c   NOT              a    b   NOT  d
     * /  \        |                         |
     * a    b       d                         c
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
     * NOT
     * |
     * ___OR_                          AND
     * /      \                      /      \
     * NOT     OR        ==>        OR      AND
     * |     /  \                 /  \     /   \
     * OR   c   NOT              a    b   NOT  d
     * /  \        |                         |
     * a    b       d                         c
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
     * NOT                                    OR
     * |                                 /        \
     * ___OR_                          AND            AND
     * /      \                        /    \        /     \
     * NOT     OR        ==>          a     AND      b     AND
     * |     /  \                         /   \          /   \
     * OR   c   NOT                       NOT  d         NOT  d
     * /  \        |                         |              |
     * a    b       d                         c              c
     */
    public RowExpression convertToDisjunctiveNormalForm(RowExpression expression)
    {
        return convertToNormalForm(expression, OR);
    }

    public RowExpression convertToSmallestNormalForm(RowExpression expression)
    {
        RowExpression conjunctiveNormalForm = convertToConjunctiveNormalForm(expression);
        RowExpression disjunctiveNormalForm = convertToDisjunctiveNormalForm(expression);
        return numOfSubPredicates(conjunctiveNormalForm) > numOfSubPredicates(disjunctiveNormalForm) ? disjunctiveNormalForm : conjunctiveNormalForm;
    }

    public RowExpression convertToNormalForm(RowExpression expression, Form clauseJoiner)
    {
        return pushNegationToLeaves(expression).accept(new ConvertNormalFormVisitor(), rootContext(clauseJoiner));
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
                .collect(toList());

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

            checkArgument(call.getArguments().size() == 1, "Not expression should have exactly one argument");
            RowExpression argument = call.getArguments().get(0);

            // eliminate two consecutive negations
            if (isNegationExpression(argument)) {
                return ((CallExpression) argument).getArguments().get(0).accept(new PushNegationVisitor(), null);
            }

            if (isComparisonExpression(argument)) {
                return negateComparison((CallExpression) argument);
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
                return or(notCallExpression(left).accept(new PushNegationVisitor(), null), notCallExpression(right).accept(this, null));
            }
            // !(a OR b) ==> !a AND !b
            return and(notCallExpression(left).accept(new PushNegationVisitor(), null), notCallExpression(right).accept(this, null));
        }

        private RowExpression negateComparison(CallExpression expression)
        {
            OperatorType newOperator = negate(getOperator(expression).orElse(null));
            if (newOperator == null) {
                return new CallExpression("NOT", functionResolution.notFunction(), BOOLEAN, singletonList(expression));
            }
            checkArgument(expression.getArguments().size() == 2, "Comparison expression must have exactly two arguments");
            RowExpression left = expression.getArguments().get(0).accept(this, null);
            RowExpression right = expression.getArguments().get(1).accept(this, null);
            return new CallExpression(
                    newOperator.getOperator(),
                    functionResolution.comparisonFunction(newOperator, left.getType(), right.getType()),
                    BOOLEAN,
                    asList(left, right));
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
                return and(left.accept(this, null), right.accept(this, null));
            }
            return or(left.accept(this, null), right.accept(this, null));
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
    }

    private static ConvertNormalFormVisitorContext rootContext(Form clauseJoiner)
    {
        return new ConvertNormalFormVisitorContext(clauseJoiner, 0);
    }

    private static class ConvertNormalFormVisitorContext
    {
        private final Form clauseJoiner;
        private final int levelFromRoot;

        public ConvertNormalFormVisitorContext(Form clauseJoiner, int levelFromRoot)
        {
            this.clauseJoiner = clauseJoiner;
            this.levelFromRoot = levelFromRoot;
        }

        public ConvertNormalFormVisitorContext childContext()
        {
            return new ConvertNormalFormVisitorContext(clauseJoiner, levelFromRoot + 1);
        }
    }

    private class ConvertNormalFormVisitor
            implements RowExpressionVisitor<RowExpression, ConvertNormalFormVisitorContext>
    {
        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialFormExpression, ConvertNormalFormVisitorContext context)
        {
            if (!isConjunctionOrDisjunction(specialFormExpression)) {
                return specialFormExpression;
            }
            // Attempt to convert sub predicates to expected normal form, deduplicate and fold constants.
            RowExpression rewritten = combinePredicates(
                    specialFormExpression.getForm(),
                    extractPredicates(specialFormExpression.getForm(), specialFormExpression)
                            .stream()
                            .map(subPredicate -> subPredicate.accept(this, context.childContext()))
                            .collect(toList()));

            if (!isConjunctionOrDisjunction(rewritten)) {
                return rewritten;
            }

            SpecialFormExpression rewrittenSpecialForm = (SpecialFormExpression) rewritten;
            Form currentForm = rewrittenSpecialForm.getForm();
            List<List<RowExpression>> subPredicates = getSubPredicates(rewrittenSpecialForm);

            // TODO stop eliminate/extract if subPredicates size is huge.
            if (subPredicates.stream().mapToInt(List::size).sum() > ELIMINATE_COMMON_SIZE_LIMIT) {
                return combine(currentForm, subPredicates);
            }
            subPredicates = eliminateCommonPredicates(subPredicates);

            //extractCommonPredicates when currentForm is same as clauseJoiner can produce opposite form and potentially bigger predicate.
            List<List<RowExpression>> flippedSubPredicates = extractCommonPredicates(currentForm, subPredicates);
            if (flippedSubPredicates != null) {
                subPredicates = flippedSubPredicates;
                currentForm = flip(currentForm);
            }

            int currentSize = subPredicates.stream()
                    .mapToInt(List::size)
                    .sum();

            // The end result can be a cartesian cross product list of list
            int expectedSize;
            try {
                expectedSize = Math.multiplyExact(subPredicates.stream()
                        .mapToInt(List::size)
                        .reduce(1, Math::multiplyExact), subPredicates.size());
            }
            catch (ArithmeticException e) {
                return combine(currentForm, subPredicates);
            }

            // TODO if the non-deterministic operation only appears in the only sub-predicates that has size >1, we can still expand it.
            //  For example: a && b && c && (d || e) can still be expanded if d or e is non-deterministic.
            boolean deterministic = subPredicates.stream()
                    .flatMap(List::stream)
                    .allMatch(determinismEvaluator::isDeterministic);

            // If we have expression like (a || b || c), and we expect conjunct, we will not change the expression.
            // TODO we can change (a || b || c) to not(a) && not(b) && not(c)
            // Do not expand too much or if there is non-deterministic element or we already get expected clauseJoiner.
            if (currentForm == context.clauseJoiner || expectedSize == currentSize || expectedSize > 2 * currentSize || !deterministic || context.levelFromRoot > 0) {
                return combine(currentForm, subPredicates);
            }

            // else, we expand and rewrite based on distributive property of Boolean algebra, for example
            // (l1 OR l2) AND (r1 OR r2) <=> (l1 AND r1) OR (l1 AND r2) OR (l2 AND r1) OR (l2 AND r2)
            subPredicates = crossProduct(subPredicates);
            return combine(context.clauseJoiner, subPredicates);
        }

        @Override
        public RowExpression visitCall(CallExpression call, ConvertNormalFormVisitorContext context)
        {
            return call;
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, ConvertNormalFormVisitorContext context)
        {
            return reference;
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, ConvertNormalFormVisitorContext context)
        {
            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, ConvertNormalFormVisitorContext context)
        {
            return lambda;
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, ConvertNormalFormVisitorContext context)
        {
            return reference;
        }
    }

    private boolean isNegationExpression(RowExpression expression)
    {
        return expression instanceof CallExpression && ((CallExpression) expression).getFunctionHandle().equals(functionResolution.notFunction());
    }

    private boolean isComparisonExpression(RowExpression expression)
    {
        return expression instanceof CallExpression && functionResolution.isComparisonFunction(((CallExpression) expression).getFunctionHandle());
    }

    /**
     * Extract the subPredicates as list of list in which the outer level has same conjunctive/disjunctive form as original predicate and inner level has opposite form.
     * For example, (a or b) and (a or c) or ( a or c) returns [[a,b], [a,c], [a,c]]
     */
    private List<List<RowExpression>> getSubPredicates(SpecialFormExpression expression)
    {
        return extractPredicates(expression.getForm(), expression).stream()
                .map(predicate -> isConjunctionOrDisjunction(predicate) ?
                        extractPredicates(predicate) : singletonList(predicate))
                .collect(toList());
    }

    private int numOfSubPredicates(RowExpression expression)
    {
        if (expression instanceof SpecialFormExpression) {
            return getSubPredicates((SpecialFormExpression) expression).stream()
                    .mapToInt(List::size)
                    .sum();
        }
        return 1;
    }

    /**
     * Eliminate a sub predicate if its sub predicates contain its peer.
     * For example: (a || b) && a = a, (a && b) || b = b
     */
    private List<List<RowExpression>> eliminateCommonPredicates(List<List<RowExpression>> predicates)
    {
        if (predicates.size() < 2) {
            return predicates;
        }
        int[] reduceTo = IntStream.range(0, predicates.size()).toArray();
        for (int i = 0; i < predicates.size(); i++) {
            // Do not eliminate predicates contain non-deterministic value
            // (a || b) && a should be kept same if a is non-deterministic.
            // TODO We can eliminate (a || b) && a if a is deterministic even b is not.
            if (predicates.get(i).stream().allMatch(determinismEvaluator::isDeterministic)) {
                for (int j = 0; j < predicates.size(); j++) {
                    if (isSuperSet(predicates.get(reduceTo[i]), predicates.get(j))) {
                        reduceTo[i] = j; //prefer smaller set
                    }
                    else if (isSameSet(predicates.get(reduceTo[i]), predicates.get(j))) {
                        reduceTo[i] = Integer.min(reduceTo[i], j); //prefer predicates that appears earlier.
                    }
                }
            }
        }

        return unmodifiableList(Arrays.stream(reduceTo).distinct().boxed()
                .map(predicates::get)
                .collect(toList()));
    }

    /**
     * Eliminate a sub predicate if its sub predicates contain its peer. Will return null if cannot extract common predicates otherwise return a nested list with flipped form
     * For example:
     * (a || b || c || d) && (a || b || e || f)  -> a || b || ((c || d) && (e || f))
     * (a || b) && (c || d) -> null
     */
    private List<List<RowExpression>> extractCommonPredicates(Form rootForm, List<List<RowExpression>> predicates)
    {
        if (predicates.isEmpty()) {
            return null;
        }
        Set<RowExpression> commonPredicates = new LinkedHashSet<>(predicates.get(0));
        predicates.forEach(commonPredicates::retainAll);

        if (commonPredicates.isEmpty()) {
            return null;
        }
        List<RowExpression> remainingPredicates = unmodifiableList(predicates
                .stream()
                .map(subPredicates ->
                        combinePredicates(flip(rootForm),
                                subPredicates
                                        .stream()
                                        .filter(predicate -> !commonPredicates.contains(predicate))
                                        .collect(toList())))
                .collect(toList()));
        return Stream.concat(commonPredicates.stream().map(predicate -> singletonList(predicate)), Stream.of(remainingPredicates))
                .collect(toList());
    }

    private RowExpression combine(Form clause, List<List<RowExpression>> predicates)
    {
        return combinePredicates(clause, predicates.stream()
                .map(predicate -> combinePredicates(flip(clause), predicate))
                .collect(toList()));
    }

    /**
     * Cartesian cross product of List of List.
     * For example, [[a], [b, c], [d]] becomes [[a,b,d], [a,c,d]]
     */
    private List<List<RowExpression>> crossProduct(List<List<RowExpression>> lists)
    {
        checkArgument(lists.size() > 0, "Must contain more than one child");
        List<List<RowExpression>> result = lists.get(0).stream().map(Collections::singletonList).collect(toList());
        for (int i = 1; i < lists.size(); i++) {
            result = crossProduct(result, lists.get(i));
        }
        return result;
    }

    private List<List<RowExpression>> crossProduct(List<List<RowExpression>> left, List<RowExpression> right)
    {
        List<List<RowExpression>> result = new ArrayList<>();
        for (List<RowExpression> combined : left) {
            for (RowExpression toAdd : right) {
                List<RowExpression> newLine = new ArrayList<>(combined);
                newLine.add(toAdd);
                result.add(newLine);
            }
        }
        return result;
    }

    private static Form flip(Form binaryLogicalOperation)
    {
        switch (binaryLogicalOperation) {
            case AND:
                return OR;
            case OR:
                return AND;
        }
        throw new UnsupportedOperationException("Invalid binary logical operation");
    }

    private Optional<OperatorType> getOperator(RowExpression expression)
    {
        if (expression instanceof CallExpression) {
            return functionMetadataManager.getFunctionMetadata(((CallExpression) expression).getFunctionHandle()).getOperatorType();
        }
        return Optional.empty();
    }

    private RowExpression notCallExpression(RowExpression argument)
    {
        return new CallExpression("not", functionResolution.notFunction(), BOOLEAN, singletonList(argument));
    }

    private static OperatorType negate(OperatorType operator)
    {
        switch (operator) {
            case EQUAL:
                return NOT_EQUAL;
            case NOT_EQUAL:
                return EQUAL;
            case GREATER_THAN:
                return LESS_THAN_OR_EQUAL;
            case LESS_THAN:
                return GREATER_THAN_OR_EQUAL;
            case LESS_THAN_OR_EQUAL:
                return GREATER_THAN;
            case GREATER_THAN_OR_EQUAL:
                return LESS_THAN;
        }
        return null;
    }

    private static void checkArgument(boolean condition, String message, Object... arguments)
    {
        if (!condition) {
            throw new IllegalArgumentException(String.format(message, arguments));
        }
    }

    private static <T> boolean isSuperSet(Collection<T> a, Collection<T> b)
    {
        // We assumes a, b both are de-duplicated collections.
        return a.size() > b.size() && a.containsAll(b);
    }

    private static <T> boolean isSameSet(Collection<T> a, Collection<T> b)
    {
        // We assumes a, b both are de-duplicated collections.
        return a.size() == b.size() && a.containsAll(b) && b.containsAll(a);
    }
}
