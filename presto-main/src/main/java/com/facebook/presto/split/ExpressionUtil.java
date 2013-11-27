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
package com.facebook.presto.split;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.InConstantRangePredicate;
import com.facebook.presto.spi.UncertainRangeConstant;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.collect.ImmutableList;
import com.google.common.base.Optional;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;

import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.tree.ComparisonExpression.matchesPattern;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Predicates.or;

public final class ExpressionUtil
{
    private ExpressionUtil()
    {
    }

    /**
     * Extract constant values from expression.
     *
     * @return the constant values or absent if expression will always be false
     */
    public static Optional<Map<ColumnHandle, Object>> extractConstantValues(Expression predicate, Map<Symbol, ColumnHandle> symbolToColumnName)
    {
        // Look for any sub-expression in an AND expression of the form <partition key> = 'value'
        Set<ComparisonExpression> comparisons = IterableTransformer.on(extractConjuncts(predicate))
                .select(instanceOf(ComparisonExpression.class))
                .cast(ComparisonExpression.class)
                .select(or(
                        matchesPattern(ComparisonExpression.Type.EQUAL, QualifiedNameReference.class, StringLiteral.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, StringLiteral.class, QualifiedNameReference.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, QualifiedNameReference.class, LongLiteral.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, LongLiteral.class, QualifiedNameReference.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, QualifiedNameReference.class, DoubleLiteral.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, DoubleLiteral.class, QualifiedNameReference.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, QualifiedNameReference.class, BooleanLiteral.class),
                        matchesPattern(ComparisonExpression.Type.EQUAL, BooleanLiteral.class, QualifiedNameReference.class)))
                .set();

        final Map<ColumnHandle, Object> bindings = new HashMap<>();
        for (ComparisonExpression comparison : comparisons) {
            // Record binding if condition is an equality comparison over a partition key
            QualifiedNameReference reference = extractReference(comparison);
            Symbol symbol = Symbol.fromQualifiedName(reference.getName());

            ColumnHandle column = symbolToColumnName.get(symbol);
            if (column != null) {
                Literal literal = extractLiteral(comparison);

                Object value;
                if (literal instanceof DoubleLiteral) {
                    value = ((DoubleLiteral) literal).getValue();
                }
                else if (literal instanceof LongLiteral) {
                    value = ((LongLiteral) literal).getValue();
                }
                else if (literal instanceof StringLiteral) {
                    value = ((StringLiteral) literal).getValue();
                }
                else if (literal instanceof BooleanLiteral) {
                    value = ((BooleanLiteral) literal).getValue();
                }
                else {
                    throw new AssertionError(String.format("Literal type (%s) not currently handled", literal.getClass().getName()));
                }

                // if there is a different constant value already bound for this column, the expression will always be false
                Object previous = bindings.get(column);
                if (previous != null && !previous.equals(value)) {
                    return Optional.absent();
                }
                bindings.put(column, value);
            }
        }
        return Optional.of(bindings);
    }

    private static final InConstantRangePredicate NO_RANGE_PREDICATE = null;

    public static Map<ColumnHandle, InConstantRangePredicate> extractConstantRanges(Expression predicate, Map<Symbol, ColumnHandle> symbolToColumnName)
    {
        final Map<ColumnHandle, InConstantRangePredicate> ranges = new HashMap<>();
        for (Map.Entry<Symbol, ColumnHandle> entry : symbolToColumnName.entrySet()) {
            InConstantRangePredicate range = extractConstantRange(entry.getKey(), predicate);
            if (range != NO_RANGE_PREDICATE) {
                ranges.put(entry.getValue(), range);
            }
        }

        return ranges;
    }

    private static InConstantRangePredicate extractConstantRange(Symbol symbol, Expression expression) {
        if(expression instanceof LogicalBinaryExpression) {
            LogicalBinaryExpression logic = (LogicalBinaryExpression) expression;
            switch (logic.getType()) {
                case AND:
                    return andConstantRange(
                            extractConstantRange(symbol, logic.getLeft()),
                            extractConstantRange(symbol, logic.getRight()));
                case OR:
                    return orConstantRange(
                            extractConstantRange(symbol, logic.getLeft()),
                            extractConstantRange(symbol, logic.getRight()));
                default:
                    return NO_RANGE_PREDICATE;
            }
        }
        else if (expression instanceof ComparisonExpression) {
            ComparisonExpression comparison = (ComparisonExpression) expression;
            ComparisonExpression.Type operator;
            Expression rightValue;

            if (comparison.getLeft() instanceof QualifiedNameReference) {
                QualifiedNameReference reference = (QualifiedNameReference) comparison.getLeft();
                if (reference == null || !Symbol.fromQualifiedName(reference.getName()).equals(symbol)) {
                    return NO_RANGE_PREDICATE;
                }
                operator = comparison.getType();
                rightValue = comparison.getRight();
            }
            else if (comparison.getRight() instanceof QualifiedNameReference) {
                QualifiedNameReference reference = (QualifiedNameReference) comparison.getRight();
                if (reference == null || !Symbol.fromQualifiedName(reference.getName()).equals(symbol)) {
                    return NO_RANGE_PREDICATE;
                }
                // flip comparison operator
                switch (comparison.getType()) {
                    case EQUAL:
                    case NOT_EQUAL:
                    case IS_DISTINCT_FROM:
                        operator = comparison.getType();
                        break;
                    case LESS_THAN:
                        operator = ComparisonExpression.Type.GREATER_THAN;
                        break;
                    case LESS_THAN_OR_EQUAL:
                        operator = ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
                        break;
                    case GREATER_THAN:
                        operator = ComparisonExpression.Type.LESS_THAN;
                        break;
                    case GREATER_THAN_OR_EQUAL:
                        operator = ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
                        break;
                    default:
                        return NO_RANGE_PREDICATE;
                }
                rightValue = comparison.getLeft();
            }
            else {
                // TODO here can evaluate other expressions including functions if the
                // expression returns boolean and it limits the possible value range
                // of the symbol.
                return NO_RANGE_PREDICATE;
            }

            UncertainRangeConstant constant = evaluateExpressionToUncertainLiteral(rightValue);
            if (constant == null) {
                return NO_RANGE_PREDICATE;
            }

            switch (operator) {
                case EQUAL:
                    return new InConstantRangePredicate(
                            constant.getType(),
                            ImmutableList.<UncertainRangeConstant>of(constant));

                case LESS_THAN:
                    return new InConstantRangePredicate(
                            constant.getType(),
                            ImmutableList.<UncertainRangeConstant>of(
                                constant.lowerBound().withUpperBoundary(false)));

                case LESS_THAN_OR_EQUAL:
                    return new InConstantRangePredicate(
                            constant.getType(),
                            ImmutableList.<UncertainRangeConstant>of(
                                constant.lowerBound().withUpperBoundary(constant.includesLowerBoundary())));

                case GREATER_THAN:
                    return new InConstantRangePredicate(
                            constant.getType(),
                            ImmutableList.<UncertainRangeConstant>of(
                                constant.upperBound().withLowerBoundary(false)));

                case GREATER_THAN_OR_EQUAL:
                    return new InConstantRangePredicate(
                            constant.getType(),
                            ImmutableList.<UncertainRangeConstant>of(
                                constant.upperBound().withLowerBoundary(constant.includesLowerBoundary())));

                case NOT_EQUAL:
                case IS_DISTINCT_FROM:
                default:
                    return NO_RANGE_PREDICATE;
            }
        }
        else {
            return NO_RANGE_PREDICATE;
        }
    }

    private static UncertainRangeConstant evaluateExpressionToUncertainLiteral(Expression expression)
    {
        if (expression instanceof Literal) {
            if (expression instanceof LongLiteral) {
                LongLiteral literal = (LongLiteral) expression;
                return new LongUncertainRangeConstant(literal.getValue(), true, literal.getValue(), true);
            }
            if (expression instanceof StringLiteral) {
                StringLiteral literal = (StringLiteral) expression;
                return new StringUncertainRangeConstant(literal.getValue(), true, literal.getValue(), true);
            }
            // other literals are not implemented
            return null;
        }

        // TODO here can evaluate other expressions including functions if the expression
        // represents a value with limited upper bound and/or limited lower bound.

        return null;
    }

    private static InConstantRangePredicate orConstantRange(InConstantRangePredicate left, InConstantRangePredicate right) {
        if (left == NO_RANGE_PREDICATE || right == NO_RANGE_PREDICATE) {
            return NO_RANGE_PREDICATE;
        }

        if (left.getType() != right.getType()) {
            // giveup to analyze this condition
            return NO_RANGE_PREDICATE;
        }

        return new InConstantRangePredicate(
                left.getType(),
                new ImmutableList.Builder<UncertainRangeConstant>()
                        .addAll(left.getPossibleRanges())
                        .addAll(right.getPossibleRanges()).build());
    }

    private static InConstantRangePredicate andConstantRange(InConstantRangePredicate left, InConstantRangePredicate right) {
        if (left == NO_RANGE_PREDICATE) {
            return right;
        }
        else if (right == NO_RANGE_PREDICATE) {
            return left;
        }

        if (left.getType() != right.getType()) {
            // giveup to analyze this condition
            return NO_RANGE_PREDICATE;
        }

        ArrayList<UncertainRangeConstant> possibleRanges = new ArrayList<>(left.getPossibleRanges().size());
        for (UncertainRangeConstant range : left.getPossibleRanges()) {
            for (UncertainRangeConstant filter : right.getPossibleRanges()) {
                possibleRanges.add(range.intersection(filter));
            }
        }

        return new InConstantRangePredicate(left.getType(), possibleRanges);
    }

    private static Literal extractLiteral(ComparisonExpression expression)
    {
        if (expression.getLeft() instanceof Literal) {
            return (Literal) expression.getLeft();
        }
        else if (expression.getRight() instanceof Literal) {
            return (Literal) expression.getRight();
        }

        throw new IllegalArgumentException("Comparison does not have a child of type Literal");
    }

    private static QualifiedNameReference extractReference(ComparisonExpression expression)
    {
        if (expression.getLeft() instanceof QualifiedNameReference) {
            return (QualifiedNameReference) expression.getLeft();
        }
        else if (expression.getRight() instanceof QualifiedNameReference) {
            return (QualifiedNameReference) expression.getRight();
        }

        throw new IllegalArgumentException("Comparison does not have a child of type QualifiedNameReference");
    }
}
