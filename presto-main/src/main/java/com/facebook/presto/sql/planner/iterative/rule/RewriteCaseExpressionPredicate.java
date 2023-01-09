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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isOptimizeCaseExpressionPredicate;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.expressions.LogicalRowExpressions.replaceArguments;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.SWITCH;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.WHEN;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * This Rule rewrites a CASE expression predicate into a series of AND/OR clauses.
 * The following CASE expression
 * <p>
 * (CASE
 * WHEN expression=constant1 THEN result1
 * WHEN expression=constant2 THEN result2
 * WHEN expression=constant3 THEN result3
 * ELSE elseResult
 * END) = value
 * <p>
 * can be converted into a series AND/OR clauses as below
 * <p>
 * (result1 = value AND expression=constant1) OR
 * (result2 = value AND expression=constant2 AND !(expression=constant1)) OR
 * (result3 = value AND expression=constant3 AND !(expression=constant1) AND !(expression=constant2)) OR
 * (elseResult = value AND !(expression=constant1) AND !(expression=constant2) AND !(expression=constant3))
 * <p>
 * The above conversion evaluates the conditions in WHEN clauses multiple times. But if we ensure these conditions are
 * disjunct, we can skip all the NOT of previous WHEN conditions and simplify the expression to:
 * <p>
 * (result1 = value AND expression=constant1) OR
 * (result2 = value AND expression=constant2) OR
 * (result3 = value AND expression=constant3) OR
 * (elseResult = value AND !(expression=constant1) AND !(expression=constant2) AND !(expression=constant3))
 * <p>
 * To ensure the WHEN conditions are disjunct, the following criteria needs to be met:
 * 1. Value is either a constant or column reference or input reference and not any function
 * 2. The LHS expression in all WHEN clauses are the same.
 *    For example, if one WHEN clause has a expression using col1 and another using col2, it will not work
 * 3. The relational operator in the WHEN clause is equals. With other operators it is hard to check for exclusivity.
 * 4. All the RHS expressions are a constant and unique
 * <p>
 * This conversion is done so that it is easy for the ExpressionInterpreter & other Optimizers to further
 * simplify this and construct a domain for the column that can be used by Readers .
 * i.e, ExpressionInterpreter can discard all conditions in which result != value and
 *      RowExpressionDomainTranslator can construct a Domain for the column
 */
public class RewriteCaseExpressionPredicate
        extends RowExpressionRewriteRuleSet
{
    public RewriteCaseExpressionPredicate(FunctionAndTypeManager functionAndTypeManager)
    {
        super(new Rewriter(functionAndTypeManager));
    }

    private static class Rewriter
            implements PlanRowExpressionRewriter
    {
        private final CaseExpressionPredicateRewriter caseExpressionPredicateRewriter;

        public Rewriter(FunctionAndTypeManager functionAndTypeManager)
        {
            requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.caseExpressionPredicateRewriter = new CaseExpressionPredicateRewriter(functionAndTypeManager);
        }

        @Override
        public RowExpression rewrite(RowExpression expression, Rule.Context context)
        {
            return RowExpressionTreeRewriter.rewriteWith(caseExpressionPredicateRewriter, expression);
        }
    }

    private static class CaseExpressionPredicateRewriter
            extends RowExpressionRewriter<Void>
    {
        private final FunctionResolution functionResolution;
        private final LogicalRowExpressions logicalRowExpressions;

        private CaseExpressionPredicateRewriter(FunctionAndTypeManager functionAndTypeManager)
        {
            this.functionResolution = new FunctionResolution(functionAndTypeManager);
            this.logicalRowExpressions = new LogicalRowExpressions(
                    new RowExpressionDeterminismEvaluator(functionAndTypeManager),
                    functionResolution,
                    functionAndTypeManager);
        }

        @Override
        public RowExpression rewriteCall(CallExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
        {
            if (functionResolution.isComparisonFunction(node.getFunctionHandle()) && node.getArguments().size() == 2) {
                RowExpression left = node.getArguments().get(0);
                RowExpression right = node.getArguments().get(1);
                if (isCaseExpression(left) && isSimpleExpression(right)) {
                    return processCaseExpression(left, expression -> replaceArguments(node, expression, right), right);
                }
                else if (isCaseExpression(right) && isSimpleExpression(left)) {
                    return processCaseExpression(right, expression -> replaceArguments(node, left, expression), left);
                }
            }
            return null;
        }

        private boolean isCaseExpression(RowExpression expression)
        {
            if (logicalRowExpressions.isCastExpression(expression)) {
                expression = ((CallExpression) expression).getArguments().get(0);
            }
            return expression instanceof SpecialFormExpression && ((SpecialFormExpression) expression).getForm().equals(SWITCH);
        }

        private boolean isSimpleExpression(RowExpression expression)
        {
            if (logicalRowExpressions.isCastExpression(expression)) {
                return isSimpleExpression(((CallExpression) expression).getArguments().get(0));
            }
            return expression instanceof ConstantExpression ||
                    expression instanceof VariableReferenceExpression ||
                    expression instanceof InputReferenceExpression;
        }

        private RowExpression processCaseExpression(RowExpression expression,
                                                    Function<RowExpression, RowExpression> comparisonExpressionGenerator,
                                                    RowExpression value)
        {
            if (expression instanceof SpecialFormExpression) {
                checkArgument(logicalRowExpressions.isCaseExpression(expression), "expression must be a CASE expression");
                return processCaseExpression(
                        (SpecialFormExpression) expression,
                        Optional.empty(),
                        comparisonExpressionGenerator,
                        value);
            }
            else {
                checkArgument(logicalRowExpressions.isCastExpression(expression), "expression must be a CAST expression");
                checkArgument(logicalRowExpressions.isCaseExpression(((CallExpression) expression).getArguments().get(0)), "expression argument must be a CASE expression");
                return processCaseExpression(
                        (SpecialFormExpression) ((CallExpression) expression).getArguments().get(0),
                        Optional.of((CallExpression) expression),
                        comparisonExpressionGenerator,
                        value);
            }
        }

        /**
         * RowExpression representation of Case Statement:
         * SpecialFormExpression:
         *  form: SWITCH
         *  arguments:
         *      [0]: RowExpression (or) ConstantExpression(TRUE)    // SimpleCaseExpression (or) SearchedCaseExpression
         *      [1..n-1 (or) n]: SpecialFormExpression(form: WHEN)  // else clause is present (or) absent
         *      [n]: RowExpression                                  // available if else clause is present
         */
        private RowExpression processCaseExpression(SpecialFormExpression caseExpression,
                                                    Optional<CallExpression> castExpression,
                                                    Function<RowExpression, RowExpression> comparisonExpressionGenerator,
                                                    RowExpression value)
        {
            Optional<RowExpression> caseOperand = getCaseOperand(caseExpression.getArguments().get(0));
            List<RowExpression> whenClauses;
            Optional<RowExpression> elseResult = Optional.empty();
            int argumentsSize = caseExpression.getArguments().size();
            RowExpression last = caseExpression.getArguments().get(argumentsSize - 1);

            if (last instanceof SpecialFormExpression && ((SpecialFormExpression) last).getForm().equals(WHEN)) {
                whenClauses = caseExpression.getArguments().subList(1, argumentsSize);
            }
            else {
                whenClauses = caseExpression.getArguments().subList(1, argumentsSize - 1);
                elseResult = Optional.of(last);
            }

            if (caseOperand.isPresent() ?
                    !canRewriteSimpleCaseExpression(whenClauses) :
                    !canRewriteSearchedCaseExpression(whenClauses)) {
                return null;
            }

            ImmutableList.Builder<RowExpression> andExpressions = new ImmutableList.Builder<>();
            ImmutableList.Builder<RowExpression> invertedOperands = new ImmutableList.Builder<>();

            for (RowExpression whenClause : whenClauses) {
                RowExpression whenOperand = ((SpecialFormExpression) whenClause).getArguments().get(0);
                if (caseOperand.isPresent()) {
                    whenOperand = logicalRowExpressions.equalsCallExpression(caseOperand.get(), whenOperand);
                }
                RowExpression whenResult = ((SpecialFormExpression) whenClause).getArguments().get(1);
                if (castExpression.isPresent()) {
                    whenResult = replaceArguments(castExpression.get(), whenResult);
                }

                RowExpression comparisonExpression = comparisonExpressionGenerator.apply(whenResult);
                andExpressions.add(and(comparisonExpression, whenOperand));
                invertedOperands.add(logicalRowExpressions.notCallExpression(whenOperand));
            }
            RowExpression elseCondition = and(
                    getElseExpression(castExpression, value, elseResult, comparisonExpressionGenerator),
                    and(invertedOperands.build()));
            andExpressions.add(elseCondition);

            return or(andExpressions.build());
        }

        private RowExpression getElseExpression(Optional<CallExpression> castExpression,
                                                RowExpression value,
                                                Optional<RowExpression> elseValue,
                                                Function<RowExpression, RowExpression> comparisonExpressionGenerator)
        {
            return elseValue.map(
                    elseVal -> comparisonExpressionGenerator.apply(castExpression.map(castExp -> replaceArguments(castExp, elseVal)).orElse(elseVal)
                    )).orElse(new SpecialFormExpression(IS_NULL, BOOLEAN, value));
        }

        private Optional<RowExpression> getCaseOperand(RowExpression expression)
        {
            boolean searchedCase = (expression instanceof ConstantExpression && expression.getType() == BOOLEAN &&
                    ((ConstantExpression) expression).getValue() == Boolean.TRUE);
            return searchedCase ? Optional.empty() : Optional.of(expression);
        }

        private boolean canRewriteSimpleCaseExpression(List<RowExpression> whenClauses)
        {
            List<RowExpression> whenOperands = whenClauses.stream()
                    .map(x -> ((SpecialFormExpression) x).getArguments().get(0))
                    .collect(Collectors.toList());
            return allExpressionsAreConstantAndUnique(whenOperands);
        }

        private boolean canRewriteSearchedCaseExpression(List<RowExpression> whenClauses)
        {
            if (!allAreEqualsExpression(whenClauses) || !allLHSOperandsAreUnique(whenClauses)) {
                return false;
            }
            List<RowExpression> rhsExpressions = whenClauses.stream()
                    .map(whenClause -> ((SpecialFormExpression) whenClause).getArguments().get(0))
                    .map(whenOperand -> ((CallExpression) whenOperand).getArguments().get(1))
                    .collect(Collectors.toList());
            return allExpressionsAreConstantAndUnique(rhsExpressions);
        }

        private boolean allLHSOperandsAreUnique(List<RowExpression> whenClauses)
        {
            return whenClauses.stream()
                    .map(whenClause -> ((SpecialFormExpression) whenClause).getArguments().get(0))
                    .map(whenOperand -> ((CallExpression) whenOperand).getArguments().get(0))
                    .distinct()
                    .count() == 1;
        }

        private boolean allAreEqualsExpression(List<RowExpression> whenClauses)
        {
            return whenClauses.stream()
                    .map(whenClause -> ((SpecialFormExpression) whenClause).getArguments().get(0))
                    .allMatch(logicalRowExpressions::isEqualsExpression);
        }

        private boolean allExpressionsAreConstantAndUnique(List<RowExpression> expressions)
        {
            Set<RowExpression> expressionSet = new HashSet<>();
            for (RowExpression expression : expressions) {
                if (!isConstantExpression(expression) || expressionSet.contains(expression)) {
                    return false;
                }
                expressionSet.add(expression);
            }
            return true;
        }

        private boolean isConstantExpression(RowExpression expression)
        {
            if (logicalRowExpressions.isCastExpression(expression)) {
                return isConstantExpression(((CallExpression) expression).getArguments().get(0));
            }
            return expression instanceof ConstantExpression;
        }
    }

    @Override
    public boolean isRewriterEnabled(Session session)
    {
        return isOptimizeCaseExpressionPredicate(session);
    }

    @Override
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                filterRowExpressionRewriteRule(),
                joinRowExpressionRewriteRule());
    }
}
