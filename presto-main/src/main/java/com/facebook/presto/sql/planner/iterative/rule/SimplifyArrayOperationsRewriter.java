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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpression.Operator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;

public class SimplifyArrayOperationsRewriter
{
    private SimplifyArrayOperationsRewriter() {}

    public static Expression simplifyArrayOperations(Expression expression, Session session)
    {
        if (SystemSessionProperties.isSimplifyArrayOperations(session)) {
            return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
        }

        return expression;
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        private static boolean isConstant(Expression expression)
        {
            if (expression instanceof Cast) {
                return isConstant(((Cast) expression).getExpression());
            }

            return expression instanceof Literal;
        }

        private static boolean isZero(Expression expression)
        {
            if (expression instanceof Cast) {
                return isZero(((Cast) expression).getExpression());
            }

            return expression instanceof LongLiteral && ((LongLiteral) expression).getValue() == 0;
        }

        private static boolean isCardinalityOfFilter(Expression expression)
        {
            if (expression instanceof FunctionCall) {
                FunctionCall functionCall = (FunctionCall) expression;
                return functionCall.getName().toString().equals("cardinality") &&
                        functionCall.getArguments().size() == 1 &&
                        functionCall.getArguments().get(0) instanceof FunctionCall &&
                        ((FunctionCall) (functionCall.getArguments().get(0))).getName().toString().equals("filter");
            }

            return false;
        }

        private static boolean isSimpleComaparison(ComparisonExpression comparisonExpression)
        {
            switch (comparisonExpression.getOperator()) {
                case EQUAL:
                case GREATER_THAN:
                case LESS_THAN:
                case NOT_EQUAL:
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN_OR_EQUAL:
                    return true;
            }

            return false;
        }

        private Expression simplifyCardinalityOfFilterComparedToZero(
                NodeLocation location,
                Expression array,
                Operator operator,
                LambdaExpression origLambda)
        {
            // Comparing to zero can be rewrtitten to none_match(= 0) or any_match(> 0)
            return new FunctionCall(
                    location,
                    QualifiedName.of(operator == EQUAL ? "none_match" : "any_match"),
                    ImmutableList.of(array, origLambda));
        }

        private Expression simplifyCardinalityOfFilter(
                FunctionCall cardinalityOfFilter,
                Optional<Operator> operator,
                Optional<Expression> rhs,
                Void context,
                ExpressionTreeRewriter<Void> treeRewriter)
        {
            FunctionCall filter = (FunctionCall) cardinalityOfFilter.getArguments().get(0);
            Expression array = treeRewriter.defaultRewrite(filter.getArguments().get(0), context);
            LambdaExpression origLambda = (LambdaExpression) filter.getArguments().get(1);
            boolean isComparison = operator.isPresent() && rhs.isPresent();

            if (isComparison && (operator.get() == Operator.EQUAL || operator.get() == Operator.GREATER_THAN) && isZero(rhs.get())) {
                return simplifyCardinalityOfFilterComparedToZero(cardinalityOfFilter.getLocation().orElse(new NodeLocation(0, 0)), array, operator.get(), origLambda);
            }

            return null;
        }

        @Override
        public Expression rewriteFunctionCall(FunctionCall functionCall, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (isCardinalityOfFilter(functionCall)) {
                return simplifyCardinalityOfFilter(functionCall, Optional.empty(), Optional.empty(), context, treeRewriter);
            }

            return treeRewriter.defaultRewrite(functionCall, context);
        }

        @Override
        public Expression rewriteComparisonExpression(ComparisonExpression comparisonExpression, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            Expression left = comparisonExpression.getLeft();
            Expression right = comparisonExpression.getRight();
            Operator operator = comparisonExpression.getOperator();

            Expression rewritten = null;
            if (isSimpleComaparison(comparisonExpression) && isCardinalityOfFilter(left) && isConstant(right)) {
                rewritten = simplifyCardinalityOfFilter((FunctionCall) left, Optional.of(operator), Optional.of(right), context, treeRewriter);
            }
            else if (isSimpleComaparison(comparisonExpression) && isCardinalityOfFilter(right) && isConstant(left)) {
                // If the left is a literal and right is cardinality, we simply normalize it to reverse the operation.
                rewritten = simplifyCardinalityOfFilter((FunctionCall) left, Optional.of(operator.negate()), Optional.of(right), context, treeRewriter);
            }

            if (rewritten != null)
            {
                return  rewritten;
            }

            return treeRewriter.defaultRewrite(comparisonExpression, context);
        }
    }
}
