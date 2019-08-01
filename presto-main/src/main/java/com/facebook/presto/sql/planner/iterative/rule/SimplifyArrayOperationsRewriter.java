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
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpression.Operator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.WhenClause;
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

            // Rewrite CARDINALITY(FILTER(arr, x -> f(x))) as REDUCE(arr, cast(0 as bigint), (s, x) -> case when f(x) then s + 1 else s, s -> s)
            LambdaArgumentDeclaration origLambdaArgument = origLambda.getArguments().get(0);
            Expression origLambdaBody = origLambda.getBody();
            NodeLocation location = filter.getLocation().orElse(new NodeLocation(0, 0));

            // New lambda arguments
            // TODO(viswanadha): Fix it to get a unique name that doesn't clash with existing ones.
            String combineFunctionArgumentName = origLambdaArgument.getName().getValue() + "__1__";
            LambdaArgumentDeclaration combineFunctionArgument = new LambdaArgumentDeclaration(new Identifier(location, combineFunctionArgumentName, false));

            // Final lambda arguments
            String outputFunctionArgumentName = origLambdaArgument.getName().getValue() + "__2__";
            LambdaArgumentDeclaration outputFunctionArgument = new LambdaArgumentDeclaration(new Identifier(location, outputFunctionArgumentName, false));

            ImmutableList.Builder<WhenClause> builder = new ImmutableList.Builder<WhenClause>();
            Expression elsePart = new Identifier(location, combineFunctionArgumentName, false);

            // New lambda body
            ArithmeticBinaryExpression plus1 =
                    new ArithmeticBinaryExpression(location,
                            ArithmeticBinaryExpression.Operator.ADD,
                            new Identifier(location, combineFunctionArgumentName, false),
                            new LongLiteral(location, "1"));

            if (isComparison) {
                // If it's a comparison, stop when the condition is true. So cardinality(filter(a, x -> f(x))) > 0 becomes reduce(a, 0, (s, x) ->tudligithfnithffeekfhuhrevjltijucase when s > 0 then s when f(x) then s + 1 else s, s->s > 0)
                builder.add(
                        new WhenClause(
                                new ComparisonExpression(
                                        location,
                                        operator.get(),
                                        new Identifier(location, combineFunctionArgumentName, false),
                                        rhs.get()),
                                new Identifier(location, combineFunctionArgumentName, false)));
            }

            builder.add(new WhenClause(origLambdaBody, plus1));
            LambdaExpression combineFunction = new LambdaExpression(
                    location,
                    ImmutableList.of(combineFunctionArgument, origLambdaArgument),
                    new SearchedCaseExpression(location, builder.build(), Optional.of(elsePart)));

            // Final argument to reduce. s -> s
            Expression outputFunctionBody = new Identifier(location, outputFunctionArgumentName, false);
            if (isComparison) {
                // If it's a comparison, simply return the comparison
                outputFunctionBody = new ComparisonExpression(cardinalityOfFilter.getLocation().orElse(new NodeLocation(0, 0)), operator.get(), outputFunctionBody, rhs.get());
            }

            LambdaExpression outputFunction = new LambdaExpression(location, ImmutableList.of(outputFunctionArgument), outputFunctionBody);
            // Now make the reduce
            return new FunctionCall(
                    location,
                    QualifiedName.of("reduce"),
                    ImmutableList.of(
                            array,
                            new Cast(location, new LongLiteral(location, "0"), "BIGINT"),
                            combineFunction,
                            outputFunction));
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

            if (isSimpleComaparison(comparisonExpression) && isCardinalityOfFilter(left) && isConstant(right)) {
                return simplifyCardinalityOfFilter((FunctionCall) left, Optional.of(operator), Optional.of(right), context, treeRewriter);
            }

            // If the left is a literal and right is cardinality, we simply normalize it to reverse the operation.
            if (isSimpleComaparison(comparisonExpression) && isCardinalityOfFilter(right) && isConstant(left)) {
                return simplifyCardinalityOfFilter((FunctionCall) left, Optional.of(operator.negate()), Optional.of(right), context, treeRewriter);
            }

            return treeRewriter.defaultRewrite(comparisonExpression, context);
        }
    }
}
