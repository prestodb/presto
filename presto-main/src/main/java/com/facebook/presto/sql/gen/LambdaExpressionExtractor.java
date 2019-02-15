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
package com.facebook.presto.sql.gen;

import com.facebook.presto.spi.relation.column.CallExpression;
import com.facebook.presto.spi.relation.column.ColumnExpression;
import com.facebook.presto.spi.relation.column.ColumnExpressionVisitor;
import com.facebook.presto.spi.relation.column.ColumnReferenceExpression;
import com.facebook.presto.spi.relation.column.ConstantExpression;
import com.facebook.presto.spi.relation.column.InputReferenceExpression;
import com.facebook.presto.spi.relation.column.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.column.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class LambdaExpressionExtractor
{
    private LambdaExpressionExtractor()
    {
    }

    public static List<LambdaDefinitionExpression> extractLambdaExpressions(ColumnExpression expression)
    {
        Visitor visitor = new Visitor();
        expression.accept(visitor, new Context(false));
        return visitor.getLambdaExpressionsPostOrder();
    }

    private static class Visitor
            implements ColumnExpressionVisitor<Void, Context>
    {
        private final ImmutableList.Builder<LambdaDefinitionExpression> lambdaExpressions = ImmutableList.builder();

        @Override
        public Void visitInputReference(InputReferenceExpression node, Context context)
        {
            // TODO: change such that CallExpressions only capture the inputs they actually depend on
            return null;
        }

        @Override
        public Void visitCall(CallExpression call, Context context)
        {
            for (ColumnExpression columnExpression : call.getArguments()) {
                columnExpression.accept(this, context);
            }

            return null;
        }

        @Override
        public Void visitConstant(ConstantExpression literal, Context context)
        {
            return null;
        }

        @Override
        public Void visitLambda(LambdaDefinitionExpression lambda, Context context)
        {
            lambda.getBody().accept(this, new Context(true));
            lambdaExpressions.add(lambda);
            return null;
        }

        @Override
        public Void visitVariableReference(VariableReferenceExpression reference, Context context)
        {
            return null;
        }

        @Override
        public Void visitColumnReference(ColumnReferenceExpression reference, Context context)
        {
            return null;
        }

        private List<LambdaDefinitionExpression> getLambdaExpressionsPostOrder()
        {
            return lambdaExpressions.build();
        }
    }

    private static class Context
    {
        private final boolean inLambda;

        public Context(boolean inLambda)
        {
            this.inLambda = inLambda;
        }

        public boolean isInLambda()
        {
            return inLambda;
        }
    }
}
