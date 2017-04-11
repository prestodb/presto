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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.relational.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.sql.relational.Signatures.TRY;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;

public class LambdaAndTryExpressionExtractor
{
    private LambdaAndTryExpressionExtractor()
    {
    }

    public static List<RowExpression> extractLambdaAndTryExpressions(RowExpression expression)
    {
        Visitor visitor = new Visitor();
        expression.accept(visitor, new Context(false));
        return visitor.getLambdaAndTryExpressionsPostOrder();
    }

    private static class Visitor
            implements RowExpressionVisitor<Context, Void>
    {
        private final ImmutableList.Builder<RowExpression> lambdaAndTryExpressions = ImmutableList.builder();

        @Override
        public Void visitInputReference(InputReferenceExpression node, Context context)
        {
            // TODO: change such that CallExpressions only capture the inputs they actually depend on
            return null;
        }

        @Override
        public Void visitCall(CallExpression call, Context context)
        {
            boolean isTry = call.getSignature().getName().equals(TRY);
            if (isTry) {
                checkState(call.getArguments().size() == 1, "try call expressions must have a single argument");
                checkState(getOnlyElement(call.getArguments()) instanceof CallExpression, "try call expression argument must be a call expression");
                if (context.isInLambda()) {
                    throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Try expression inside lambda expression is not support yet");
                }
            }

            for (RowExpression rowExpression : call.getArguments()) {
                rowExpression.accept(this, context);
            }

            if (isTry) {
                lambdaAndTryExpressions.add(getOnlyElement(call.getArguments()));
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
            lambdaAndTryExpressions.add(lambda);
            return null;
        }

        @Override
        public Void visitVariableReference(VariableReferenceExpression reference, Context context)
        {
            return null;
        }

        private List<RowExpression> getLambdaAndTryExpressionsPostOrder()
        {
            return lambdaAndTryExpressions.build();
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
