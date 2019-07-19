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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.builder;

/**
 * Extracts and returns the set of all RowExpression subtrees within an RowExpression, including Expression itself
 */
public class SubRowExpressionExtractor
{
    private static final PreOrderVisitor PRE_ORDER_TRAVERSER = new PreOrderVisitor();

    private SubRowExpressionExtractor() {}

    public static Set<RowExpression> extract(RowExpression expression)
    {
        ImmutableSet.Builder result = builder();
        expression.accept(PRE_ORDER_TRAVERSER, result);
        return result.build();
    }

    private static class PreOrderVisitor
            implements RowExpressionVisitor<Void, ImmutableSet.Builder>
    {
        @Override
        public Void visitCall(CallExpression call, ImmutableSet.Builder context)
        {
            context.add(call);
            call.getArguments().forEach(argument -> argument.accept(this, context));
            return null;
        }

        @Override
        public Void visitInputReference(InputReferenceExpression reference, ImmutableSet.Builder context)
        {
            context.add(reference);
            return null;
        }

        @Override
        public Void visitConstant(ConstantExpression literal, ImmutableSet.Builder context)
        {
            context.add(literal);
            return null;
        }

        @Override
        public Void visitLambda(LambdaDefinitionExpression lambda, ImmutableSet.Builder context)
        {
            context.add(lambda);
            lambda.getBody().accept(this, context);
            return null;
        }

        @Override
        public Void visitVariableReference(VariableReferenceExpression reference, ImmutableSet.Builder context)
        {
            context.add(reference);
            return null;
        }

        @Override
        public Void visitSpecialForm(SpecialFormExpression specialForm, ImmutableSet.Builder context)
        {
            context.add(specialForm);
            specialForm.getArguments().forEach(argument -> argument.accept(this, context));
            return null;
        }
    }
}
