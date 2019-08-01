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
package com.facebook.presto.expressions;

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

/**
 * The default visitor serves as a template for "consumer-like" tree traversal.
 * {@param context} is the consumer to apply customized actions on the visiting RowExpression.
 */
public class DefaultRowExpressionTraversalVisitor<C>
        implements RowExpressionVisitor<Void, C>
{
    @Override
    public Void visitInputReference(InputReferenceExpression input, C context)
    {
        return null;
    }

    @Override
    public Void visitCall(CallExpression call, C context)
    {
        call.getArguments().forEach(argument -> argument.accept(this, context));
        return null;
    }

    @Override
    public Void visitConstant(ConstantExpression literal, C context)
    {
        return null;
    }

    @Override
    public Void visitLambda(LambdaDefinitionExpression lambda, C context)
    {
        return null;
    }

    @Override
    public Void visitVariableReference(VariableReferenceExpression reference, C context)
    {
        return null;
    }

    @Override
    public Void visitSpecialForm(SpecialFormExpression specialForm, C context)
    {
        specialForm.getArguments().forEach(argument -> argument.accept(this, context));
        return null;
    }
}
