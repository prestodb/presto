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
package com.facebook.presto.expressions.translator;

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import static java.util.Objects.requireNonNull;

public class RowExpressionTreeTranslator<T, C>
{
    private final RowExpressionTranslator<T, C> rowExpressionTranslator;
    private final RowExpressionVisitor<TranslatedExpression<T>, C> visitor;

    private RowExpressionTreeTranslator(RowExpressionTranslator<T, C> rowExpressionTranslator)
    {
        this.rowExpressionTranslator = requireNonNull(rowExpressionTranslator, "rowExpressionTranslator is null");
        this.visitor = new TranslatingVisitor();
    }

    public TranslatedExpression<T> rewrite(RowExpression node, C context)
    {
        return node.accept(this.visitor, context);
    }

    public static <T, C> TranslatedExpression<T> translateWith(
            RowExpression expression,
            RowExpressionTranslator<T, C> translator,
            C context)
    {
        return expression.accept(new RowExpressionTreeTranslator<>(translator).visitor, context);
    }

    private class TranslatingVisitor
            implements RowExpressionVisitor<TranslatedExpression<T>, C>
    {
        @Override
        public TranslatedExpression<T> visitCall(CallExpression call, C context)
        {
            return rowExpressionTranslator.translateCall(call, context, RowExpressionTreeTranslator.this);
        }

        @Override
        public TranslatedExpression<T> visitInputReference(InputReferenceExpression reference, C context)
        {
            // InputReferenceExpression should only be used by Presto engine rather than connectors
            throw new UnsupportedOperationException("Cannot translate RowExpression that contains inputReferenceExpression");
        }

        @Override
        public TranslatedExpression<T> visitConstant(ConstantExpression literal, C context)
        {
            return rowExpressionTranslator.translateConstant(literal, context, RowExpressionTreeTranslator.this);
        }

        @Override
        public TranslatedExpression<T> visitLambda(LambdaDefinitionExpression lambda, C context)
        {
            return rowExpressionTranslator.translateLambda(lambda, context, RowExpressionTreeTranslator.this);
        }

        @Override
        public TranslatedExpression<T> visitVariableReference(VariableReferenceExpression reference, C context)
        {
            return rowExpressionTranslator.translateVariable(reference, context, RowExpressionTreeTranslator.this);
        }

        @Override
        public TranslatedExpression<T> visitSpecialForm(SpecialFormExpression specialForm, C context)
        {
            return rowExpressionTranslator.translateSpecialForm(specialForm, context, RowExpressionTreeTranslator.this);
        }
    }
}
