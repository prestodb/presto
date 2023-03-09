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

import static java.lang.String.format;

public interface RowExpressionVisitor<R, C>
{
    default R visitExpression(RowExpression expression, C context)
    {
        throw new UnsupportedOperationException(format("Unimplemented RowExpression visitor for %s", expression.getClass()));
    }

    default R visitCall(CallExpression call, C context)
    {
        return visitExpression(call, context);
    }

    default R visitInputReference(InputReferenceExpression reference, C context)
    {
        return visitExpression(reference, context);
    }

    default R visitConstant(ConstantExpression literal, C context)
    {
        return visitExpression(literal, context);
    }

    default R visitLambda(LambdaDefinitionExpression lambda, C context)
    {
        return visitExpression(lambda, context);
    }

    default R visitVariableReference(VariableReferenceExpression reference, C context)
    {
        return visitExpression(reference, context);
    }

    default R visitSpecialForm(SpecialFormExpression specialForm, C context)
    {
        return visitExpression(specialForm, context);
    }

    default R visitIntermediateFormExpression(IntermediateFormExpression intermediateFormExpression, C context)
    {
        return visitExpression(intermediateFormExpression, context);
    }

    default R visitInSubqueryExpression(InSubqueryExpression inSubqueryRowExpression, C context)
    {
        return visitIntermediateFormExpression(inSubqueryRowExpression, context);
    }

    default R visitQuantifiedComparisonExpression(QuantifiedComparisonExpression quantifiedComparisonRowExpression, C context)
    {
        return visitIntermediateFormExpression(quantifiedComparisonRowExpression, context);
    }

    default R visitExistsExpression(ExistsExpression existsRowExpression, C context)
    {
        return visitIntermediateFormExpression(existsRowExpression, context);
    }
}
