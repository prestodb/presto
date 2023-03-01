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

import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;

public interface RowExpressionVisitor<R, C>
{
    R visitCall(CallExpression call, C context);

    R visitInputReference(InputReferenceExpression reference, C context);

    R visitConstant(ConstantExpression literal, C context);

    R visitLambda(LambdaDefinitionExpression lambda, C context);

    R visitVariableReference(VariableReferenceExpression reference, C context);

    R visitSpecialForm(SpecialFormExpression specialForm, C context);

    // Default implementations for IntermediateFormRowExpression are provided below, as these only exist as
    // an intermediate form during some parts of Optimizer, and not all usecases need to care about these.
    default R visitIntermediateFormExpression(IntermediateFormExpression intermediateFormRowExpression, C context)
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Unexpected intermediate form expression: %s", intermediateFormRowExpression));
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
